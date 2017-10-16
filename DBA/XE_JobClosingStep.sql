IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'XE_JobClosingStep')
      DROP PROCEDURE [dbo].[XE_JobClosingStep]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Does various non critical things at the end of the DBA-Xevents job

Description:
Various tasks include:
	- Send an email to XMO if sessions conf are going to expire soon. AS it can be a mistake on someone's part
	- Send an email to XMO if last Job execution failed. With error details.
	- Send an email to XMO if lots of calls to web_UserMigrationInfo_ByPseudoPasswordHash

History:
	2017-07-20 - XMO - Send mail alert at two errors
	2017-04-06 - XMO - Creation from hardcoded job step
*/
CREATE PROCEDURE [dbo].[XE_JobClosingStep]
(
	@Debug BIT = 0
)
AS
BEGIN TRY

	DECLARE @HTML NVARCHAR(4000) 
	DECLARE @current_time TIME = (SELECT CONVERT(TIME, dateadd(mi, datediff(mi,0, GETDATE()) / 1 * 1, 0) ))

	--Only once a day, Check if no sessions are going to expire
	if (@current_time=CONVERT(TIME,'00:00') )
	BEGIN
		--CHECK for sessions expiring In the coming days
		SET @HTML=
		(
			SELECT *
			FROM DBA.dbo.XESessionsData WITH(NOLOCK)
			WHERE ExpiryDate < DATEADD(hour, 72, GETDATE())
			FOR XML RAW, ELEMENTS XSINIL
		)
		
		SET @HTML = 'Warning, the following sessions confs are gonna expire in the next 72H !<br>
		SERVER : '+(SELECT  @@servername)
		+DBA.dbo.Convert_XMLtoHTML(@HTML)

		SELECT @HTML
		IF @HTML != ''
			EXEC msdb.dbo.sp_send_dbmail
				@profile_name = 'DBATEAM_Reports',
				@recipients = 'x.montamat@betclicgroup.com',
				@subject = 'XEvents sessions Expiring today ',
				@body = @HTML,
				@body_format = 'HTML';
	END



	-- Check If Last Execution Fail. If first fail in the last 10 min, send an email with step details error message
	;WITH j AS (
		SELECT * 
		FROM DBA.[dbo].[GetJobHistory] ('DBA-XEvents',DATEADD(mi, -10, GETDATE()))
	)
	--Get last job steps details
	SELECT @HTML =(
		SELECT LastFail.*
		FROM j AS LastFail
		WHERE LastFail.RunOcc = 1
		--Only if the two last job exec failed
		AND (SELECT COUNT(*) FROM j WHERE j.RunOcc <=2 AND j.run_status != 1) = 2
		--And not if it already failed in the last 10 min (so a mail was already sent, no need to spam)
		AND NOT EXISTS (SELECT 1 FROM j WHERE j.RunOcc > 2 AND j.run_status != 1)
		FOR XML RAW, ELEMENTS XSINIL
	)

	IF @HTML IS NOT NULL
	BEGIN
		SET @HTML =  'Last DBA-XEvents job failed ! <br> Details below <br>
		SERVER : '+(SELECT  @@servername)
		+DBA.dbo.Convert_XMLtoHTML (@HTML) 
		EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DBATEAM_Reports',
			@recipients = 'x.montamat@betclicgroup.com',
			@subject = 'DBA-XEvents Job failed',
			@body = @HTML,
			@body_format = 'HTML';
	END

	--Trigger a mail alert if high web_UserMigrationInfo_ByPseudoPasswordHash count in the latest 5 minute while the previous 2H were quiet
	IF (SELECT CAST(DBA.Indus.GetTokenValue('_EnvironmentType') AS VARCHAR(100))) = 'PROD'
		AND (OBJECT_ID('temp.XEstats5min_targeted_rpcs')) IS NOT NULL
	BEGIN
		DECLARE @Count_usermig_attack INT 
		SELECT top 1 @Count_usermig_attack = count
			FROM temp.XEstats5min_targeted_rpcs
			WHERE object_name = 'web_UserMigrationInfo_ByPseudoPasswordHash'
			AND timestamp >= DATEADD(minute, -6, GETUTCDATE())
			ORDER BY count DESC

		IF @Count_usermig_attack > 1000
			AND NOT EXISTS (
				SELECT 1 
				FROM temp.XEstats5min_targeted_rpcs
				WHERE object_name = 'web_UserMigrationInfo_ByPseudoPasswordHash'
				AND timestamp < DATEADD(minute, -6, GETUTCDATE())
				AND timestamp > DATEADD(minute, -120, GETUTCDATE())
				AND count > 1000
			)
		BEGIN
		EXEC msdb.dbo.sp_send_dbmail
				@profile_name = 'DBATEAM_Reports',
				@recipients = 'x.montamat@betclicgroup.com',
				@subject = 'web_UserMigrationInfo_ByPseudoPasswordHash High count Alert',
				@body = 'Hi, there is currently a high number of calls to the infamous :web_UserMigrationInfo_ByPseudoPasswordHash (over a 1000 calls last 5 min)',
				@body_format = 'HTML';
		END
	END

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [dbo].[XE_JobClosingStep] TO [public];
GO

