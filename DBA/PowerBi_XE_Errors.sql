IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'PowerBi_XE_Errors')
      DROP PROCEDURE [dbo].[PowerBi_XE_Errors]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Returns a restricted Select of XEvents stats, used by PowerBi to display stats

Policicy override:
GRANT EXECUTE ON [dbo].[PowerBi_XE_Errors] TO [nobody];

History:
	2017-04-21 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[PowerBi_XE_Errors]
(
	@NbDays TINYINT = 7
	,@AgregateMin SMALLINT = 10
)
AS
BEGIN TRY
	IF( OBJECT_ID ('XELogs_Errors') IS NOT NULL
		)
	BEGIN
		SELECT * FROM (
			SELECT CAST(DATEADD(mi, DATEDIFF(mi,0, timestamp) / @AgregateMin * @AgregateMin, 0) AS DATETIME2(0)) as timestamp
				,Count(*) AS count
				,XE.server_instance_name
				,XE.database_name
				,XE.username
				,XE.client_app_name
				,XE.error_number
			FROM XELogs_Errors AS XE WITH (NOLOCK)
			WHERE XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
			AND	  (XE.username NOT LIKE 'BETCLIC\%' OR XE.username = 'BETCLIC\sqlagent')
			GROUP BY CAST(DATEADD(mi, DATEDIFF(mi,0, timestamp) / @AgregateMin * @AgregateMin, 0) AS DATETIME2(0))
				,XE.server_instance_name
				,XE.database_name
				,XE.username
				,XE.client_app_name
				,XE.error_number
			) AS XE
		CROSS APPLY (
			SELECT TOP (1)	message 
			FROM XELogs_Errors AS XE_Details WITH (NOLOCK)
			WHERE
					XE.server_instance_name =	XE_Details.server_instance_name
				AND XE.database_name		=	XE_Details.database_name			
				AND XE.username				=	XE_Details.username			
				AND XE.client_app_name		=	XE_Details.client_app_name		
				AND XE.error_number			=	XE_Details.error_number		
				AND XE_Details.timestamp BETWEEN XE.timestamp AND DATEADD(mi, @AgregateMin, XE.timestamp)
			) AS XE_Details
		ORDER BY Timestamp DESC
	END

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

