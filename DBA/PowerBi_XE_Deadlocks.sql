IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'PowerBi_XE_Deadlocks')
      DROP PROCEDURE [dbo].[PowerBi_XE_Deadlocks]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Returns a restricted Select of XEvents stats, used by PowerBi to display stats

Policicy override:
GRANT EXECUTE ON [dbo].[PowerBi_XE_Deadlocks] TO [nobody];

History:
	2017-04-21 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[PowerBi_XE_Deadlocks]
AS
BEGIN TRY
	
	IF( OBJECT_ID ('XELogs_Deadlocks') IS NOT NULL
		)
	BEGIN
		SELECT CAST(DATEADD(mi, DATEDIFF(mi,0, timestamp) / 120 * 120, 0) AS DATETIME2(0)) as timestamp
			,Count(*) AS count
			,Details.*
		FROM XELogs_Deadlocks AS XE
		CROSS APPLY (
			SELECT   TOP 1
					X.server_instance_name
					,Processes.p.value('@currentdb', 'TINYINT') AS db_id
					,Processes.p.value('@clientapp', 'SYSNAME') AS clientapp
					,Processes.p.value('@loginname', 'SYSNAME') AS loginname
					,Processes.p.value('(executionStack/frame/@procname)[1]', 'varchar(50)') AS procname
					,Processes.p.value('(executionStack)[1]', 'varchar(4000)') AS Command
	
			FROM    XELogs_Deadlocks AS X WITH(NOLOCK)
			CROSS APPLY X.XML_Report.nodes('deadlock/process-list/process') AS Processes (p)
			WHERE x.Id = XE.Id
		) AS Details
		WHERE XE.timestamp > DATEADD(day, -7, GETUTCDATE())
		GROUP BY Details.server_instance_name
			,Details.db_id
			,Details.clientapp
			,Details.loginname
			,Details.procname
			,Details.Command
			,CAST(DATEADD(mi, DATEDIFF(mi,0, timestamp) / 120 * 120, 0) AS DATETIME2(0))
		ORDER BY Timestamp DESC
	END

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO
