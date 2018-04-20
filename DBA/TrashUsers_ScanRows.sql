IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'TrashUsers_ScanRows')
      DROP PROCEDURE [dbo].[TrashUsers_ScanRows]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
Title : Scan all tables in TrashUsersImpactedTables and fill the columns
	- NbrowsToDelete  
	- ScanDurationMin (minute)
	- RangeStart		
	- RangeEnd		

Warning : This uses nolocks but does a lot of reads!
Doesn't manage rows multiple PKs

Note : This can return an error of type mismatch if for exemple a FoundTable doesn't have an INT column type

GRANT EXECUTE ON TO [nobody]

History:
	2017-12-06 - XMO - Updates
	2017-11-27 - XMO - Creation 
*/
CREATE PROCEDURE [dbo].[TrashUsers_ScanRows]
(
	 @Scope SYSNAME = 'ARJEL' --the scope of the trash
	,@SpecificTable sysname = '%'
	,@Debug BIT = 0
)
AS
BEGIN TRY
	SELECT ListTables.Id 
	INTO #ListIds 
	FROM TrashUsersImpactedTables AS ListTables
	CROSS APPLY (
		SELECT Top 1 _list.Id
		FROM TrashUsersImpactedTables AS _list
		WHERE	_list.TableFullName = ListTables.TableFullName
		AND		(ListTables.FKName IS NULL OR
				 _list.FKName = ListTables.FKName)
		AND		_list.Scope = @Scope
	) AS FirstColumn
	WHERE ListTables.Scope = @Scope
	AND ListTables.JoinQuery IS NOT NULL 
	AND (FirstColumn.id = ListTables.id OR ListTables.FKName IS NULL)
	AND ScanDurationMin IS NULL --already scanned
	AND TableFullName LIKE @SpecificTable --if specified

	DECLARE @CurrentId INT
	DECLARE @SQLExec NVARCHAR(4000) 
	DECLARE @StartedScan DATETIME2(0)
	DECLARE @EndedScan DATETIME2(0) 
	DECLARE @NbRowsToDelete INT
	DECLARE @RangeStart SQL_VARIANT
	DECLARE @RangeEnd	SQL_VARIANT

	WHILE EXISTS (SELECT 1 FROM #ListIds)
	BEGIN
		SELECT TOP 1 @CurrentId = ID FROM #ListIds
		SELECT @StartedScan = GETDATE()

		DECLARE @RangeColumn SYSNAME 
		SELECT @RangeColumn=
			CASE WHEN PKColumnNames IS NULL THEN ColumnName
				WHEN  PKColumnNames LIKE '%,%' THEN LEFT (PKColumnNames, CHARINDEX(',', PKColumnNames)-1)
				ELSE PKColumnNames
			END
		FROM TrashUsersImpactedTables 
		WHERE id = @CurrentId


		SELECT @SQLExec = '
		SELECT @NbRowsToDeleteOUT = COUNT(*) 
			,@RangeStartOUT = MIN('+'[Lvl'+CAST(FKLevel AS VARCHAR(2))+'].'+@RangeColumn+')
			,@RangeEndOUT = MAX('+'[Lvl'+CAST(FKLevel AS VARCHAR(2))+'].'+@RangeColumn+')
		'+JoinQuery
		FROM TrashUsersImpactedTables 
		WHERE id = @CurrentId

		IF @Debug = 1
			SELECT @SQLExec
		ELSE
		EXEC sp_executesql @SQLExec
		, N'@NbRowsToDeleteOUT INT OUTPUT
		, @RangeStartOUT SQL_VARIANT  OUTPUT
		, @RangeEndOUT	SQL_VARIANT	   OUTPUT
		'
		, @NbRowsToDeleteOUT = @NbRowsToDelete OUTPUT
		, @RangeStartOUT = @RangeStart OUTPUT
		, @RangeEndOUT = @RangeEnd OUTPUT
		
		IF @Debug = 0
		UPDATE TrashUsersImpactedTables
		SET NbRowsToDelete = @NbRowsToDelete
			,ScanDurationMin = DATEDIFF(minute, @StartedScan, GETDATE())
			,RangeStart = @RangeStart
			,RangeEnd = @RangeEnd
			,RangeColumn = @RangeColumn
		FROM TrashUsersImpactedTables 
		WHERE id = @CurrentId

		DELETE #ListIds WHERE id = @CurrentId
	END

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO