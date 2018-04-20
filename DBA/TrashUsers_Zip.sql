IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'TrashUsers_Zip')
      DROP PROCEDURE [dbo].[TrashUsers_Zip]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
Title : Zips all TrashUsers infos, from data in TrashUsersImpactedTables 

Uses the JoinQuery as filter
, Range start and Range End fon pk columns

GRANT EXECUTE ON TO [nobody]

History:
	2018-01-15 - XMO - Fix on PKColumnNames
	2018-01-12 - XMO - Small fixes
	2017-12-06 - XMO - Updates
	2017-11-27 - XMO - Creation 
*/
CREATE PROCEDURE [dbo].[TrashUsers_Zip]
(
	 @Scope SYSNAME = 'ARJEL' -- the scope of the trash
	,@SpecificTable sysname = '%'
	,@MaxScanDuration TINYINT = 10 -- in minutes
	,@MaxNbRowsToDelete INT = 5000000 
	,@ForceId SMALLINT = NULL -- To force the zip of this ID in TrashUsersImpactedTables, ignores all other filters! Use with caution 
	,@Debug BIT = 0
)
AS
BEGIN TRY
	CREATE TABLE #ListIds (ID INT Primary KEY)

	IF @ForceId IS NOT NULL
		INSERT  INTO #ListIds SELECT @ForceId AS Id
	ELSE
		INSERT  INTO #ListIds
		SELECT Id 
		FROM TrashUsersImpactedTables WITH (NOLOCK)
		WHERE Scope = @Scope
		AND JoinQuery IS NOT NULL
		ANd ScanDurationMin IS NOT NULL
		AND NbRowsToDelete > 0
		AND ScanDurationMin < @MaxScanDuration
		AND NbRowsToDelete <= @MaxNbRowsToDelete
		AND RangeColumn IS NOT NULL
		AND TableFullName LIKE @SpecificTable
		AND RangeStart IS NOT NULL
		AND RangeEnd IS NOT NULL
		AND NbZipedRows IS NULL
		--AND IsNullable = 0 zip accepted for nullables

	DECLARE @CurrentId INT
		,@SQLExec NVARCHAR(4000) 
		,@StartedScan DATETIME2(0)
		,@EndedScan DATETIME2(0) 
		,@NbRowsToDelete INT
		,@RangeStart SQL_VARIANT
		,@RangeEnd	SQL_VARIANT
		,@RangeColumn SYSNAME
		,@FKLevel	TINYINT
		,@TableFullName	SYSNAME
		,@ColumnName	SYSNAME
		,@JoinQuery	nvarchar(3000)
		,@Filter    nvarchar(4000)
		,@ZipPath	VARCHAR(500) 
		,@NbZipedRows INT 
	
	WHILE EXISTS (SELECT 1 FROM #ListIds)
	BEGIN
		SELECT TOP 1 @CurrentId = List.ID 
			,@TableFullName = TableFullName
			,@ColumnName = ColumnName
			,@NbRowsToDelete = NbRowsToDelete
			,@RangeStart = RangeStart 
			,@RangeEnd	 = RangeEnd	
			,@RangeColumn = RangeColumn
			,@JoinQuery = JoinQuery
			,@FKLevel = FKLevel
		FROM #ListIds AS List
		INNER JOIN TrashUsersImpactedTables AS tuit WITH (NOLOCK) ON tuit.Id = List.id
		ORDER BY List.Id DESC
		
		SELECT @Filter = @ColumnName+' IN (SELECT  [Lvl'+CAST(@FKLevel AS VARCHAR(2))+'].'+@ColumnName+' '+ @JoinQuery+')'
		SELECT @ZipPath= CAST(DBA.Indus.GetTokenValue('Path_TableArchives')  AS VARCHAR(100))+'\TrashUsers\'+@Scope+'\'+@TableFullName+'\'

		EXEC @NbZipedRows=DBA.dbo.ArchiveTable_CopyIntoZip 
		@Table				= @TableFullName
		,@RangeColumn		= @RangeColumn
		,@RangeStart		= @RangeStart
		,@RangeEnd			= @RangeEnd
		,@Filter			= @Filter
		,@ZipPath			= @ZipPath
		,@RangeEndIncluded	= 1
		,@Debug				= @Debug
		
		
		IF @NbZipedRows != @NbRowsToDelete 
		BEGIN;
			DECLARE @ErrorMsg VARCHAR(500) = 'TrashUsers_Zip incoherent number between Ziped and expected rows for table '+@TableFullName+', scope '+@Scope+', NbZipedRows '+CAST(@NbZipedRows AS varchar(10))+', NbRowsToDelete '+CAST(@NbRowsToDelete AS varchar(10))
			;
			THROW 55133 , @ErrorMsg, 2
		END

		UPDATE TrashUsersImpactedTables 
		SET NbZipedRows = @NbZipedRows
		FROM TrashUsersImpactedTables
		WHERE Id = @CurrentId

		DELETE #ListIds WHERE id = @CurrentId
	END
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO