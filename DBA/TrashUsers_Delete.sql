IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'TrashUsers_Delete')
      DROP PROCEDURE [dbo].[TrashUsers_Delete]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
Title : Deletes all TrashUsers infos, from data in TrashUsersImpactedTables 

Uses the JoinQuery as filter
, Range start and Range End fon pk columns

GRANT EXECUTE ON TO [nobody]

History:
	2018-01-18 - XMO - Removed Multi PK restriction
	2018-01-15 - XMO - Fix on PKColumnNames
	2018-01-12 - XMO - Small fix
	2017-12-06 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[TrashUsers_Delete]
(
	 @Scope SYSNAME = 'ARJEL' -- the scope of the trash
	,@SpecificTable sysname = '%'
	,@MaxScanDuration TINYINT = 10 -- in minutes
	,@MaxNbRowsToDelete INT = 5000000 
	,@ForceId SMALLINT = NULL -- To force the delete of this ID in TrashUsersImpactedTables, ignores all other filters! Use with caution 
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
		AND NbZipedRows IS NOT NULL
		AND RangeStart IS NOT NULL
		AND RangeEnd IS NOT NULL
		AND NbDeletedRows IS NULL
		AND IsNullable = 0

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
		,@PKColumnNames varchar(500)
		,@JoinQuery	nvarchar(3000)
		,@Filter    nvarchar(4000)
		--
		,@NbDeletedRows INT 
		,@ErrorMsg VARCHAR(500)
	
	WHILE EXISTS (SELECT 1 FROM #ListIds)
	BEGIN
		SELECT TOP 1 @CurrentId = List.ID 
			,@TableFullName = TableFullName
			,@ColumnName = ColumnName
			,@PKColumnNames = PKColumnNames
			,@NbRowsToDelete = NbRowsToDelete
			,@RangeStart = RangeStart 
			,@RangeEnd	 = RangeEnd	
			,@RangeColumn = RangeColumn
			,@JoinQuery = JoinQuery
			,@FKLevel = FKLevel
		FROM #ListIds AS List
		INNER JOIN TrashUsersImpactedTables AS tuit WITH (NOLOCK) ON tuit.Id = List.id
		ORDER BY tuit.FKLevel DESC, List.id DESC
		
		SELECT @Filter = @ColumnName+' IN (SELECT  [Lvl'+CAST(@FKLevel AS VARCHAR(2))+'].'+@ColumnName+' '+ @JoinQuery+')'

		IF ISNULL(@Filter, '') = ''
		BEGIN;
			SELECT @ErrorMsg = 'TrashUsers_Delete empty/null Filter ! Query stopped before deletion. Please investigate, param: Id '+CAST(@CurrentId as varchar(10))+' TableFullname '+@TableFullName+', scope '+@Scope
			;
			THROW 55132 , @ErrorMsg, 2
		END

		EXEC @NbDeletedRows=DBA.dbo.ArchiveTable_DeleteRows 
		@Table				= @TableFullName
		,@RangeColumn		= @RangeColumn
		,@RangeStart		= @RangeStart
		,@RangeEnd			= @RangeEnd
		,@Filter			= @Filter
		,@PrevArchiveAction	= 'CopyIntoZip'
		,@BatchSize			= 10000
		,@UseTruncatePartition = 0
		,@RangeEndIncluded	= 1
		,@Debug				= @Debug
		
		
		IF @NbDeletedRows != @NbRowsToDelete 
		BEGIN;
			SELECT @ErrorMsg = 'TrashUsers_Delete incoherent number between Deleted and expected rows for Id '+CAST(@CurrentId as varchar(10))+', table '+@TableFullName+', scope '+@Scope+', NbDeletedRows '+CAST(@NbDeletedRows AS varchar(10))+', NbRowsToDelete '+CAST(@NbRowsToDelete AS varchar(10))
			;
			THROW 55133 , @ErrorMsg, 2
		END

		UPDATE TrashUsersImpactedTables 
		SET NbDeletedRows = @NbDeletedRows
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