IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'ArchiveTable_DeleteRows')
      DROP PROCEDURE [dbo].[ArchiveTable_DeleteRows]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : ArchiveTable proc (only deletion). To use after ArchiveTable_CopyIntoZip for exemple

Deletes a given range of rows in a table. Via a batches of @BatchSize rows every 1 seconds.

Results Saved in dba.dbo.TablesArchivingHistory

Example to delete a month of data (use same parameters & values as previous step):
DECLARE @nbDeletedRows INT 
exec @nbDeletedRows = dba.dbo.ArchiveTable_DeleteRows
 @Table = 'Betclick.dbo.BigTableToArchive'
,@RangeColumn = 'date'
,@RangeStart = {d'2016-02-01'}
,@RangeEnd = {d'2016-03-01'}

Policicy override:
GRANT EXECUTE ON [dbo].[ArchiveTable_DeleteRows] TO [nobody];

History:
	2017-08-28 - XMO - Change AlwaysOn Lag no infinite wait
	2017-04-14 - XMO - Fix Long durations due to empty @ranges
	2017-02-28 - XMO - Add token parametered speed
	2017-02-24 - XMO - Fix Sql_Variants
	2017-02-23 - XMO - Add parametered BatchSize to control the speed
	2016-09-20 - MTG/XMO - fix partition archive
	2016-08-18 - MTG - Generate @TableFullName from FormatObjectName function, check the AlwaysOn lag every 5 loops
	2016-07-04 - XMO - Switch Ranges to sql_variants and add truncate partition
	2016-06-23 - XMO - Creation

*/
CREATE PROCEDURE [dbo].[ArchiveTable_DeleteRows]
(
	@Table		sysname -->Specify 'DBName.schema.TableName' if possible
	,@RangeColumn	sysname --> The column on which to check the range, can be a date, an id...
	,@RangeStart	sql_variant -->inclusive can be a date, an id...
	,@RangeEnd		sql_variant -->exclusive can be a date, an id...
	,@Filter		VARCHAR(5000)= NULL --Optional, if only rows respecting this condition should be archived
	,@PrevArchiveAction VARCHAR(500) = 'CopyIntoZip' --Set to NULL to ignore this Check, by default it checks if the previous action was done succesfully via TablesArchivingHistory
	,@BatchSize		INT = 1000 --Number of rows to be deleted in one batch. (Wait of 1 second between each batch)
	,@UseTruncatePartition BIT = NULL -- Use 1 to force full partition truncate within the range. Use 0 to delete rows manually Only. Use NULL for default, Truncate if found, delete otherwise
	,@Debug			BIT = 0
)
AS
BEGIN TRY
	DECLARE @OriginalObjectName SYSNAME = @Table
		, @SchemaName SYSNAME
		, @DbName SYSNAME
		, @TableName SYSNAME
		, @TableFullName SYSNAME
		, @RangeColumnType sysname 
		, @VerboseMode BIT =  IIF (SUSER_SNAME()  LIKE '%sqlagent', 0, 1)
		, @ArchiveSpeedRatio DECIMAL(8,3) = 1 -- Get from tokens a multiplier for speed
		, @ExecSQL NVARCHAR(max)

	SET NOCOUNT ON;

	--If one of the two ranges is null, Abort the whole thing. As it can mess up the query
	IF @RangeStart IS NULL OR @RangeEnd IS NULL
	BEGIN
		SELECT 'NULL @RangeStart OR @RangeEnd.' AS ABORT
		RETURN
	END

	--Get Table detailed name
	SELECT @DbName = [db_name]
	,@SchemaName = [schema_name]
	,@TableName = [object_name]
	,@TableFullName = [object_fullname]
	FROM DBA.dbo.FormatObjectName (@OriginalObjectName)

	IF @TableFullName IS NULL BEGIN
		SELECT 'ERROR trying to format the @ObjectName : '+@TableName+' AS DbName.SchemaName.ObjectName.' AS ERROR
		RETURN
	END

	--Get Archive speed ratio
	SELECT @ArchiveSpeedRatio = CAST(TokenValue AS  DECIMAL(8,3))
	FROM DBA.indus.Tokens WITH(NOLOCK)
	WHERE TokenName = 'ArchiveSpeedRatio'
	SET @ArchiveSpeedRatio = ISNULL(@ArchiveSpeedRatio, 1)
	SET @ArchiveSpeedRatio = IIF(@ArchiveSpeedRatio < 0.02, 0.02, @ArchiveSpeedRatio)
	IF @Debug = 1
		SELECT 'Delay between batches ''00:00:'+ CAST(CAST (ROUND(1/@ArchiveSpeedRatio, 3) AS DECIMAL (5,3)) AS VARCHAR(6))+'''' AS DelayRatio

	DECLARE @NbRowsToDelete INT = NULL -- Will stay unkown if no previous action foud in TablesArchivingHistory
	 
	IF @PrevArchiveAction IS NOT NULL
	BEGIN
		SELECT TOP 1 @NbRowsToDelete = NbRowsArchived
		FROM TablesArchivingHistory 
		WHERE TableName = @TableFullName
		AND	RangeStart = @RangeStart
		AND	RangeEnd = @RangeEnd
		AND RangeColumn = @RangeColumn
		AND	(@Filter IS NULL OR FilterUsed = @Filter)
		AND ActionMade = @PrevArchiveAction
		ORDER BY ArchivedDate DESC

		IF @NbRowsToDelete IS NULL BEGIN
			SELECT  'No Previous action found in History. Use @PrevArchiveAction = NULL to force the delete ' AS ERROR
			RETURN -1
		END

	END

	SET @ExecSQL =
	N'
	SELECT @RangeColumnTypeOUT= Data_Type+
		+CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL
			 THEN "("+CAST(CHARACTER_MAXIMUM_LENGTH AS SYSNAME)+")"
			 WHEN DATA_TYPE = "datetime2"
			 THEN "("+CAST(DATETIME_PRECISION AS SYSNAME)+")"
			 WHEN DATA_TYPE = "decimal"
			 THEN "("+CAST(NUMERIC_PRECISION_RADIX AS SYSNAME)+","+CAST(NUMERIC_SCALE AS SYSNAME)+")"
			ELSE ""
		END
	FROM @DbName.INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = "'+@TableName +'" 
	AND TABLE_SCHEMA = "'+@SchemaName +'" 
	AND COLUMN_NAME = "'+@RangeColumn +'" 
	'
	
	SET @ExecSQL = REPLACE(@ExecSQL, '"', '''')
	SET @ExecSQL = REPLACE(@ExecSQL, '@DbName', @DbName)

	IF @debug = 1 
		SELECT @ExecSQL AS 'SearchColumnType Query'
	
	EXEC sp_executesql @ExecSQL
		,N'@RangeColumnTypeOUT SYSNAME OUTPUT'
		,@RangeColumnTypeOUT = @RangeColumnType OUTPUT

	IF @VerboseMode = 1
		/*--*/PRINT 'Starting Delete of '+@TableFullName+' for intervals '+CAST(@RangeStart AS VARCHAR(50))+' to '+CAST(@RangeEnd AS VARCHAR(50))

	DECLARE @NbDeletedRows INT = 0
	
	--Truncate partition method to speed up deletion,  IF @UseTruncatePartition IS NULL, both truncate and regular deletion will be tried
	IF @UseTruncatePartition IS NULL OR @UseTruncatePartition = 1
	BEGIN
		CREATE TABLE #ArchiveTable_DeleteRows_DeletePartitions ( partition_number INT, RangeStart sql_variant, RangeEnd sql_variant, rows BIGINT)
		SET @ExecSQL = '
		USE '+QUOTENAME(@DbName)+';
		INSERT INTO #ArchiveTable_DeleteRows_DeletePartitions
		SELECT p.partition_number
			, prv.value as RangeStart
			, next_prv.value as RangeEnd
			, p.rows
		FROM sys.objects AS o
		INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		INNER JOIN sys.indexes AS i ON i.object_id = o.object_id
		INNER JOIN sys.partition_schemes AS ps ON i.data_space_id = ps.data_space_id
		INNER JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
		INNER JOIN sys.partition_range_values AS prv ON prv.function_id = pf.function_id
		INNER JOIN sys.partitions AS p ON o.object_id = p.object_id AND p.index_id = i.index_id AND p.partition_number -1 = prv.boundary_id
		OUTER APPLY  (
			SELECT TOP (1) *
			FROM  sys.partition_range_values AS next_prv
			WHERE next_prv.function_id = prv.function_id
			AND	next_prv.boundary_id > prv.boundary_id 
			ORDER BY next_prv.boundary_id  ASC
		) AS next_prv
		WHERE	o.name = @TableName
		AND		s.name = @SchemaName
		AND		o.type_DESC = ''USER_TABLE''
		AND		i.type_desc = ''CLUSTERED''
		AND		prv.value >= @RangeStart 
		AND		next_prv.value  <= @RangeEnd;
		'

		EXEC sp_executesql @ExecSQL
			,N'@TableName	SYSNAME
			,@SchemaName	SYSNAME
			,@RangeStart	SQL_VARIANT
			,@RangeEnd		SQL_VARIANT
			'
			,@TableName  = @TableName
			,@SchemaName = @SchemaName
			,@RangeStart = @RangeStart
			,@RangeEnd	 = @RangeEnd

		IF @debug = 1 BEGIN
				SELECT @ExecSQL AS 'SearchPartition Query'
				SELECT * FROM #ArchiveTable_DeleteRows_DeletePartitions
		END
					
		SET @NbDeletedRows = ISNULL((SELECT SUM (rows) as nb_rows FROM #ArchiveTable_DeleteRows_DeletePartitions), 0)
		 --> Aproximated as this will sum the rows from sys.partitions.[rows] which might not be accurate
		IF @PrevArchiveAction IS NOT NULL AND @NbRowsToDelete IS NOT NULL AND @NbRowsToDelete < @NbDeletedRows BEGIN
			SELECT  'More rows found in the partitions to purge than the number of rows archived previously ' AS ERROR
			RETURN -1
		END

		--Cannot truncate partition live until SQL Server 2014 at least. So need to do a schema switch and truncate that
		--Will be a lot easier with SQL 2016. Just Truncate with ONLINE option.
		DECLARE @Current_partition INT
		WHILE EXISTS (SELECT 1 FROM #ArchiveTable_DeleteRows_DeletePartitions) BEGIN
			--As we cannot directly truncate, have to switch the partition to a new purge table prevously created (very inconvenient)
			SET  @Current_partition = (SELECT TOP 1 partition_number FROM #ArchiveTable_DeleteRows_DeletePartitions ORDER BY partition_number)
			--Switch partition to exisitn purge table and truncate that purge table
			SET	@ExecSQL = 'ALTER TABLE '+@TableFullName+' SWITCH PARTITION '+CAST(@Current_partition AS VARCHAR)+' TO '+ QUOTENAME(@DbName)+'.purge.'+QUOTENAME(@TableName)+' PARTITION '+CAST(@Current_partition AS VARCHAR)+';
			TRUNCATE TABLE '+QUOTENAME(@DbName)+'.purge.'+QUOTENAME(@TableName)+';'

			DELETE #ArchiveTable_DeleteRows_DeletePartitions WHERE partition_number = @Current_partition

			IF @debug = 1 
				SELECT @ExecSQL AS 'Delete Partitions Loop'
			ELSE
				EXEC sp_executesql @ExecSQL

		END
	END

	--Regular Deletion, IF @UseTruncatePartition IS NULL, both truncate and regular deletion will be tried
	IF @UseTruncatePartition IS NULL OR @UseTruncatePartition = 0
	BEGIN
		SET @ExecSQL = '
		USE '+QUOTENAME(@DbName)+';
		DECLARE @Batch INT = '+CAST(@BatchSize AS VARCHAR(10))+'
		DECLARE @LastBatch INT = @Batch
		DECLARE @RangeStart '+@RangeColumnType+'  = CAST( @RangeStartIN	 AS '+@RangeColumnType+')
		DECLARE @RangeEnd   '+@RangeColumnType+'  = CAST( @RangeEndIN	 AS '+@RangeColumnType+')

		WHILE @LastBatch >= @Batch  AND (@NbDeletedRows <= @NbRowsToDelete OR @NbRowsToDelete IS NULL)' -- > Stop deletion when no lines found or when @NbRowsToDelete has been reached
		+'
		BEGIN
		
			IF	@NbDeletedRows % (@Batch*5) < @Batch --Every 5 batches, check AlwaysOn Lag
			BEGIN 
				IF  DBA.supervision.MaxAlwaysOnLatency(''' + @DbName +''') > 10
				BEGIN
					--Additional wait for alwaysOn;
					SELECT ''Waiting for alwaysOn delay'' AS INFO
					WAITFOR DELAY ''00:00:30'';
				END
			END

			DELETE TOP (@Batch)
			FROM '+@TableFullName+' WITH(ROWLOCK)
			WHERE '+QUOTENAME(@RangeColumn)+' >= @RangeStart AND '+QUOTENAME(@RangeColumn)+' < @RangeEnd'
			+ISNULL(' AND '+@Filter, '')+'
			;
		
			SET @LastBatch = @@ROWCOUNT;
			SET @NbDeletedRows+=@LastBatch;
			
			IF @VerboseMode = 1
				PRINT CAST(@NbDeletedRows AS VARCHAR) + '' / '' + ISNULL(CAST(@NbRowsToDelete AS VARCHAR), ''??'')+'' rows deleted.''
			
			IF @LastBatch >= @Batch
			BEGIN
				WAITFOR DELAY ''00:00:'+ CAST(CAST (ROUND(1/@ArchiveSpeedRatio, 3) AS DECIMAL (5,3)) AS VARCHAR(6))+''';
				--Checkpoint
				IF	@NbDeletedRows % (@Batch*20) < @Batch --Every 20 batches, check AlwaysOn Lag
				BEGIN
					CHECKPOINT;
					IF @VerboseMode = 1
						PRINT ''CHECKPOINT''
				END
			END

		END -- end while
		SET @NbDeletedRowsOUT = @NbDeletedRows;
		'

		IF @debug = 1 
		BEGIN
			SELECT ' -- Debug inserted declarations
			DECLARE 
				@NbDeletedRows INT = '+CAST( @NbDeletedRows  AS VARCHAR)+'
				,@NbRowsToDelete INT  = '+ISNULL(CAST( @NbRowsToDelete  AS VARCHAR) , 'NULL')+'
				,@NbDeletedRowsOUT INT = 0 
				,@RangeStart	SQL_VARIANT = '+CAST( @RangeStart  AS VARCHAR)+'
				,@RangeEnd		SQL_VARIANT = '+CAST( @RangeEnd  AS VARCHAR)+'
				,@VerboseMode BIT =  '+CAST (@VerboseMode AS CHAR(1))+'
			'+@ExecSQL AS 'Batch Delete Query'
		END
		ELSE
			EXEC sp_executesql @ExecSQL
				,N'@NbDeletedRows INT
				,@NbRowsToDelete INT
				,@NbDeletedRowsOUT INT = 0 OUTPUT
				,@RangeStartIN		SQL_VARIANT
				,@RangeEndIN		SQL_VARIANT
				,@VerboseMode	BIT'
				,@NbDeletedRows = @NbDeletedRows
				,@NbRowsToDelete = @NbRowsToDelete
				,@NbDeletedRowsOUT = @NbDeletedRows OUTPUT
				,@RangeStartIN = @RangeStart
				,@RangeEndIN	 = @RangeEnd
				,@VerboseMode = @VerboseMode

	END

	--Save ArchivingAction and parameters
	IF @Debug = 0
		INSERT INTO TablesArchivingHistory(
			ArchivedDate 
			,TableName 
			,RangeColumn
			,RangeStart
			,RangeEnd
			,FilterUsed
			,NbRowsArchived
			,ActionMade
		)
		VALUES(
			GETDATE()
			,@TableFullName
			,@RangeColumn
			,@RangeStart
			,@RangeEnd
			,@Filter
			,@NbDeletedRows
			,'DeleteRows' --> Name of the proc to identify the action made
		)
	IF @Debug = 1
		SELECT
			GETDATE()					   AS ArchivedDate
			,@TableFullName				   AS TableName 
			,@RangeColumn				   AS RangeColumn
			,@RangeStart				   AS RangeStart
			,@RangeEnd					   AS RangeEnd
			,@Filter					   AS FilterUsed
			,@NbDeletedRows				   AS NbRowsArchived
			,'DeleteRows'				   AS ActionMade

	RETURN @NbDeletedRows; --Returns the number of rows archived. Or -1 if something went wrong

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO