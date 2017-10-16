IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'ArchiveTable_CopyIntoTable')
      DROP PROCEDURE [dbo].[ArchiveTable_CopyIntoTable]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Proc Archive Table into another Table. Only copies the data without deleting rows (use dba.dbo.ArchiveTable_DeleteRows  for deletion)

Needs a specific inclusive range for wich to archive the table (like a range of ids or dates)

Returns -1 if the archive failed
Returns the number of rows archived otherwise

Results are also saved in dba.dbo.TablesArchivingHistory

Then use dba.dbo.ArchiveTable_DeleteRows  for deletion

Example to delete a month of data:
DECLARE @nbArchivedRows INT 
EXEC @nbArchivedRows = dba.dbo.ArchiveTable_CopyIntoTable
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
	2017-02-13 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[ArchiveTable_CopyIntoTable]
(
	@Table		sysname -->Table from where the data is copied. Specify 'DBName.schema.TableName' if possible
	,@DestinationTable sysname -->Table where the data will be copied to. Specify 'DBName.schema.TableName' if possible
	,@RangeColumn	sysname --> The column on which to check the range, can be a date, an id...
	,@RangeStart	sql_variant -->inclusive can be a date, an id...
	,@RangeEnd		sql_variant -->exclusive can be a date, an id...
	,@Filter		VARCHAR(5000)= NULL --Optional, if only rows respecting this condition should be archived
	,@Excluded_columns	VARCHAR(200)= NULL --Optional, if some colums don't need to be archived. To save space
	,@BatchSize		INT = 1000 --Number of rows to be copied in one batch. (Wait of 1 second between each batch)
	,@Debug			BIT = 0 -- no real write action, just displays the execs
)
AS
BEGIN TRY
	DECLARE  @VerboseMode BIT =  IIF (SUSER_SNAME()  LIKE '%sqlagent', 0, 1)
		,@RangeColumnType sysname 
		, @ArchiveSpeedRatio DECIMAL(8,3) = 1 -- Get from tokens a multiplier for speed

	
	--If one of the two ranges is null, abort the whole thing. As it can mess up the query
	IF @RangeStart IS NULL OR @RangeEnd IS NULL
	BEGIN
		SELECT 'NULL @RangeStart OR @RangeEnd.' AS ABORT
		RETURN
	END

	SET NOCOUNT ON;

	DECLARE @OriginalObjectName SYSNAME = @Table
		, @SchemaName SYSNAME
		, @DbName SYSNAME
		, @TableName SYSNAME
		, @TableFullName SYSNAME

		, @Destination_SchemaName SYSNAME
		, @Destination_DbName SYSNAME
		, @Destination_TableName SYSNAME
		, @Destination_TableFullName SYSNAME

	SELECT 
		 @DbName = [db_name]
		,@SchemaName = [schema_name]
		,@TableName = [object_name]
		,@TableFullName = [object_fullname]
	FROM DBA.dbo.FormatObjectName (@OriginalObjectName)

	SELECT 
	     @Destination_DbName = [db_name]
		,@Destination_SchemaName = [schema_name]
		,@Destination_TableName = [object_name]
		,@Destination_TableFullName = [object_fullname]
	FROM DBA.dbo.FormatObjectName (@DestinationTable)

	--Get Archive speed ratio
	SELECT @ArchiveSpeedRatio = CAST(TokenValue AS  DECIMAL(8,3))
	FROM DBA.indus.Tokens WITH(NOLOCK)
	WHERE TokenName = 'ArchiveSpeedRatio'
	SET @ArchiveSpeedRatio = ISNULL(@ArchiveSpeedRatio, 1)
	SET @ArchiveSpeedRatio = IIF(@ArchiveSpeedRatio < 0.02, 0.02, @ArchiveSpeedRatio)
	IF @Debug = 1
		SELECT 'Delay between batches ''00:00:'+ CAST(CAST (ROUND(1/@ArchiveSpeedRatio, 3) AS DECIMAL (5,3)) AS VARCHAR(6))+'''' AS DelayRatio

	--Format potential excluded column list into a string which can be used AS NOT IN(...) in next query
	IF @Excluded_columns IS NOT NULL
		SET @Excluded_columns = ''''+REPLACE(REPLACE(@Excluded_columns, ' ', ''), ',', ''',''')+''''

	--Check if the table exists and get the columns names
	DECLARE @ExecSQL NVARCHAR(max)
	DECLARE @ArchivedColumns VARCHAR(2000) = NULL
	SET @ExecSQL =
	N'
	SELECT @ArchivedColumnsOUT =(
		SELECT "["+c.name+"],"
			AS [text()]
		FROM @DbName.sys.tables AS t 
		INNER JOIN @DbName.sys.schemas AS s ON t.schema_id = s.schema_id
		INNER JOIN @DbName.sys.columns AS c ON c.object_id = t.object_id
		WHERE s.name ="'+@SchemaName+'" 
		AND t.name LIKE "'+@TableName + CASE WHEN @DbName = 'TempDb' THEN '%' ELSE '' END +'"
		'+ISNULL('AND c.name NOT IN ('+@Excluded_columns+')', '')
		+'For XML PATH ("")
	)
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
		SELECT @ExecSQL AS 'SearchTableColumns Query'
	
	EXEC sp_executesql @ExecSQL
		,N'@ArchivedColumnsOUT VARCHAR(2000) OUTPUT
		,@RangeColumnTypeOUT SYSNAME OUTPUT'
		,@ArchivedColumnsOUT = @ArchivedColumns OUTPUT
		,@RangeColumnTypeOUT = @RangeColumnType OUTPUT
		
	IF @ArchivedColumns IS NULL BEGIN
		SELECT 'TABLE '+@TableFullName+' NOT FOUND' AS ERROR
		RETURN -1
	END
	SET @ArchivedColumns = LEFT (@ArchivedColumns, LEN(@ArchivedColumns) -1)
		
	DECLARE @NbArchivedRows INT = 0

	IF @VerboseMode = 1
		/*--*/PRINT 'Starting insert from '+@TableFullName+' INTO '+@Destination_TableFullName +' for intervals '+CAST(@RangeStart AS VARCHAR(50))+' to '+CAST(@RangeEnd AS VARCHAR(50))
	
	SET @ExecSQL = '
	USE '+QUOTENAME(@Destination_DbName)+';
	CREATE TABLE #Inserted (RangeColumn '+@RangeColumnType+');

	DECLARE @Batch INT = '+CAST(@BatchSize AS VARCHAR(10))+'
	DECLARE @LastBatch INT = @Batch
	DECLARE @LastInsertedLine '+@RangeColumnType+';
	DECLARE @RangeStart '+@RangeColumnType+'  = CAST( @RangeStartIN	 AS '+@RangeColumnType+')
	DECLARE @RangeEnd   '+@RangeColumnType+'  = CAST( @RangeEndIN	 AS '+@RangeColumnType+')
	WHILE @LastBatch >= @Batch ' -- > Stop deletion when no lines found or when @NbRowsToDelete has been reached
	+'
	BEGIN
			IF	@NbArchivedRows % (@Batch*5) < @Batch --Every 5 batches, check AlwaysOn Lag
			BEGIN 
				IF  DBA.supervision.MaxAlwaysOnLatency(''' + @DbName +''') > 10
				BEGIN
					--Additional wait for alwaysOn;
					SELECT ''Waiting for alwaysOn delay'' AS INFO
					WAITFOR DELAY ''00:00:30'';
				END
			END

			INSERT INTO '+@Destination_TableFullName+' (' + @ArchivedColumns + ')
				OUTPUT INSERTED.'+@RangeColumn+' INTO  #Inserted(RangeColumn)
			SELECT TOP(@Batch) WITH TIES ' + @ArchivedColumns
			+'
			FROM '+@TableFullName+' WITH(NOLOCK)
			WHERE '+QUOTENAME(@RangeColumn)+' >= @RangeStart AND '+QUOTENAME(@RangeColumn)+' < @RangeEnd 
			AND (@LastInsertedLine IS NULL OR '+QUOTENAME(@RangeColumn)+' > @LastInsertedLine )'
			+ISNULL(' AND '+@Filter, '')+'
			ORDER BY '+@RangeColumn+' ASC
		
			SET @LastBatch = @@ROWCOUNT;
			SET @NbArchivedRows+=@LastBatch;

			SELECT @LastInsertedLine =MAX(RangeColumn) FROM #Inserted
			DELETE #Inserted

			IF @VerboseMode = 1
				PRINT CAST(@NbArchivedRows AS VARCHAR) +'' rows inserted.''
 
			IF @LastBatch >= @Batch
			BEGIN
				WAITFOR DELAY ''00:00:'+ CAST(CAST (ROUND(1/@ArchiveSpeedRatio, 3) AS DECIMAL (5,3)) AS VARCHAR(6))+''';
				--Checkpoint
				IF	@NbArchivedRows % (@Batch*20) < @Batch
				BEGIN
					CHECKPOINT;
					IF @VerboseMode = 1
						PRINT ''CHECKPOINT''
				END
			END
	END -- end while
	SET @NbArchivedRowsOUT = @NbArchivedRows;
	'

	IF @debug = 1 
		SELECT '-- Debug inserted declarations
			DECLARE @NbArchivedRows INT = 0
			,@NbArchivedRowsOUT INT = 0 
			,@RangeStartIN	sql_variant ='+ CAST(@RangeStart AS VARCHAR)+'
			,@RangeEndIN	sql_variant ='+ CAST(@RangeEND AS VARCHAR)+'
			,@VerboseMode BIT =  '+CAST (@VerboseMode AS CHAR(1))
			+ @ExecSQL AS 'Insert Into Query'
	ELSE BEGIN
		DECLARE @Params nvarchar(500) = 
			N'@NbArchivedRows INT
			,@NbArchivedRowsOUT INT = 0 OUTPUT
			,@RangeStartIN	sql_variant
			,@RangeEndIN	sql_variant
			,@VerboseMode	BIT'
		EXEC sp_executesql @ExecSQL
			,@Params
			,@NbArchivedRows = @NbArchivedRows
			,@NbArchivedRowsOUT = @NbArchivedRows OUTPUT
			,@RangeStartIN	= @RangeStart
			,@RangeEndIN	= @RangeEnd
			,@VerboseMode = @VerboseMode
	END

	
	--Save ArchivingAction and parameters
	IF @Debug = 0
		INSERT INTO TablesArchivingHistory(
			ArchivedDate 
			,TableName 
			,RangeStart
			,RangeEnd
			,RangeColumn
			,FilterUsed
			,Excluded_columns
			,NbRowsArchived
			,ActionMade
			,Destination
		)
		VALUES(
			GETDATE()
			,@TableFullName
			,@RangeStart
			,@RangeEnd
			,@RangeColumn
			,@Filter
			,@Excluded_columns
			,@NbArchivedRows
			,'CopyIntoTable'  --> Name of the proc to identify the action made
			,@Destination_TableFullName
		)
	IF @Debug = 1
		SELECT
			GETDATE()					   AS ArchivedDate
			,@TableFullName				   AS TableName 
			,@RangeStart				   AS RangeStart
			,@RangeEnd					   AS RangeEnd
			,@RangeColumn				   AS RangeColumn
			,@Filter					   AS FilterUsed
			,@Excluded_columns			   AS Excluded_columns
			,@NbArchivedRows			   AS NbRowsArchived
			,'CopyIntoTable'			   AS ActionMade
			,@Destination_TableFullName	   AS Destination


	RETURN @NbArchivedRows; --Returns the number of rows archived. Or -1 if something went wrong

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO