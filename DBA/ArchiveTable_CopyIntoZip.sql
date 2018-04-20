IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'ArchiveTable_CopyIntoZip')
      DROP PROCEDURE [dbo].[ArchiveTable_CopyIntoZip]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Proc Archive Table into Zip. Only copies the data without deleting rows (use dba.dbo.ArchiveTable_DeleteRows  for deletion)

Needs a specific inclusive range for wich to archive the table (like a range of ids or dates)

Returns -1 if the archive failed
Returns the number of rows archived otherwise

Results are also saved in dba.dbo.TablesArchivingHistory

Then use dba.dbo.ArchiveTable_DeleteRows  for deletion

Example to delete a month of data:
DECLARE @nbArchivedRows INT 
EXEC @nbArchivedRows = dba.dbo.ArchiveTable_CopyIntoZip
 @Table = 'Betclick.dbo.BigTableToArchive'
,@RangeColumn = 'date'
,@RangeStart = {d'2016-02-01'}
,@RangeEnd = {d'2016-03-01'}

Policicy override:
GRANT EXECUTE ON [dbo].[ArchiveTable_CopyIntoZip] TO [nobody];

History:
	2018-02-27 - XMO - Varchar Rangecolumn
	2018-02-22 - XMO - Fix Range columns for BCP back to ODBC notation
	2017-11-27 - XMO - Add @RangeEndIncluded Param 
	2016-08-18 - MTG
		1) fix the cast of the columns to keep length and unicode property
		2) format the date ranges in the filename
		3) Use special characters as column and row separators : ¤ and {§}
		4) seperate columns names and data in 2 files inside the zip
	2016-07-04 - XMO - Switch ranges to sql_variants
	2016-06-13 - XMO - Creation
	
*/
CREATE PROCEDURE [dbo].[ArchiveTable_CopyIntoZip]
(
	@Table		sysname -->Specify 'DBName.schema.TableName' if possible
	,@RangeColumn	sysname --> The column on which to check the range, can be a date, an id...
	,@RangeStart	sql_variant -->inclusive can be a date, an id...
	,@RangeEnd		sql_variant -->exclusive can be a date, an id... 
	,@Filter		VARCHAR(5000)= NULL --Optional, if only rows respecting this condition should be archived
	,@ZipPath			VARCHAR(500) = 'default' -- Default = CAST(DBA.Indus.GetTokenValue('Path_TableArchives')  AS VARCHAR(100)))+'\'+@TableFullName+'\'
	,@Excluded_columns	VARCHAR(200)= NULL --Optional, if some colums don't need to be archived. To save space
	,@RangeEndIncluded BIT = 0 -- Put 1 to include RangeEnd instead of the default excluded behavior
	,@Debug			BIT = 0
)
AS
BEGIN TRY
	DECLARE @FormatedRangeStart VARCHAR(50); --For the FileName
	DECLARE @FormatedRangeEnd VARCHAR(50);	 --For the FileName
	DECLARE @StrRangeStart VARCHAR(100); --For the BCP cmd
	DECLARE @StrRangeEnd VARCHAR(100);	 --For the BCP cmd

	--If one of the two ranges is null, abort the whole thing. As it can mess up the query
	IF @RangeStart IS NULL OR @RangeEnd IS NULL
	BEGIN
		SELECT 'NULL @RangeStart OR @RangeEnd.' AS ABORT
		RETURN
	END

	--Format the ranges for the filename And BCP
	IF SQL_VARIANT_PROPERTY ( @RangeStart , 'BaseType') IN ('datetime', 'datetime2')
	BEGIN
		SET @FormatedRangeStart = FORMAT(CAST(@RangeStart AS DATETIME), 'yyyyMMddTHH-mm-ss') ;
		SET @FormatedRangeEnd = FORMAT(CAST(@RangeEnd AS DATETIME), 'yyyyMMddTHH-mm-ss') ;
		SET @StrRangeStart	=	'{ts'''+CONVERT(varchar(100), @RangeStart, 121)+'''}'
		SET @StrRangeEnd	=	'{ts'''+CONVERT(varchar(100), @RangeEnd, 121)+'''}'
		--Replace plain days to have shorter names + backward compatible
		SET @FormatedRangeStart = REPLACE(@FormatedRangeStart, 'T00-00-00', '')
		SET @FormatedRangeEnd	= REPLACE(@FormatedRangeEnd, 'T00-00-00', '')
	END
	ELSE IF SQL_VARIANT_PROPERTY ( @RangeStart , 'BaseType') IN ('date') -- Probably never used. Dates are identified as datetime
	BEGIN
		SET @FormatedRangeStart = FORMAT(CAST(@RangeStart AS DATE), 'yyyyMMdd') ;
		SET @FormatedRangeEnd = FORMAT(CAST(@RangeEnd AS DATE), 'yyyyMMdd') ;
		SET @StrRangeStart	=	'{d'''+CONVERT(varchar(100), @RangeStart, 121)+'''}'
		SET @StrRangeEnd	=	'{d'''+CONVERT(varchar(100), @RangeEnd, 121)+'''}'
	END
	ELSE IF SQL_VARIANT_PROPERTY ( @RangeStart , 'BaseType') IN ('char', 'varchar', 'nchar', 'nvarchar')
	BEGIN
		SET @FormatedRangeStart = CAST(@RangeStart AS VARCHAR(100));
		SET @FormatedRangeEnd	= CAST(@RangeEnd AS VARCHAR(100));
		SET @StrRangeStart	= ''''+CAST( @RangeStart AS NVARCHAR(100))+'''';
		SET @StrRangeEnd	= ''''+CAST( @RangeEnd AS NVARCHAR(100))+'''';
	END
	ELSE
	BEGIN
		SET @FormatedRangeStart = CAST(@RangeStart AS VARCHAR);
		SET @FormatedRangeEnd = CAST(@RangeEnd AS VARCHAR);
		SET @StrRangeStart = CAST( @RangeStart AS VARCHAR(100))
		SET @StrRangeEnd = CAST( @RangeEnd AS VARCHAR(100))
	END

	DECLARE @OriginalObjectName SYSNAME = @Table
		, @SchemaName SYSNAME
		, @DbName SYSNAME
		, @TableName SYSNAME
		, @TableFullName SYSNAME

	SELECT @DbName = [db_name]
	,@SchemaName = [schema_name]
	,@TableName = [object_name]
	,@TableFullName = [object_fullname]
	FROM DBA.dbo.FormatObjectName (@OriginalObjectName)


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
	'
	SET @ExecSQL = REPLACE(@ExecSQL, '"', '''')
	SET @ExecSQL = REPLACE(@ExecSQL, '@DbName', @DbName)

	IF @debug = 1 
		SELECT @ExecSQL AS 'SearchTableColumns Query'
	
	EXEC sp_executesql @ExecSQL
		,N'@ArchivedColumnsOUT VARCHAR(2000) OUTPUT'
		,@ArchivedColumnsOUT = @ArchivedColumns OUTPUT
		
	IF @ArchivedColumns IS NULL BEGIN
		SELECT 'TABLE '+@TableFullName+' NOT FOUND' AS ERROR
		RETURN -1
	END
	SET @ArchivedColumns = LEFT (@ArchivedColumns, LEN(@ArchivedColumns) -1)
		
	--Get indus token path if it exists
	IF ISNULL(@ZipPath , 'default') = 'default'
		SET @ZipPath = (SELECT  CAST(DBA.Indus.GetTokenValue('Path_TableArchives')  AS VARCHAR(100)))+'\'+@TableFullName+'\'
	IF @ZipPath IS NULL BEGIN
		SELECT 'Token Indus Path_TableArchives unavailable'
		RETURN -1
	END

	DECLARE @strCmdShell VARCHAR(4096);
	DECLARE @FileName VARCHAR(256), @ColumnsFileName VARCHAR(256)
	DECLARE @ExecOutput TABLE ([Output] VARCHAR(255));
	DECLARE @NbArchivedRows INT = -1

	SET @strCmdShell = 'if not exist "'+@ZipPath+'" mkdir '+@ZipPath
	IF @Debug = 1
		SELECT @strCmdShell AS CreateZipFolder
	IF @Debug = 0
		EXEC xp_cmdshell @strCmdShell, no_output
	
		
	SELECT 	@FileName = '['+ @TableFullName + ']_'+@RangeColumn +'_'+ @FormatedRangeStart +'-'+ @FormatedRangeEnd + '.csv';
	SELECT	@ColumnsFileName = replace(@FileName,'.csv', '_ColumnsNames.csv') 
	
	--Remove unallowed Windows chars from the filename
	WHILE PatIndex('%[<>:"/\|?*]%', @FileName) > 0
		SET @FileName = STUFF(@FileName, PatIndex('%[<>:"/\|?*]%', @FileName), 1, '_')

	--Prepare the bcp of the Columns Names
	SET @ExecSQL = 'SELECT '+ ''''+REPLACE(REPLACE(REPLACE(@ArchivedColumns, ']', ''), '[', ''), ',', ''',''')+''''

	--BCP OUT
	SET @strCmdShell = 'BCP "'+@ExecSQL+'" queryout "'+ @ZipPath + @ColumnsFileName + '" -T -c -S ' + @@SERVERNAME;

	IF @Debug = 1
		SELECT @strCmdShell AS [Columns Names BCP_command]
	IF @Debug = 0 BEGIN
		INSERT INTO @ExecOutput (Output)
			EXEC xp_cmdshell @strCmdShell

		SET @NbArchivedRows = (SELECT CAST(replace(Output, ' rows copied.','') AS INT) FROM @ExecOutput WHERE [Output] LIKE '% rows copied.')
	
		IF @NbArchivedRows IS NULL
		BEGIN
			SELECT  'Error during the columns names BCP' AS ERROR
			SELECT * FROM @ExecOutput 
			RETURN -1
		END
	END

	-- Prepare SELECT query of the rows to archive  (! no line return allowed for the bcp command !)  -- removed later		
	SET @ExecSQL =
	'SELECT ' + @ArchivedColumns
	+' FROM '+@TableFullName+' WITH(NOLOCK)	WHERE '
	+@RangeColumn+' >= '+@StrRangeStart+' AND '+@RangeColumn+CASE WHEN @RangeEndIncluded=1 THEN ' <= ' ELSE ' < ' END+@StrRangeEnd
	+ISNULL(' AND '+@Filter, '')

	IF @debug = 1 BEGIN
		SELECT @ExecSQL AS SelectDebug
		SET @ExecSQL = REPLACE(@ExecSQL, 'SELECT', 'SELECT TOP (99)')
		EXEC sp_executesql @ExecSQL
		SET @ExecSQL = REPLACE(@ExecSQL, 'SELECT TOP (99)', 'SELECT')
	END
	
	--BCP OUT
	SET @strCmdShell = 'BCP "'+@ExecSQL+'" queryout "'+ @ZipPath + @FileName + '" -T -c -t ¤ -r {§} -S ' + @@SERVERNAME;-- the -c is a type of storage technique, -r and -t are row and term(column) delimiters
	
	IF (LEN(@strCmdShell) >4095) BEGIN SELECT '@strCmdShell size limit reached. Query Canceled' AS ERROR 	RETURN -1 END

	--Remove potential carriage return in query (that could be introduced in the @filter)
	SET @strCmdShell = REPLACE(@strCmdShell, CHAR(10), ' ')
	SET @strCmdShell = REPLACE(@strCmdShell, CHAR(13), ' ')
		
	DELETE @ExecOutput

	IF @Debug = 1
		SELECT @strCmdShell AS [BCP_command]
	IF @Debug = 0 BEGIN
		INSERT INTO @ExecOutput (Output)
			EXEC xp_cmdshell @strCmdShell

		SET @NbArchivedRows = (SELECT CAST(replace(Output, ' rows copied.','') AS INT) FROM @ExecOutput WHERE [Output] LIKE '% rows copied.')
	
		IF @NbArchivedRows IS NULL
		BEGIN
			SELECT  'Error during the BCP' AS ERROR
			SELECT * FROM @ExecOutput 
			RETURN -1
		END
	END

	SET @strCmdShell = '""C:\Program Files\7-Zip\7z.exe" a -tzip "' +@ZipPath +replace(@FileName,'.csv','.7z')+ '" "' +@ZipPath + replace(@FileName,'.csv', '*.csv') +'""';
	
	--Zip the csv file using 7z
	DELETE @ExecOutput
	IF @Debug = 1
		SELECT @strCmdShell AS [7z_command]
	IF @Debug = 0 AND @NbArchivedRows > 0 BEGIN
		INSERT INTO @ExecOutput (Output)
			EXEC xp_cmdshell @strCmdShell

		--Test if 7zip went fine
		IF NOT EXISTS(SELECT 1 FROM @ExecOutput WHERE Output like '%Everything is Ok%')
		BEGIN
			SELECT  'Error during the 7zipping' AS ERROR
			SELECT * FROM @ExecOutput 
			RETURN -1
		END
	END
	
	--IF Zip OK, DELETE the .csv file
	SET @strCmdShell = 'del /Q "' + @ZipPath + @FileName+'"';
	EXEC xp_cmdshell @strCmdShell, no_output

	SET @strCmdShell = 'del /Q "' + @ZipPath + @ColumnsFileName+'"';
	EXEC xp_cmdshell @strCmdShell, no_output
	
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
			,'CopyIntoZip'  --> Name of the proc to identify the action made
			,@ZipPath +replace(@FileName,'.csv','.7z')
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
			,'CopyIntoZip'				   AS ActionMade
			,@ZipPath +replace(@FileName,'.csv','.7z')	   AS Destination


	RETURN @NbArchivedRows; --Returns the number of rows archived. Or -1 if something went wrong

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

