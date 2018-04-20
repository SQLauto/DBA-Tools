IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('Tools') AND name = 'AddIndex')
      DROP PROCEDURE [Tools].[AddIndex]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Add index with DBA template

Description:
Helps auto generate index creation script :
	-Generates right IndexName according to indexType and DBA Naming Convention
	-Adds the correct filegroup (even for partitionned tables)
	-Natively Idempotent 
	-Automatically uses 'Live DB' parameters

Parameters :
	- @Table : TableName with or without schema name. Add DbName.Schema.TableName if needed (default is current DB and dbo schema)
	- @columns : String of successive columns names (in order) for the index, separated by a coma. Add 'DESC' statements where needed.
	- @Debug=1 will give you the script to execute. While @Debug=0 will also execute it

History:	
	2017-10-20 - XMO - Fix if exists on index
	2017-09-01 - XMO - Change IF exists management
	2017-07-21 - XMO - TempDb special cases
	2016-12-15 - XMO - Dynamic FileGroup/partition search, changed clustered naming from PK to IX
	2016-07-28 - XMO - Little rework with FormatObjectName and new template
	2016-06-02 - XMO - Fix for cluster and unique
	2016-05-31 - XMO - Fix PK Names
	2016-05-31 - XMO - Fix for TempDb Indexes
	2016-04-29 - XMO - Add Filtered Index, Unique Type and FillFactor
	2016-04-25 - XMO - Added IndexName force
	2016-04-21 - XMO - Add Cluster_type and included columns
	2016-04-04 - XMO - Creation
*/
CREATE PROCEDURE [Tools].[AddIndex]
(
	@Table sysname
	,@columns VARCHAR(200)
	,@included_columns VARCHAR(500) = NULL --> (If any)
	,@index_type VARCHAR(50) = 'NONCLUSTERED' --> [PRIMARY KEY] / [UNIQUE] [CLUSTERED/NONCLUSTERED]
	,@Filter VARCHAR(200) = '' --> No need to add the WHERE
	,@FillFactor TINYINT = 90
	,@Data_Compression SYSNAME = NULL --> Put ROW / PAGE if any, or leave null
	,@IndexName sysname = 'default' --> To force a specific IndexName
	,@Debug BIT = 0 --> To display the result without executing
)
AS
BEGIN TRY
	DECLARE @OriginalObjectName SYSNAME = @Table
	, @SchemaName SYSNAME
	, @DbName SYSNAME
	, @ObjectFullName SYSNAME
	, @ObjectName SYSNAME

	SELECT @DbName = [db_name]
	,@SchemaName = [schema_name]
	,@ObjectName = [object_name]
	,@ObjectFullName = [object_fullname] --> the schema and fullname will eventually be updated after the first query
	FROM DBA.dbo.FormatObjectName (@OriginalObjectName)
	
	IF @ObjectFullName IS NULL 
		BEGIN SELECT 'ERROR trying to format the @ObjectName : '+@ObjectName+' AS DbName.SchemaName.ObjectName.' AS ERROR RETURN END
	IF @DbName = ''
		BEGIN SELECT 'Please specify explicitly your Database for : '+@ObjectName+' AS DbName.SchemaName.ObjectName.' AS ERROR RETURN END

	DECLARE @Index_prefix VARCHAR(5)
	SET @Index_type = LTRIM(RTRIM(@Index_type))

	--Check index type, PRIMARY KEY NONCLUSTERED NOT ALLOWED
	IF @Index_type ='PRIMARY KEY'
		SET @Index_prefix = 'PK_'
	ELSE IF @Index_type IN('UNIQUE', 'UNIQUE CLUSTERED', 'UNIQUE NONCLUSTERED')
		SET @Index_prefix = 'UQ_'
	ELSE IF @Index_type IN('CLUSTERED','NONCLUSTERED')
		SET @Index_prefix = 'IX_'
	ELSE
		BEGIN SELECT 'Unknown @Index_type. Only use [PRIMARY KEY] / [UNIQUE] [CLUSTERED/NONCLUSTERED]' AS ERROR RETURN END

	--Define is the index is clustered or not
	DECLARE @IsClustered BIT =
	CASE 
		WHEN @Index_type IN('CLUSTERED','UNIQUE CLUSTERED','PRIMARY KEY') THEN 1
		ELSE 0
	END
 
	--Filtered Index ?
	IF @Filter IS NOT NULL AND @Filter != '' BEGIN
		IF  @IsClustered =1 BEGIN SELECT 'Filtered indexes have to be NONCLUSTERED' AS ERROR RETURN END --but can be Unique
		SET @Index_prefix = 'IF_'
		SET @Filter = replace(@Filter, 'WHERE', '')
		SET @Filter = '	
		WHERE '+@Filter
	END
	
	--Generate index name if none was forced
	IF @IndexName = 'default' BEGIN
		SET @IndexName = @Index_prefix+@ObjectName
		--No column name suffix for PKs
		IF @Index_prefix != 'PK_'
		BEGIN
			SET @IndexName +='_'+
			replace(
				replace(
					replace(
						replace(
							replace(
								replace(
									replace(
										replace(
											@columns
										, ']', '')
									, '[', '')
								, ' ASC', '')
							, ' DESC', '')
						, ',', '_')
					, ' ', '')
				, CHAR(13), '') --NewLine
			, CHAR(10), '') --NewLine
		END
	END

	IF @Debug = 1
		SELECT @IndexName AS [Suggested IndexName]


	DECLARE @DisplayedQuery NVARCHAR(2000) = ''
	DECLARE @ExecSQL NVARCHAR(2000) = ''
	
	DECLARE @USE_DB_Str NVARCHAR(50) = CASE WHEN  @DbName NOT IN ('',DB_NAME(),'TempDb' )  THEN 'USE '+QUOTENAME(@DbName)+';'+CHAR(10) ELSE '' END
		
	--Find adequate Filegroup / partition scheme
	DECLARE @IndexFileGroup SYSNAME
	SET @ExecSQL=@USE_DB_Str+'
	SELECT TOP 1 @IndexFileGroupOUT = QUOTENAME(name)
	FROM sys.filegroups
	ORDER BY CASE
		WHEN @IsClustered=1 AND name = ''DATA'' THEN 1
		WHEN @IsClustered=0 AND name IN (''INDEX'', ''SECONDARY'') THEN 1
		WHEN is_default = 1 THEN 2 --Usually PRIMARY
		ELSE 3
	END
	
	DECLARE @ObjectId INT = OBJECT_ID(@SchemaName+''.''+@ObjectName)
	IF @ObjectId IS NOT NULL
	SELECT TOP 1 
		@IndexFileGroupOUT =
		CASE
			WHEN @IsClustered = 1 THEN REPLACE(ps.name, ''INDEX'', ''DATA'')
			WHEN @IsClustered = 0 THEN REPLACE(ps.name, ''DATA'', ''INDEX'')
		END
		+''(''+c.name+'')''
	FROM sys.indexes AS i WITH(NOLOCK)
	INNER JOIN sys.partition_schemes AS ps WITH(NOLOCK) ON i.data_space_id = ps.data_space_id
	INNER JOIN sys.index_columns AS ic WITH(NOLOCK) ON ic.object_id = i.object_id AND ic.index_id = i.index_id
	INNER JOIN sys.columns AS c WITH(NOLOCK) ON c.object_id = i.object_id AND c.column_id = ic.column_id
	WHERE i.object_id = @ObjectId
	AND ps.type = ''PS''
	AND ic.partition_ordinal = 1

	'

	IF @debug = 1 
		SELECT @ExecSQL AS DebugQuery_FindFileGroup

	EXEC sp_executesql @ExecSQL
		, N'@IndexFileGroupOUT sysname OUTPUT
			,@IsClustered BIT
			,@SchemaName SYSNAME
			,@ObjectName SYSNAME
			'
		, @IndexFileGroupOUT = @IndexFileGroup OUTPUT
		, @IsClustered = @IsClustered
		, @SchemaName = @SchemaName
		, @ObjectName = @ObjectName


	SET @ExecSQL=@USE_DB_Str+CHAR(10)

	SET @DisplayedQuery+='IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '''+@IndexName+''')  BEGIN'+CHAR(10)
	IF @Index_type !='PRIMARY KEY'
		SET @DisplayedQuery +='CREATE '+@Index_type+' INDEX '+CASE WHEN @DbName != 'TempDb' THEN @IndexName ELSE '' END+' ON '+CASE WHEN @DbName != 'TempDb' THEN @SchemaName+'.' ELSE ''END+@ObjectName+'('+@columns+')'
	ELSE IF @Index_type ='PRIMARY KEY'
		SET @DisplayedQuery +='ALTER TABLE '+@SchemaName+'.'+@ObjectName+' ADD CONSTRAINT '+@IndexName+' PRIMARY KEY ('+@columns+')'

	SET @DisplayedQuery +=ISNULL('
	INCLUDE ('+@included_columns+')', '')
	+ISNULL(@Filter, '')+'
	'
	IF @DbName !='TempDb'
	SET @DisplayedQuery +=
	 'WITH (SORT_IN_TEMPDB = ON
		'+CASE WHEN @DbName != 'TempDb' THEN ', ONLINE = ON' ELSE '' END --> use these options for release without downtime
		+', FILLFACTOR = '+CAST(@FillFactor AS VARCHAR(3))
		+CASE WHEN ISNULL(@Data_Compression, '') != '' THEN ', DATA_COMPRESSION = '+@Data_Compression ELSE '' END
		+') 
	ON '+@IndexFileGroup+' '

	SET @ExecSQL+=@DisplayedQuery+'
		IF @@NESTLEVEL < 4 -- Minimum Level
				SELECT ''Index '+@IndexName+' Created'' AS SUCCESS'
				
SET @ExecSQL+=CHAR(10)+'END'
SET @DisplayedQuery+=CHAR(10)+'END'

	IF @@NESTLEVEL = 1
		SELECT @DisplayedQuery AS 'CreateIndex Query'
	

	IF @debug = 1 
		SELECT @ExecSQL AS DebugQuery_CreateIndex
	ELSE
		EXEC sp_executesql @ExecSQL


END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [Tools].[AddIndex] TO [public];
GO