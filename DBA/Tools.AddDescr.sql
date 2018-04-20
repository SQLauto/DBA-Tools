IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('Tools') AND name = 'AddDescr')
      DROP PROCEDURE [Tools].[AddDescr]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Adds a description to a table or column (extended property)

Description:

Just like a sys.sp_ADDextendedproperty @name="MS_Description" but :
	-Simpler syntax
	-Natively Idempotent
	-Drops old description automatically if exists

Parameters :
	- @Table : TableName with or without schema name. Add DbName.Schema.TableName if needed (default is current DB and dbo schema)
	- @columns : Optional name of the column to describe
	- @Debug=1 will display the script executed

History:
	2016-11-28 - XMO - Add description for VIEWs
	2016-07-28 - XMO - Rework with FormatObjectName and new template
	2016-05-11 - XMO - Creation
*/
CREATE PROCEDURE [Tools].[AddDescr]
(
	@Table sysname -- Name of the table, can also of the form DbName.SchemaName.TableName
	,@column sysname = NULL --> If Null is used, the table descr will be updated
	,@descr VARCHAR(256) -- The description string to apply
	,@Debug BIT = 0 --> To display the result without executing
)
AS
BEGIN TRY
	DECLARE @OriginalObjectName SYSNAME = @Table
	, @SchemaName SYSNAME
	, @DbName SYSNAME
	, @ObjectFullName SYSNAME
	, @ObjectName SYSNAME --> will be Table name

	SELECT @DbName = [db_name]
	,@SchemaName = [schema_name]
	,@ObjectName = [object_name]
	,@ObjectFullName = [object_fullname] 
	FROM DBA.dbo.FormatObjectName (@OriginalObjectName)
	
	IF @DbName = 'TempDb'
		BEGIN SELECT 'Can''t add description on temp objects.' AS ERROR RETURN END

	IF @ObjectFullName IS NULL 
		BEGIN SELECT 'ERROR trying to format the @ObjectName : '+@ObjectName+' AS DbName.SchemaName.ObjectName.' AS ERROR RETURN END

	
	SET @column = REPLACE(REPLACE(@column, '[', ''), ']', '') --Need brackets removal to avoid errors
	SET @column = LTRIM(RTRIM(@column))
	SET @column = NULLIF(@column, '')

	SET @descr = LTRIM(RTRIM(@descr))
	--Auto MAJ first char
	SET @descr = UPPER(LEFT(@descr,1))+SUBSTRING(@descr,2,LEN(@descr))

	DECLARE @ExecSQL NVARCHAR(4000) = ''

	DECLARE @SQL_ADD_Descr NVARCHAR(500) =
	'EXEC sys.sp_ADDextendedproperty @name="MS_Description"
		, @level0type="SCHEMA", @level0name=@SchemaName
		, @level1type= @object_type, @level1name=@ObjectName
		'+CASE WHEN @column IS NOT NULL THEN ', @level2type="COLUMN" ,@level2name=@column ' ELSE ' ' END
		
	SET @ExecSQL+=
	'USE '+@DbName+';
	DECLARE @object_type SYSNAME 
	SELECT TOP 1 @object_type=type_desc FROM sys.objects WHERE name = @ObjectName ORDER BY CASE WHEN type_desc = ''USER_TABLE'' THEN 1 ELSE 2 END;
	IF (@object_type IS NULL OR @object_type = ''USER_TABLE'')
		SET @object_type= ''TABLE''

	ADD_Desr:
	BEGIN TRY
	SET ROWCOUNT 1;
	'
	SET @ExecSQL+=@SQL_ADD_Descr+'
		, @value=@descr;
	
	SELECT "Description added to "+@SchemaName+"."+@ObjectName+ISNULL("["+@column+"]", "")+" : "+@descr  AS [SUCCESS														]

	END TRY
	BEGIN CATCH
		IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;

		IF ERROR_NUMBER() = 15233 BEGIN --> If Descr Already Exists
			--DROP it
			'+
			REPLACE(@SQL_ADD_Descr, 'sp_ADD', 'sp_DROP')+'
			--Recreate it
			GOTO ADD_Desr;
		END
		ELSE
			THROW;
	END CATCH
	'
		
	SET @ExecSQL = REPLACE(@ExecSQL, '"', '''')
	IF @debug = 1 
		SELECT @ExecSQL AS 'Debug Query'
	ELSE
		EXEC sp_executesql @ExecSQL
			,N'@descr  VARCHAR(256)
			,@SchemaName SYSNAME
			,@ObjectName SYSNAME
			,@column SYSNAME
			'
			,@descr = @descr
			,@SchemaName = @SchemaName
			,@ObjectName = @ObjectName
			,@column = @column

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [Tools].[AddDescr] TO [public];
GO