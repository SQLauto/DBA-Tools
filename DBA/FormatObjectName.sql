IF EXISTS(SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID('dbo') AND name = 'FormatObjectName' AND type IN ('FN', 'TF', 'IF'))
      DROP FUNCTION [dbo].[FormatObjectName]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : 
Table valued function to return the Full Object Name from a given string of type 'DbName.SchemaName.ObjectName', 
Manages optional Dbname and SchemaName :
	- sets DbName as CurrentDB (deduced from SharedTransaction)
	- sets tempDb Automatically if # is used before the objectName
	- sets SchemaName to dbo
	- removes eventual [] or tab / returns, spaces...

History :
	2016-08-04 - XMO - Add VIEW SERVER STATE permission check
	2016-07-08 - XMO - Creation

*/
CREATE FUNCTION [dbo].[FormatObjectName]
(
	 @ObjectName SYSNAME
)
RETURNS @tbl_results TABLE ([db_name] SYSNAME, [schema_name] SYSNAME, [object_name] SYSNAME, [object_fullname] SYSNAME)
AS
BEGIN;

	SET @ObjectName = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@ObjectName, '[', ''), ']', ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), ' ', '');

	--Splitting the Table value into DB+schema+name
	DECLARE @ReverseString VARCHAR(500) = REVERSE(@ObjectName) + '...';
	DECLARE @ShortObjectName SYSNAME = REVERSE(LEFT(@ReverseString, CHARINDEX('.', @ReverseString) - 1));
	SET @ReverseString = SUBSTRING(@ReverseString, CHARINDEX('.',@ReverseString) + 1, LEN(@ReverseString));
	DECLARE @SchemaName SYSNAME = REVERSE(LEFT(@ReverseString, CHARINDEX('.',@ReverseString) - 1));
	SET @ReverseString = SUBSTRING(@ReverseString, CHARINDEX('.',@ReverseString) + 1, LEN(@ReverseString));
	DECLARE @DbName	SYSNAME	= REVERSE(LEFT(@ReverseString, CHARINDEX('.',@ReverseString) - 1));

	IF @SchemaName = ''
		SET @SchemaName = 'dbo';
	IF @DbName = ''
	BEGIN;
		IF @ShortObjectName LIKE '#%'
			SET @DbName = 'tempdb';
		ELSE BEGIN
			DECLARE @Db_history TABLE ([order] TINYINT IDENTITY (1,1), [db_id] TINYINT,  [db_name] SYSNAME)

			IF (SELECT HAS_PERMS_BY_NAME(null, null, 'VIEW SERVER STATE')) = 1 -- Check permission
				INSERT INTO @Db_history (db_id, db_name)
				SELECT    
					t1.resource_database_id
					,db_name(t1.resource_database_id )
				FROM sys.dm_tran_locks as t1  
				WHERE request_session_id = @@SPID and resource_type = 'DATABASE' and request_owner_type = 'SHARED_TRANSACTION_WORKSPACE'
			ELSE BEGIN -- if no permissions, can't deduce the database
				INSERT @tbl_results VALUES('', @SchemaName, @ShortObjectName, @SchemaName+'.'+@ShortObjectName);
				RETURN ;
			END

			DECLARE @nb_results TINYINT = (SELECT count(*) FROM @Db_history)
	
			IF @nb_results = 0
				SET @DbName =  db_name(1) -- master
			ELSE IF @nb_results = 1
				SET @DbName =  (SELECT TOP 1 db_name FROM @Db_history)
			ELSE IF @nb_results = 2
				SET @DbName =  (SELECT TOP 1 db_name FROM @Db_history WHERE db_id != DB_ID())
			ELSE IF @nb_results > 2
				SET @DbName =  ''
		END
	END;
	
	INSERT @tbl_results VALUES(@DbName, @SchemaName, @ShortObjectName, @DbName + '.' + @SchemaName + '.' + @ShortObjectName);
	RETURN;
END
GO

GRANT SELECT ON [dbo].[FormatObjectName] TO [public];
GO
