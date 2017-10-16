IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'CloneUser')
      DROP PROCEDURE [dbo].[CloneUser]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : The procedure will Return a SELECT of all the @ClonedUser current permissions , that are missing to @NewUserName
, WITH correct scripting format to be executed right away
, This will also return the associated login creation if still present

Can also be executed multi-INST and multi-DB

In a v2 this proc could also apply the returned scripts. (further testings required)

Carefull! This does copy some login parameters but will ignore unusual parameters 

Usage exemple for 1 Db:
EXEC DBA..[CloneUser] 'Betclick' , 'bi_load', 'new_user', @debug = 0

Usage exemple for several Dbs:
EXEC DBA..[CloneUser] '%' , 'bi_load', 'new_user', @debug = 0

Policicy override:
GRANT EXECUTE ON [dbo].[CloneUser] TO [nobody];

History:
	2017-03-30 - XMO - Add Check_policy as supported login param
	2017-03-28 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[CloneUser]
(
	@DbName SYSNAME --use '%' for 'ALL'
	,@ClonedUser SYSNAME --Username of the cloned user (supposed it's also the clonedLogin name)
	,@NewUserName SYSNAME
	,@NewUserPwd NVARCHAR(100) = '' --Needed if you want to create a login with this
	,@NewUserLogin SYSNAME = NULL -- Default is same AS @NewUserName
	,@NewUserLoginSID VARBINARY(85) = NULL -- Default is a new random one
	,@Debug BIT = 0
	
)
AS
BEGIN TRY

IF @NewUserLogin IS NULL 
	SET @NewUserLogin =  @NewUserName
--Multi Db
IF (@DbName = '%')
BEGIN
	DECLARE @EachDbCommand nvarchar(2000) 
	
	IF @NewUserLoginSID IS NULL --Generate new random global SID for the user
		SELECT @NewUserLoginSID = cast(newId() as varbinary(85)) 

	IF OBJECT_ID('tempdb..#AllDBsPermissions') IS NOT NULL DROP TABLE #AllDBsPermissions;
	CREATE TABLE #AllDBsPermissions (stmt NVARCHAR(4000))

	--Create login if not exists
	INSERT INTO #AllDBsPermissions
	SELECT
		CASE  --Create login command with db_name and default _lang from cloned one. New SID
			WHEN new.principal_id IS NULL THEN
				'CREATE LOGIN '+QUOTENAME(@NewUserLogin)+' WITH PASSWORD =N'''+@NewUserPwd
				+''', SID = '+CONVERT(VARCHAR(50), @NewUserLoginSID,1) 
				+', DEFAULT_DATABASE='+QUOTENAME(cloned.default_database_name)
				+', DEFAULT_LANGUAGE='+QUOTENAME(cloned.default_language_name)
				+', CHECK_POLICY='+CASE WHEN cloned.is_policy_checked = 1 THEN 'ON' ELSE 'OFF' END
				+CASE WHEN cloned.is_expiration_checked =1 THEN ', CHECK_EXPIRATION=ON ' ELSE '' END
				+';'+CHAR(10)+'GO'+CHAR(10)
			ELSE ''
		END
		+CASE 	--Disable login if cloned disabled
			WHEN cloned.is_disabled = 1 AND ISNULL(new.is_disabled, 0) != 1 THEN
			 'ALTER LOGIN '+QUOTENAME(@NewUserLogin)+' DISABLE '+CHAR(10)+'GO'+CHAR(10)
			 ELSE ''
		END 
	FROM sys.sql_logins AS cloned
	LEFT JOIN sys.sql_logins AS new ON new.name = @NewUserName AND	new.type IN ('S', 'U')
	WHERE	cloned.name = @ClonedUser
	AND		cloned.type IN ('S', 'U') --SQL_LOGIN /Windows_Login
	

	SELECT @EachDbCommand = '
		EXEC DBA..CloneUser ''?'' , '''+@ClonedUser+''', '''+@NewUserName+''', '''+@NewUserLogin+'''
	' 
	INSERT INTO #AllDBsPermissions
	EXEC sp_MSforeachdb @EachDbCommand

	SET @EachDbCommand = '' --reset and reuse this variable
	SELECT @EachDbCommand += stmt FROM #AllDBsPermissions
	SELECT @@SERVERNAME AS INST , @EachDbCommand AS SQL_to_Exec


END
ELSE
--Single Db
	BEGIN
	IF (SELECT DATABASEPROPERTYEX(@DbName, 'Updateability') )= 'READ_ONLY'
		RETURN
 
	DECLARE @SqlExec NVARCHAR(4000)

	SET @SqlExec = 'USE '+@DbName
	SET @SqlExec += '
	DECLARE @ClonedUser sysname = '''+@ClonedUser+'''
		, @NewUserName sysname = '''+@NewUserName+'''
		, @NewUserLogin sysname = '''+@NewUserLogin+'''

	--Check if old user exists on this DB
	IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @ClonedUser)
	BEGIN	
	
		DECLARE @CloneUserStmt VARCHAR(2000) = ''''
	
		--Add CreateUser statement if user does not exist
		IF NOT EXISTS (SELECT 1 FROM sys.database_principals AS usr WHERE usr.Name = @NewUserName)
			SELECT @CloneUserStmt= ''CREATE USER ''+QUOTENAME(@NewUserName)+'' FOR LOGIN ''+QUOTENAME(@NewUserLogin)+CHAR(10) 
	

		;WITH Perms AS (
		SELECT  ''EXEC sp_addrolemember @rolename =''
			+ SPACE(1) + QUOTENAME(USER_NAME(rm.role_principal_id), '''''''') + '', @membername ='' + SPACE(1) + QUOTENAME(@NewUserName, '''''''')  AS ''Permissions''
			,USER_NAME(rm.member_principal_id) AS UserName
		FROM    sys.database_role_members AS rm
		WHERE   USER_NAME(rm.member_principal_id) IN (@ClonedUser,@NewUserName)
		UNION ALL
		SELECT  CASE WHEN perm.state = ''W'' THEN ''GRANT'' ELSE perm.state_desc END
			+'' ''+ perm.permission_name +'' ''+ 
			CASE WHEN perm.major_id != 0 AND obj.object_id IS NOT NULL
				THEN '' ON '' + QUOTENAME(USER_NAME(obj.schema_id)) + ''.'' + QUOTENAME(obj.name)
				+ CASE WHEN cl.column_id IS NULL THEN SPACE(0) ELSE ''('' + QUOTENAME(cl.name) + '')'' END
		
			ELSE '''' END +
			'' TO '' + QUOTENAME(@NewUserName) COLLATE database_default
			+ CASE WHEN perm.state <> ''W'' THEN '''' ELSE '' WITH GRANT OPTION'' END AS ''Permissions''
			,usr.name AS UserName
		FROM    sys.database_permissions AS perm
			INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
			LEFT JOIN sys.objects AS obj ON perm.major_id = obj.[object_id]
			LEFT JOIN sys.columns AS cl ON cl.column_id = perm.minor_id AND cl.[object_id] = perm.major_id
		WHERE   usr.name IN (@ClonedUser,@NewUserName)
		) 
		SELECT @CloneUserStmt += Perms.Permissions+CHAR(10)
		FROM Perms
		WHERE Perms.UserName = @ClonedUser
		AND NOT EXISTS (SELECT 1 FROM Perms AS ExistingPerms WHERE ExistingPerms.UserName = @NewUserName AND ExistingPerms.Permissions = Perms.Permissions)

		IF @CloneUserStmt != ''''
		BEGIN		
			SELECT ''--''+@@SERVERNAME+ '' ( ''+DB_NAME()+'' )''+CHAR(10)+''USE ''+DB_NAME()+CHAR(10)+@CloneUserStmt+''GO''+CHAR(10)
		END
	
	END
	' 
	IF @Debug = 1
		SELECT @SqlExec
	ELSE 
		EXEC sp_executesql @SqlExec
END --end of single DB condition
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO
