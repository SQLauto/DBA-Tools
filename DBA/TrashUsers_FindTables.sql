IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'TrashUsers_FindTables')
      DROP PROCEDURE [dbo].[TrashUsers_FindTables]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
Title : Searches for tables that could contain dependancies on Users tables to cleanup

Warning ! Not tested fully. Do not use on prod without checking data, especially the join queries

GRANT EXECUTE ON TO [nobody]
EXEC [TrashUsers_FindTables] 'Betclick', @InsertResultsInto = null
History:
	2018-01-23 - XMO - Add payment hardcoded TableName ad fix
	2018-01-15 - XMO - Fix. Not tested fully on complicated joins
	2018-01-12 - XMO - Small fix
	2017-12-06 - XMO - Added Join query generation
	2017-11-24 - XMO - Rename AS TrashUsers_FindTables
	2017-10-19 - XMO - Creation AS ScanTables_UsersFR
*/
CREATE PROCEDURE [dbo].[TrashUsers_FindTables]
(
	@DbName SYSNAME -- SET '%' for all Dbs on the instance, Needs InsertResultsInto to be set
	,@InsertResultsInto sysname = 'DBA.dbo.TrashUsersImpactedTables' -- Avoid changing this table as other procs use this exact name
	,@Scope VARCHAR(25) = 'ARJEL'
	,@TrashUsersListTable sysname  = 'DBA.dbo.TrashUsersListARJEL'
	,@ForceDb BIT= 0 -- Do not use unless for testing. Forces unreadable DBs to be scanned
	,@Debug BIT = 0
)
AS
BEGIN TRY
	IF (@DbName  NOT IN('tempdb', 'master', 'msdb', 'model', '%') )BEGIN
		IF (SELECT DATABASEPROPERTYEX(@DbName, 'Updateability') )= 'READ_ONLY' AND @ForceDb = 0
			RETURN


		IF OBJECT_ID('tempdb..#ParentTables') IS NOT NULL DROP TABLE #ParentTables;
			CREATE TABLE #ParentTables(
				TableFullName SYSNAME
				, ColumnName sysname
				, IsNullable BIT NULL
				, PKColumnNames VARCHAR(500) NULL
				, TableObjectId AS OBJECT_ID(TableFullName)
				)

		IF @DbName = 'Betclick'
			INSERT INTO #ParentTables SELECT 'betclick.dbo.users', 'Id', 0, 'Id'

		IF @DbName = 'EverestMigration' BEGIN
			INSERT INTO #ParentTables SELECT 'EverestMigration.betclick.users', 'Id', 0, 'Id'
			INSERT INTO #ParentTables SELECT 'EverestMigration.betclick.filleul_inscrit', 'filleul_id', 0, 'filleul_id'
		END
		IF @DbName = 'Payments' BEGIN
			INSERT INTO #ParentTables SELECT 'Payments.dbo.payers', 'ExternalId', 0, 'Id'
		END

		IF OBJECT_ID('tempdb..#ExcludedTables') IS NOT NULL DROP TABLE #ExcludedTables;
		CREATE TABLE #ExcludedTables(TableFullName sysname PRIMARY KEY)
		
		INSERT INTO #ExcludedTables
		VALUES (@TrashUsersListTable)

		If @Scope = 'ARJEL'
		BEGIN
			INSERT INTO #ExcludedTables
			VALUES 
			 ('%UsersPl%')
			,('%UsersUk%')
			,('%UsersPt%')
			,('%UsersBe%')
			,('%UsersIt%')
			,('%aams%')
			,('%casino%')
			,('%italy%')
			,('%ITCaptor%')
			,('%netent%')
			,('%mcc%')
			,('%expekt%')
			,('%Betclick_it%')
			,('%CountryClosure_%')
			,('P_Captor%')
			,('%BAH.Users%')
			,('EverestBonusUsersBcp') --not an int
			,('EverestGamesUsersBcp') --not an int
		END
		
		IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
		CREATE TABLE #Results
		(	FKLevel				tinyint	NOT NULL
			,TableFullName			sysname	NOT NULL
			,TableObjectId			sysname	NOT NULL
			,ColumnName				sysname	NOT NULL
			,IsNullable				BIT NULL
			,PKColumnNames			varchar(500) NULL
			,FKName					sysname	NULL
			,ParentTableFullName	nvarchar(533)  NULL
			,ParentColumnName		sysname	NULL
			,JoinQuery				nvarchar(4000) NULL
		)

		DECLARE @SqlExec NVARCHAR(MAX)
		SET @SqlExec = N'USE '+@DbName
		
		SET @SqlExec += N'
		DECLARE @FKLevel tinyint = 0
		---------------- GOTO POINT ----------------
		Add_FK_FromParents:
		
		INSERT INTO #Results(
			FKLevel			
			,TableFullName		
			,TableObjectId		
			,ColumnName		
			,IsNullable
			,PKColumnNames	
		)
		SELECT @FKLevel AS FKLevel
			,p.TableFullName
			,p.TableObjectId
			,p.ColumnName
			,p.IsNullable
			,p.PKColumnNames
		FROM #ParentTables AS p
		WHERE p.TableObjectId is not null
		

		SET @FKLevel+=1

		--Checking FKs tables for all the parents.
		WHILE EXISTS (SELECT 1 FROM #ParentTables)
		BEGIN
			-- Display All Columns details
			INSERT INTO #Results(
				FKLevel			
				,TableFullName		
				,TableObjectId		
				,ColumnName		
				,IsNullable
				,PKColumnNames
				,FKName		
				,ParentTableFullName
				,ParentColumnName
			)
			SELECT	
				@FKLevel AS FKLevel
				,FK_ReferredBy.*
			FROM #ParentTables AS p
			CROSS APPLY (
				SELECT 
						FK_table.FullName AS TableFullName
						,ref_table.object_id AS TableObjectId
						,ref_column.name AS ColumnName
						,ref_column.Is_Nullable AS IsNullable
						,PKColumn.Names AS PKColumnNames
						,FK.Name
						,p.TableFullName AS ParentTableFullName
						,refed_column.name AS ParentColumnName
				FROM sys.foreign_key_columns as FKC
				INNER JOIN sys.foreign_keys AS FK ON FKC.constraint_object_id = FK.object_id 
				INNER JOIN sys.objects AS ref_table ON ref_table.object_id = FKC.parent_object_id
				INNER JOIN sys.schemas AS ref_schema ON ref_table.schema_id = ref_schema.schema_id
				INNER JOIN sys.columns AS ref_column ON ref_column.object_id = ref_table.object_id AND ref_column.column_id = FKC.parent_column_id
				INNER JOIN sys.columns AS refed_column ON refed_column.object_id = FKC.referenced_object_id AND refed_column.column_id = FKC.referenced_column_id
				CROSS APPLY (SELECT  DB_NAME()+''.''+ref_schema.name+''.''+ref_table.name AS FullName ) AS FK_table
				OUTER APPLY (SELECT 1 AS Yes FROM #ExcludedTables WHERE FK_table.FullName LIKE #ExcludedTables.TableFullName) AS excluded
				OUTER APPLY (
				SELECT (
				SELECT CASE WHEN ic.key_ordinal >1 THEN '','' ELSE '''' END+c.name
					AS [text()]
					FROM sys.indexes i 
					INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
					INNER JOIN sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
					WHERE i.object_id = ref_table.object_id 
					AND i.is_primary_key = 1
					ORDER BY ic.key_ordinal
				FOR XML PATH ('''')
				) AS Names
				) AS PKColumn
				WHERE	FKC.referenced_object_id = p.TableObjectId
				--AND		FKC.referenced_column_id = 1
				AND		FK_table.FullName NOT IN (SELECT TableFullName FROM #Results )
				AND		excluded.yes is null
			) AS FK_ReferredBy --Get the list of FKs if this column is referred by others
			ORDER BY 1,2

			DELETE #ParentTables

			INSERT INTO #ParentTables (TableFullName, ColumnName)
			SELECT DISTINCT TableFullName, ''''
			FROM #Results 
			WHERE FKLevel = @FKLevel

			SET @FKLevel+=1
		END

		--Adding Tables without FK but with ''userid'' in column name

		INSERT INTO #ParentTables (TableFullName, ColumnName, PKColumnNames, IsNullable)
		SELECT TOP 999 FK_table.FullName AS Tables_with_users_in_name 
			,ref_column.name AS column_name
			,PKColumn.Names AS PKColumnNames
			,ref_column.is_nullable AS IsNullable
		FROM sys.tables AS ref_table
		JOIN sys.schemas AS ref_schema ON ref_table.schema_id = ref_schema.schema_id
		INNER JOIN sys.columns AS ref_column ON ref_column.object_id = ref_table.object_id 
		CROSS APPLY (SELECT  DB_NAME()+''.''+ref_schema.name+''.''+ref_table.name AS FullName ) AS FK_table
		OUTER APPLY (SELECT 1 AS yes FROM #ExcludedTables WHERE FK_table.FullName LIKE #ExcludedTables.TableFullName) AS excluded
		OUTER APPLY (
				SELECT (
				SELECT CASE WHEN ic.key_ordinal >1 THEN '','' ELSE '''' END+c.name
					AS [text()]
					FROM sys.indexes i 
					INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
					INNER JOIN sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
					WHERE i.object_id = ref_table.object_id 
					AND i.is_primary_key = 1
					ORDER BY ic.key_ordinal
				FOR XML PATH ('''')
				) AS Names
				) AS PKColumn
		WHERE	FK_table.FullName NOT IN (SELECT TableFullName FROM #Results )
		AND		excluded.yes is null
		AND		type_DESC like ''USER_TABLE''
		AND		ref_column.name like ''%user%id%''
		AND		ref_column.name != ''ExternalId''

		--Check if these tables have FKs
		IF (EXISTS (SELECT 1 FROM #ParentTables) AND @FKLevel <10)
		BEGIN
			SET @FKLevel = 10
			GOTO Add_FK_FromParents
		END
		'
		------------------------------------ EXECUTING Find ALL TABLES  -----------------------------------
		If @Debug = 1
			SELECT @SqlExec AS SqlExec_FindAllTables_Debug
		EXEC sp_executesql @SqlExec
		
		------------------------------------ Loop through #results To generate JOIN Queries  -----------------------------------

		DECLARE @TableFullName SYSNAME --Name of the table we're currently interested in
		DECLARE @ColumnName SYSNAME --Name of the corresponding column
		DECLARE @FKName SYSNAME
		DECLARE @FKLevel TINYINT 
		DECLARE @ParentTableFullName SYSNAME
		DECLARE @ParentColumnName SYSNAME
		DECLARE @ParentFKName SYSNAME
		DECLARE @ChildTableFullName SYSNAME
		DECLARE @ChildColumnName SYSNAME

		DECLARE @JoinQuery NVARCHAR(4000) 
		WHILE EXISTS (SELECT 1 FROM #Results WHERE JoinQuery IS NULL)
		BEGIN
		
			SELECT TOP 1
				@TableFullName = TableFullName
				,@ColumnName = ColumnName
				,@FKName = ISNULL(FKName, TableFullName+ColumnName)
				,@ParentTableFullName = ISNULL(ParentTableFullName, @TrashUsersListTable)
				,@FKLevel = FKLevel
			FROM #Results
			WHERE JoinQuery IS NULL
			ORDER BY FKLevel DESC, TableFullName DESC, FKName DESC -- find a way to do an ORDER BY column number in the PK 

			DECLARE @NbFKColumns TINYINT =0
			SELECT @ChildTableFullName  = @TableFullName
			SELECT @ChildColumnName = @ColumnName
	
			SELECT @JoinQuery = 
			/*SELECT '+@ColumnName+*/' FROM '+@TableFullName+ ' AS [Lvl'+CAST(@FKLevel AS VARCHAR(2))+'] WITH(NOLOCK)'

			WHILE (@FKLevel%10>=0) BEGIN
					--could be several columns
					SELECT @NbFKColumns = COUNT(*)
						,@ParentTableFullName = ParentTableFullName
					FROM #Results
					WHERE TableFullName = @ChildTableFullName
					AND (TableFullName = @ChildTableFullName OR @FKName = ISNULL(FKName, TableFullName+ColumnName))
					AND FKLevel = @FKLevel
					GROUP BY ParentTableFullName

					IF @ParentTableFullName IS NULL SET @ParentTableFullName = @TrashUsersListTable

					DECLARE @ChildAliasName SYSNAME='[Lvl'+CAST(@FKLevel AS VARCHAR(2))+']'
					DECLARE @ParentAliasName SYSNAME='[Lvl'+CAST(@FKLevel-1 AS VARCHAR(2))+']'

					SELECT @JoinQuery +=CHAR(10)+' INNER JOIN '+@ParentTableFullName+' AS '+@ParentAliasName+' WITH(NOLOCK) ON '
					
					--Debug :
					--SELECT @NbFKColumns NbFKColumns, @ChildTableFullName Child_TableName, @FKLevel FKLevel, @TableFullName TableFullName,@ParentTableFullName ParentTableFullName,@FKName FKName --Debug
					
					IF @ParentTableFullName = @TrashUsersListTable
					BEGIN
						 SELECT @JoinQuery +=@ChildAliasName+'.'+@ChildColumnName + ' = '+@ParentAliasName+'.UserId '
					END
					ELSE WHILE (@NbFKColumns >0) BEGIN
						 SELECT @ParentColumnName = ISNULL(ParentColumnName, 'UserId')
							, @ChildColumnName = ColumnName
						 FROM #Results
						 WHERE TableFullName = @ChildTableFullName
						 AND (TableFullName = @ChildTableFullName OR @FKName = ISNULL(FKName, TableFullName+ColumnName))
						 AND FKLevel = @FKLevel
						 ORDER BY ColumnName OFFSET (@NbFKColumns-1) ROW FETCH NEXT 1 ROW ONLY

						 SELECT @JoinQuery +=@ChildAliasName+'.'+@ChildColumnName + ' = '+@ParentAliasName+'.'+@ParentColumnName+' '
						  IF (@NbFKColumns>1) BEGIN 
							SELECT @JoinQuery += ' AND ' 
						 END
						 SET @NbFKColumns -= 1
					END

				SELECT @ChildTableFullName  = @ParentTableFullName
						,@ChildColumnName = @ParentColumnName
						,@ParentTableFullName = NULL
						,@NbFKColumns = 1

				IF @FKLevel%10 <=0
					BREAK
				SET @FKLevel-=1
			END
			
			--This can update several rows if several columns  
			UPDATE r
			SET r.JoinQuery = @JoinQuery
			FROM #Results AS r
			WHERE @TableFullName = TableFullName
			AND @FKName = ISNULL(FKName, TableFullName+ColumnName)
		END


		------------------------------------ SAVE OR DISPLAY RESULTS -----------------------------------------
		SELECT @Scope AS Scope, * From #Results ORDER BY FKLevel, TableFullName
		SET @SqlExec =' INSERT INTO '+@InsertResultsInto+'(
			Scope					
			,FKLevel				
			,TableFullName			
			,TableObjectId		
			,ColumnName					
			,IsNullable			
			,PKColumnNames			
			,FKName			
			,ParentTableFullName	
			,ParentColumnName	
			,JoinQuery	
		)
		SELECT '''+@Scope+''' AS Scope, * From #Results ORDER BY FKLevel DESC, TableFullName
		'

		IF @Debug = 1 OR (ISNULL(@InsertResultsInto, '') = '' ) BEGIN
			--Display Results
			SELECT @Scope AS Scope, * From #Results ORDER BY FKLevel, TableFullName
			SELECT 'Not inserted into '+@InsertResultsInto
			SELECT @SqlExec AS Debug_Query
			
		END
		ELSE BEGIN
			EXEC sp_executesql @SqlExec -- Exec insert results
			SELECT @@ROWCOUNT AS NbRowsInserted
		END
	END
	--When Several db choosen, exec recursively for each one
	ELSE IF (@DbName = '%')
	BEGIN
		DECLARE @EachDbCommand nvarchar(2000) ='
			EXEC DBA.dbo.TrashUsers_FindTables ''?'' , @InsertResultsInto= '''+@InsertResultsInto+''',  @Scope= '''+@Scope+''', @ForceDb='+CAST(@ForceDb AS sysname)+', @Debug ='+CAST(@Debug AS char(1))+'
			'
		IF @InsertResultsInto IS NULL BEGIN;
			THROW 50000, 'User Error. You need @InsertResultsInto param to be set in order to use ALL Databases. % DbName', 2;
		END

		EXEC sp_MSforeachdb @EachDbCommand

	END
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

