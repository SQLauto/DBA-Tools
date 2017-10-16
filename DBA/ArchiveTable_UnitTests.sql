IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'ArchiveTable_UnitTests')
      DROP PROCEDURE [dbo].[ArchiveTable_UnitTests]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : UnitTest Proc to test that critical Archive table procs are working as expected.

Description: Creates a small test table
 - Archive it via copying to a another test table
 - Archive it via Zip
 - Delete the rows
 - Rollback all actions at the end

Policicy override:
GRANT EXECUTE ON [dbo].[This_Proc] TO [nobody];

History:
	2017-08-29 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[ArchiveTable_UnitTests]
AS
BEGIN TRY
	SET NOCOUNT ON;
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	IF OBJECT_id('TestArchiveTemp') IS NOT NULL OR OBJECT_id('TestArchiveTemp2') IS NOT NULL BEGIN
		DROP TABLE TestArchiveTemp; DROP TABLE TestArchiveTemp2
	END
	CREATE TABLE TestArchiveTemp(id tinyint PRIMARY KEY IDENTITY (1,1), val sysname, extra char(1) DEFAULT('d'))
	CREATE TABLE TestArchiveTemp2(id tinyint PRIMARY KEY, val sysname)
	DECLARE @Timer DATETIME2(3) = GETDATE()
	
	SELECT  CAST(TokenValue AS  DECIMAL(8,3)) AS 'ArchiveSpeedRatio'
	FROM DBA.indus.Tokens WITH(NOLOCK)
	WHERE TokenName = 'ArchiveSpeedRatio'

	BEGIN TRAN -- This TRAN will not be commited
		DECLARE @i tinyint = 1
		WHILE @i < 255 BEGIN
			INSERT INTO TestArchiveTemp (val) VALUES ('test'+CAST(@i AS sysname))
			SET @i+=1
		END
		DECLARE @NbArchivedRows TINYINT
		----------------COPY TABLE ------------------
		EXEC @NbArchivedRows=DBA.dbo.ArchiveTable_CopyIntoTable 'TestArchiveTemp', 'TestArchiveTemp2', 'id', 0, 150, 'val != ''test50''', @Excluded_columns = 'extra' , @BatchSize = 40, @debug = 0
		
		
		--Do several checks after copy:
		SELECT * FROM TestArchiveTemp2 WITH(NOLOCK)
		SELECT @NbArchivedRows AS NbCopiedRows
		IF @NbArchivedRows != 148 BEGIN; 
			THROW 51337, 'Archived rows num should be 148',10 END

		 -- should be 3 seconds if speedratio of 1	
		IF DATEDIFF (second, @Timer, GETDATE()) <=2  BEGIN;
			THROW 51337, 'Too fast copy',10  END
			
		IF	NOT (SELECT COUNT(*) FROM TestArchiveTemp2 WITH(NOLOCK)) = 148 --(test50 excluded and test149 excluded)
			OR EXISTS (SELECT 1 FROM TestArchiveTemp2 WITH(NOLOCK) WHERE val IN ('test50', 'test150'))
			OR NOT EXISTS (SELECT 1 FROM TestArchiveTemp2 WITH(NOLOCK) WHERE val IN ('test1', 'test149'))
			BEGIN;
			THROW 51337, 'Error in the Copy',10 END

		IF NOT EXISTS (SELECT 1 FROM TablesArchivingHistory WITH(NOLOCK) WHERE TableName = 'DBA.dbo.TestArchiveTemp' AND RangeColumn = 'id' AND RangeStart = 0 AND RangeEnd = 150 AND NbRowsArchived = 148 AND ActionMade ='CopyIntoTable' AND Destination = 'DBA.dbo.TestArchiveTemp2') BEGIN;
			THROW 51337, 'Missing Archiving Histo line',10 END

		----------------COPY ZIP ------------------
		EXEC @NbArchivedRows=DBA.dbo.ArchiveTable_CopyIntoZip 'TestArchiveTemp', 'id', 0, 150, 'val != ''test50''', @Excluded_columns = 'extra' ,  @debug = 0
		SELECT @NbArchivedRows AS NbCopiedRows
		
		--Do several checks after copy:
		IF NOT EXISTS (SELECT 1 FROM TablesArchivingHistory WITH(NOLOCK) WHERE TableName = 'DBA.dbo.TestArchiveTemp' AND RangeColumn = 'id' AND RangeStart = 0 AND RangeEnd = 150 AND NbRowsArchived = 148 AND ActionMade ='CopyIntoZip')  BEGIN;
			THROW 51337, 'Missing Archiving Histo line',10 END

		IF	NOT (SELECT COUNT(*) FROM TestArchiveTemp WITH(NOLOCK)) = 254  BEGIN;
			THROW 51337, 'Error in the Zip or Copy',10 END

		DECLARE @ExecOutput TABLE ([Output] VARCHAR(255)); 
		DECLARE @strCmdShell VARCHAR(500)= 'del '+CAST(DBA.Indus.GetTokenValue('Path_TableArchives') AS VARCHAR(150))+'\DBA.dbo.TestArchiveTemp\[DBA.dbo.TestArchiveTemp]_id_0-150.7z' 
		INSERT INTO @ExecOutput (Output)
			EXEC xp_cmdshell @strCmdShell

		IF EXISTS (SELECT 1 FROM @ExecOutput WHERE output IS NOT NULL) BEGIN
			SELECT @strCmdShell
			SELECT * FROM @ExecOutput;
			THROW 51337, 'Zip file del error',10
		END

		SELECT @Timer = GETDATE()

		----------------DELETE ------------------
		EXEC @NbArchivedRows=DBA.dbo.ArchiveTable_DeleteRows 'TestArchiveTemp', 'id', 0, 150, 'val != ''test50''', @PrevArchiveAction = 'CopyIntoTable' , @BatchSize = 40, @debug = 0
		
		--Do several checks after delete:
		SELECT * FROM TestArchiveTemp WITH(NOLOCK)
		SELECT @NbArchivedRows AS NbDeletedRows
		IF @NbArchivedRows != 148  BEGIN;
			THROW 51337, 'Archived rows num should be 148',10 END
		
		-- should be 3 seconds if speedratio of 1
		IF DATEDIFF (second, @Timer, GETDATE()) <=2  BEGIN;
			THROW 51337, 'Too fast delete',10 END

		IF	NOT (SELECT COUNT(*) FROM TestArchiveTemp WITH(NOLOCK)) = 106 --(test50 excluded and test149 excluded)
			OR NOT EXISTS (SELECT 1 FROM TestArchiveTemp WITH(NOLOCK) WHERE val IN ('test50', 'test150')) 
			OR EXISTS (SELECT 1 FROM TestArchiveTemp WITH(NOLOCK) WHERE val IN ('test1', 'test149'))  BEGIN;
			THROW 51337, 'Error in the Delete',10 END
			
		IF NOT EXISTS (SELECT 1 FROM TablesArchivingHistory WITH(NOLOCK) WHERE TableName = 'DBA.dbo.TestArchiveTemp' AND RangeColumn = 'id' AND RangeStart = 0 AND RangeEnd = 150 AND NbRowsArchived = 148 AND ActionMade ='DeleteRows')  BEGIN;
			THROW 51337, 'Missing Archiving Histo line',10 END

		SELECT 'IT''S ALL GOOD MAN!'  AS [SAUL GOODMAN]
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	DROP TABLE TestArchiveTemp
	DROP TABLE TestArchiveTemp2
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO