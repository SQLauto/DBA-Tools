IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'XE_ImportSessionLogs')
      DROP PROCEDURE [dbo].[XE_ImportSessionLogs]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Xevents Session Logs Import into table

Description:
Import Existing Log files from a specific XEvent session, into a prexising table
The table columns need to be named after XEvents Data Names (if you want them mapped).
The table needs to be on DBA (small improvement could take it in parameter)

Other currently supported column names :
- id auto incremented
- EventName to get the Event name
- DBVersion & Changeset to get the Indus Release name

If NO new logs are found, the proc does nothing

Multi Tables usage : 
	- The function can be called without @SessionName and @DestinationTable parameters in order to recursively call itself for each of sessions tables found in XESessionsData

Parameters :
	- @SessionName : XEvent Session Name (The session must be pre-existing (and running if you want the target file to be automatically found))
	- @DestinationTable : The table into which to export the data FROM the logs files
	- @MaxLogFilesToImport : This will determine a maximum number of files to import during one proc execution (to do tests or limit the exec time)
	- @ForceFilePath : MANDATORY IF your session is not on the same Instance/doesn't exist anymore. Can be left blank if not.
		It takes the place of the Log target
		Example : '\\MAL-SQLBACKUP\SLOWSAN\Reports\INST01\XEvents_logs\Betclick_RPCs.xel'
	- @ERROR_output Is used to acumulate ERROR_MESSAGES() before doing the final throw errors summary at the end
	- @Debug : if set to 1, will deactivate all writing actions like insert and moving files

Debug :
exec dbo.XE_ImportSessionLogs 'Session_Betclick_RPCs', 'Table_XELogs_Rpc', 1, @debug = 1
exec XE_ImportSessionLogs 'ALL', @debug = 1

History:
	2017-07-03 - XMO - Use FileOffset using XELogFilesImport table
	2017-03-08 - XMO - Fix event fields xml parsing
	2016-12-06 - XMO - Added EmptyFileSize auto param
	2016-11-21 - XMO - Moved out log files deletion logic out into a dedicated PS Step
	2016-10-26 - XMO - Removed %10 to fix null timestamps
	2016-09-28 - XMO - Change errors management
	2016-09-23 - XMO - Add XML column type support
	2016-03-23 - XMO - Update Token from Path_Report to Path_XEvents
	2016-03-11 - XMO - Add multi sessions import (with XESessionsData). Recursive
	2016-02-29 - XMO - Add statement substring support
	2016-02-24 - XMO - Changed the way to Ignore files being written. Automatically deduced.
	2016-01-04 - XMO - Add Smarter Path Detection and Print infos
	2015-12-21 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[XE_ImportSessionLogs]
(
	@SessionName VARCHAR(128) = NULL
	,@DestinationTable sysname  = NULL
	,@MaxLogFilesToImport INT = 25
	,@ForceFilePath VARCHAR(512) = NULL
	,@EmptyFileSize VARCHAR(7) = NULL
	,@ERROR_output NVARCHAR(4000) = '' OUTPUT
	,@debug BIT = 0
)
AS
BEGIN TRY
	-- To import All the sessions found in the Table SessionNamesData, use the @SessionName : ALL
	-- This will recursively call XE_ImportSessionLogs proc for each session found
	IF (@SessionName IS NULL AND @DestinationTable IS NULL )BEGIN
		DECLARE @ImportSessions TABLE (SessionName VARCHAR (128), ImportTable VARCHAR (128), CustomPath VARCHAR (512), EmptyFileSize VARCHAR(7))
		DECLARE @ImportTable VARCHAR (128), @CustomPath VARCHAR (512)

		INSERT INTO @ImportSessions(SessionName, ImportTable, CustomPath, EmptyFileSize)
		SELECT ImportTable.SessionName
			, ImportTable.Value
			, CustomPath.Value
			, EmptyFileSize.Value
		FROM XESessionsData AS ImportTable WITH(NOLOCK)
		LEFT JOIN XESessionsData AS CustomPath WITH(NOLOCK) ON ImportTable.SessionName = CustomPath.SessionName AND CustomPath.DataType = 'CustomPath' 
		LEFT JOIN XESessionsData AS EmptyFileSize WITH(NOLOCK) ON ImportTable.SessionName = EmptyFileSize.SessionName AND EmptyFileSize.DataType = 'EmptyFileSize' 
		WHERE	ImportTable.DataType = 'ImportTable'
		AND		ImportTable.Value IS NOT NULL

		IF @Debug = 1 
			SELECT * FROM @ImportSessions

		WHILE EXISTS(SELECT 1 FROM @ImportSessions) BEGIN
			SELECT TOP (1) @SessionName = SessionName, @ImportTable = ImportTable, @CustomPath = CustomPath, @EmptyFileSize = EmptyFileSize
			FROM @ImportSessions 
			DECLARE @LastErrorOutput NVARCHAR(4000) = ''
			-- Recursive call for each session found
			EXEC XE_ImportSessionLogs @SessionName = @SessionName
			, @DestinationTable = @ImportTable
			, @MaxLogFilesToImport = @MaxLogFilesToImport
			, @ForceFilePath = @CustomPath
			, @EmptyFileSize = @EmptyFileSize
			, @ERROR_output = @LastErrorOutput OUTPUT
			, @debug = @debug

			--IF Error found, add it to the Errors Messages Varchar (a throw of the errors will be done at the end)
			IF ISNULL(@LastErrorOutput,'') != ''
				SET @ERROR_output +=ISNULL(@LastErrorOutput,'')+CHAR(10)

			DELETE @ImportSessions WHERE @SessionName = SessionName
		END

	END
	-- For a single session Import
	ELSE IF (@SessionName IS NOT NULL AND @DestinationTable IS NOT NULL )BEGIN
		--Get the Names of the columns of the DestinationTable in parameter. Also the type of these column.
		--The Column names need to match the event data fields names exactly to be picked up
		--Any Other column name obviously needs to be hardcoded with a specific action below in the procedure
		IF OBJECT_ID('tempdb..#DestinationTableColumns') IS NOT NULL DROP TABLE #DestinationTableColumns
		SELECT	
				c.column_id
				,c.name AS column_name
				,CASE
					WHEN t.name LIKE '%VARCHAR' AND c.max_length = -1  THEN t.name+'(max)'
					WHEN t.name IN ('CHAR', 'VARCHAR')  THEN t.name+'('+CAST(c.max_length   AS VARCHAR(4))+')' 
					WHEN t.name = 'NVARCHAR' THEN t.name+'('+CAST(c.max_length/2 AS VARCHAR(4))+')' 
					WHEN t.name = 'DATETIME2' THEN t.name+'('+CAST(c.scale AS VARCHAR(1))+')'
					WHEN t.name = 'DECIMAL' THEN t.name+'('+CAST(c.precision AS VARCHAR(2))+','+CAST(c.scale AS VARCHAR(2))+')'
					ELSE t.name
				END AS column_type
				,SUBSTRING(df.definition, 2, LEN(df.definition)-2) AS DefaultVal
				,c.is_nullable
		INTO	#DestinationTableColumns
		FROM	sys.columns AS c WITH(NOLOCK)
				INNER JOIN sys.types AS t WITH(NOLOCK) ON t.system_type_id = c.system_type_id AND t.user_type_id = c.user_type_id
				LEFT JOIN sys.default_constraints AS df
				ON df.parent_object_id = c.object_id AND df.parent_column_id = c.column_id
		WHERE	c.object_id = OBJECT_ID(@DestinationTable)
			AND c.is_computed = 0
		ORDER BY c.column_id;
	
		IF @@ROWCOUNT = 0 BEGIN
			SELECT('Destination table '+@DestinationTable+' not found for session '+@SessionName) AS ERROR; RETURN
		END

		--Define the associated Target File Path for the XE session.
		DECLARE @Target_FullPath VARCHAR(512) = '';
		IF @ForceFilePath != '' AND @ForceFilePath IS NOT NULL
			SET @Target_FullPath = @ForceFilePath
		ELSE --> This works dynamically only if the session is running!
		BEGIN
			SELECT @Target_FullPath =cast(value AS nvarchar(500))
			FROM sys.server_event_sessions AS s
			INNER JOIN sys.server_event_session_fields AS f WITH(NOLOCK) ON f.event_session_id = s.event_session_id
			WHERE s.name = @SessionName
			AND f.name = 'filename'
		END

		If @Target_FullPath = '' --> If @Target_FullPath still not found, trying to guess it from indus token + session name
			SET @Target_FullPath=CAST((SELECT TOP 1 TokenValue FROM dba.Indus.Tokens WITH(NOLOCK) WHERE TokenName = 'Path_XEvents') AS VARCHAR(150))+@SessionName+'.xel'


		DECLARE @Target_DirPath		AS VARCHAR(512) = LEFT(@Target_FullPath, Len(@Target_FullPath) - CHARINDEX('\', REVERSE(@Target_FullPath))) ;
		DECLARE @Target_FileName	AS VARCHAR(200) = RIGHT(@Target_FullPath, CHARINDEX('\', REVERSE(@Target_FullPath)) -1 ) ;
		DECLARE @Original_FileName	AS VARCHAR(200) = LEFT(@Target_FileName, CHARINDEX('.xel', (@Target_FileName)) -1) ;

		IF (CHARINDEX('_0_', (@Target_FileName)) > 0  )
			SET @Original_FileName	= LEFT(@Target_FileName, CHARINDEX('_0_', (@Target_FileName)) -1) ;
	
		IF @Original_FileName LIKE '% 0 %'
			RAISERROR('Error: Logs FileName for the session Event is not supposed to contain this string " 0 "', 10, 1)

		IF @debug = 1
			PRINT('Searching for files to import in '+@Target_DirPath+' other than '+@Target_FileName)
		
		DECLARE @EventFilesToImport	AS TABLE(
			ID INT IDENTITY(1,1)
			, result_line VARCHAR(255) NULL
			, file_name AS RIGHT(result_line,  CHARINDEX(' ', REVERSE(result_line)) -1) PERSISTED --Format result_line column to get only the files names
			, file_size AS LTRIM(SUBSTRING(LEFT (result_line,LEN(result_line) - CHARINDEX(' ',  REVERSE(result_line))), CHARINDEX('   ', result_line), 999)) PERSISTED --get file size in text like '7 854' (in bytes)
			);

		--List all the files in the xel files in the folder corresponding to the selected event
		DECLARE @strCmdShell		AS VARCHAR(1024) = 'dir ' + @Target_DirPath +'\'+@Original_FileName+ '_0_*.xel /od'; -->Order By Date
		INSERT INTO @EventFilesToImport EXEC xp_cmdshell @strCmdShell;
		
		IF @debug = 1
			SELECT * FROM @EventFilesToImport 

		-- Only keep files names
		DELETE FROM @EventFilesToImport 
		WHERE  result_line IS NULL
		OR result_line NOT LIKE '%.xel'
		-- if result_line LIKE '% 0 %' means files being written

		-- Delete all supposed empty files directly to optimize speed. 
		-- Note : There should be a lot less of those since the switch to FileOffset method. Could be removed to reduce complexity.
		WHILE EXISTS (SELECT TOP 1 1 FROM @EventFilesToImport WHERE file_size = @EmptyFileSize)
		BEGIN
			DECLARE @line_empty_to_delete AS VARCHAR(200) = (SELECT TOP 1 file_name FROM @EventFilesToImport WHERE file_size = @EmptyFileSize ORDER BY result_line);
			
			SET @strCmdShell='del ' + @Target_DirPath+'\'+@line_empty_to_delete
				
			IF @Debug = 0
				EXEC xp_cmdshell @strCmdShell, no_output
			ELSE
				SELECT @EmptyFileSize AS EmptyFileConf, @line_empty_to_delete AS EmptyFileFound,  @strCmdShell AS DeleteCmd

			DELETE FROM @EventFilesToImport WHERE file_name = @line_empty_to_delete
		END

		--Don't import files if their number is above @MaxLogFilesToImport
		DELETE @EventFilesToImport WHERE ID NOT IN( SELECT TOP (@MaxLogFilesToImport) ID FROM @EventFilesToImport )
		
		IF @debug = 1
			SELECT * FROM @EventFilesToImport 

		--Import All files to the table 
		DECLARE  @query_import_xml	NVARCHAR(max)
				,@EventFileToImport  VARCHAR(200)
				,@EventFileToImport_FullPath NVARCHAR(260)
				,@LogFileStartOffset BIGINT = NULL


		WHILE EXISTS (SELECT 1 FROM @EventFilesToImport)
		BEGIN TRY
			DECLARE @FileBeingWritten BIT = NULL -- null will mean unknown
			SELECT TOP 1 
				@EventFileToImport =file_name
				,@EventFileToImport_FullPath =  @Target_DirPath+'\'+file_name
				,@FileBeingWritten =CASE WHEN result_line LIKE  '% 0 %' THEN 1 ELSE NULL END -- " 0 " size means being written
			FROM @EventFilesToImport
			ORDER BY result_line

			SET @LogFileStartOffset = (SELECT TOP 1 LogFileOffset FROM XELogFilesImport WHERE SessionName = @SessionName AND LogFileName = @EventFileToImport_FullPath) -- Do not replace the SET by SELECT 
			IF @Debug = 1 
				SELECT @EventFileToImport_FullPath AS EventFileToImport_FullPath , @FileBeingWritten AS FileBeingWritten, @LogFileStartOffset AS LogFileStartOffset

			IF OBJECT_ID('tempdb..#File_DATA_XML') IS NOT NULL DROP TABLE #File_DATA_XML
			-- Extract the raw XML data from the file into a temp table
			SELECT file_offset,
				CAST(event_data AS XML) AS event_data_XML
			INTO #File_DATA_XML
			FROM sys.fn_xe_file_target_read_file(@EventFileToImport_FullPath, null,CASE WHEN @LogFileStartOffset IS NOT NULL THEN @EventFileToImport_FullPath ELSE NULL END, @LogFileStartOffset)
			WHERE (@LogFileStartOffset IS NULL OR file_offset != @LogFileStartOffset)

			If @Debug = 1 BEGIN
				SELECT 'SELECT file_offset, CAST(event_data AS XML) AS event_data_XML
				FROM sys.fn_xe_file_target_read_file('''+@EventFileToImport_FullPath+''', null, '+CASE WHEN @LogFileStartOffset IS NOT NULL THEN ''''+@EventFileToImport_FullPath+'''' ELSE 'NULL' END +', '+ISNULL(cast(@LogFileStartOffset AS varchar(20)), 'null')+')
				ORDER BY File_name DESC
				, file_offset DESC '
				SELECT TOP 1 event_data_XML FROM #File_DATA_XML
			END

			--check if the file is empty
			IF NOT EXISTS (SELECT 1 FROM #File_DATA_XML)
			BEGIN
				IF ISNULL(@FileBeingWritten, 0) = 0 AND @LogFileStartOffset IS NULL AND @LogFileStartOffset IS NULL
				BEGIN
					--Empty file not removed previously by @EmptyFileSize check. Updating @EmptyFileSize value accordingly
					DECLARE @NewEmptyFileSizeConf VARCHAR(7) = (SELECT TOP 1 file_size FROM @EventFilesToImport WHERE file_name = @EventFileToImport)
					EXEC XE_ConfigureSession @SessionName, 'EmptyFileSize', @NewEmptyFileSizeConf, @Action ='Replace'
				END
			END
			--Create the Import Query from the temp table above to the store table based on the destination table structure
			ELSE
			BEGIN
				--Save raw xml of event parameters here in a string, to compare them with expected parameter names later
				DECLARE @event_parameters NVARCHAR(max) = CAST((SELECT TOP 1 event_data_XML FROM #File_DATA_XML) AS NVARCHAR(max))
				SET @event_parameters = LEFT(@event_parameters, CHARINDEX('>',@event_parameters))

				--Get Database Name
				DECLARE @DatabaseName [sysname] = (SELECT TOP 1 event_data_XML.value ('(/event/action  [@name=''database_name'']/value)[1]', 'sysname') FROM #File_DATA_XML)

				--Prepare the Insert Query (FROM XML table just created : File_DATA_XML)
				SET @query_import_xml	= ''
				DECLARE	 @query_import_xml_prefix	NVARCHAR(max)='' --> potentially used to add text to query before the SELECT part
						,@query_import_xml_suffix	NVARCHAR(max)='' --> potentially used to add text to query after the SELECT part

				SET @query_import_xml_prefix =' INSERT INTO '+@DestinationTable+'('
				SET @query_import_xml = '
				SELECT '

				--Complete the dynamic query with each column of the @DestinationTable
				DECLARE @i TINYINT = 0;
				WHILE EXISTS(SELECT * FROM #DestinationTableColumns  ORDER BY column_id OFFSET @i ROWS)
				BEGIN
					DECLARE @column_name sysname, @column_type sysname, @DefaultVal NVARCHAR(512), @is_nullable BIT;
					SELECT @column_name = column_name, @column_type = column_type, @DefaultVal= DefaultVal, @is_nullable = is_nullable
					FROM #DestinationTableColumns  ORDER BY column_id OFFSET @i ROWS FETCH NEXT 1 ROWS ONLY
					SET @i+=1
			
					--we'll ignore the id column
					IF @column_name = 'id'
						CONTINUE

					--Update Insert Statement with Current Field
					SET @query_import_xml_prefix += @column_name+'
					,'

					--Then treat the SELECT statement
			
					--In case there is a DBVersion/Changeset requested in the column names, outer apply done with Indus.DatabasesVersions
					IF @column_name = 'DBVersion' OR @column_name = 'Changeset' BEGIN
						IF CHARINDEX('DatabasesVersions', @query_import_xml_suffix) = 0
							SET @query_import_xml_suffix ='
							OUTER APPLY (
								SELECT TOP 1 VersionNumber AS DBVersion, Changeset
								FROM [DBAWorks].[Indus].[DatabasesVersions] AS DBv WITH(NOLOCK)
								WHERE DBv.DatabaseName = '''+@DatabaseName+'''
								AND DBv.ReleaseDate < event_data_XML.value (''(event/@timestamp)[1]'', ''DATETIME2'') -->could be an issue there between UTC timestamps and UTC+1 DB
								ORDER BY Changeset DESC
							) AS DBVersion
							'+@query_import_xml_suffix

						SET @query_import_xml +='DBVersion.'+@column_name+' AS '+@column_name+'
						,'
						CONTINUE
					END
				
					--If the column is 'statement' but with a non max value, it is replaced by the folowing which susbtrings the result(left) and also replaces the empty chars
					DECLARE @statement_special_1 NVARCHAR(100) = '';
					DECLARE @statement_special_2 NVARCHAR(100) = '';
					IF @column_name = 'statement' AND @column_type NOT LIKE ('%VARCHAR(MAX)') BEGIN 
						IF @Debug = 1
							SELECT 'statement column will be automatically truncated if needed as the size is not MAX' AS INFO
						
						DECLARE @statement_max_length VARCHAR(5) = replace(replace(replace(@column_type, 'varchar(', ''),')', ''), 'n', '')-1
						SET @statement_special_1 = 'LEFT(replace(replace(replace( '
						SET @statement_special_2 = ','' '',''<>''),''><'',''''),''<>'','' '')
						,'+@statement_max_length+')
						'
						SET @column_type = replace(@column_type, @statement_max_length+1, 'max')
						--Column type is reset to (max) but will then be truncated to the expected amount
					END

					--If the column is not nullable but with a default value, add of a isnull(... 'default') statement
					DECLARE @NULL_val_1 NVARCHAR(100) = ''
					DECLARE @NULL_val_2 NVARCHAR(100) = ''
					IF @DefaultVal IS NOT NULL AND @is_nullable = 0 BEGIN
						SET @NULL_val_1 = 'ISNULL('
						SET @NULL_val_2 = ', '+@DefaultVal+')'
					END

					-- Below are the Dynamic Xevents fields, which are searched through XML parsing . Therefore the fields have to be named correctly
					SET @query_import_xml += @column_name +' = '+@NULL_val_1+@statement_special_1+'
					event_data_XML.'+CASE WHEN @column_type != 'xml' THEN 'value' ELSE 'query' END+'(''('

					--Different formats are used for name/timestamp (event parameters) and Event Data fields
					IF @column_name = 'EventName' OR @column_name = 'EventType'
						SET @query_import_xml += 'event/@name)[1]'
					ELSE IF CHARINDEX(' '+@column_name+'=', @event_parameters) >0
						SET @query_import_xml += 'event/@'+@column_name+')[1]'
					ELSE
						SET @query_import_xml += '//*[@name='''''+@column_name+''''']/value)[1]'
				

					SET @query_import_xml +=CASE WHEN @column_type != 'xml' THEN  ''', '''+@column_type+'''' ELSE '/*''' END+')'+@statement_special_2+@NULL_val_2+' 
					,'

					--Some extra Rules
					IF @column_name = 'timestamp'
						SET @query_import_xml_suffix +='ORDER BY timestamp'

				END
				--removing last comma (,)
				SET @query_import_xml			= LEFT(@query_import_xml		, len(@query_import_xml)-1)
				SET @query_import_xml_prefix	= LEFT(@query_import_xml_prefix	, len(@query_import_xml_prefix)-1)+')'
				--adding the from
				SET @query_import_xml += '
				FROM #File_DATA_XML AS F '

				IF @Debug = 1 
					SELECT  @query_import_xml_prefix AS query_start,@query_import_xml AS query_middle,@query_import_xml_suffix AS query_end;

				--adding eventual suffixes and prefixes
				SET @query_import_xml = @query_import_xml_prefix+@query_import_xml+@query_import_xml_suffix

				--execute
				IF @Debug = 0
					exec sp_executesql @query_import_xml

			END -- > End query generation
			
		
			-- @FileBeingWritten = NULL means unknown
			IF @FileBeingWritten IS NULL  OR @FileBeingWritten = 0
			BEGIN
				--Try to move the file just imported into an archive folder (to be deleted)
				SET @strCmdSHELL = 'move '+ @Target_DirPath +'\'+ @EventFileToImport + ' ' + @Target_DirPath+'\Archives\'
				DECLARE @MoveFileResult	AS TABLE(result_line VARCHAR(255) NULL)

				IF @Debug = 0 BEGIN
					INSERT INTO @MoveFileResult EXEC xp_cmdshell @strCmdSHELL
					IF --EXISTS (SELECT 1 FROM @MoveFileResult WHERE result_line = 'The process cannot access the file because it is being used by another process.')
					 NOT EXISTS (SELECT 1 FROM @MoveFileResult WHERE result_line LIKE '%1 file(s) moved%')
						SET @FileBeingWritten = 1
					ELSE IF @LogFileStartOffset IS NOT NULL
						DELETE TOP (1) FROM  XELogFilesImport WHERE SessionName = @SessionName AND LogFileName = @EventFileToImport_FullPath
				END
				ELSE
					SELECT @strCmdSHELL AS MoveFile_cmd
					
			END
			
			--Don't use ELSE. Can be updated by previous IF
			IF @FileBeingWritten = 1 AND @Debug = 0
			BEGIN
				--Just update the XELogFilesImport table with @LogFileStartOffset
				DECLARE @LogFileLastestOffset BIGINT = (SELECT Max(file_offset) FROM #File_DATA_XML)
				IF @LogFileLastestOffset IS NOT NULL
				BEGIN
					IF @LogFileStartOffset IS NULL
						INSERT INTO XELogFilesImport (
							SessionName /*PK*/
							,LogFileName /*PK*/
							,LogFileOffset
							,ImportUTCDate)
						VALUES (@SessionName, @EventFileToImport_FullPath, @LogFileLastestOffset, GETUTCDATE())
					ELSE
						UPDATE XELogFilesImport
						SET 
							LogFileOffset		= @LogFileLastestOffset
							,ImportUTCDate		=  GETUTCDATE()
						WHERE SessionName = @SessionName AND LogFileName = @EventFileToImport_FullPath		
				END		
			END
			-- End Processing of current file
			DELETE FROM @EventFilesToImport WHERE file_name = @EventFileToImport
			DROP TABLE #File_DATA_XML

		END TRY -- end loop for this file and go to the next
		BEGIN CATCH
			DELETE FROM @EventFilesToImport WHERE file_name = @EventFileToImport

			--First Error found, log it and go to next file
			IF ISNULL(@ERROR_output, '') = ''
				SET @ERROR_output = ERROR_MESSAGE()	
			ELSE -- Second Error, log it and exit loop
			BEGIN
				SET @ERROR_output +=(CHAR(10))+ERROR_MESSAGE();
				BREAK;
			END
				
		END CATCH

	END --End single session IF

	--Throw Error if any, (but only on the last nestlevel)
	IF @@NESTLEVEL < 2 AND ISNULL(@ERROR_output,'') != ''
	BEGIN
		SELECT @ERROR_output;
		THROW 50000, @ERROR_output, 1;
	END
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [dbo].[XE_ImportSessionLogs] TO [public];
GO
