IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'XE_RefreshSession')
      DROP PROCEDURE [dbo].[XE_RefreshSession]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*
Title : Xevents Dynamic session refresher. Drops&Recreates the session from parameters found in DBA.dbo.XESessionsData

Description:
	Takes a XE session name in parameter and creates it based on the parameters found in DBA.dbo.XESessionsData for that name.
	Updates an already existing session by drop and recreate if needed.
	Note: Could use change tracking to target which session to recreate ?

Parameters :
	-- @SessionName		: Name of our XEvent Session
	-- @debug : if set to 1, will deactivate all writing actions like insert/delete

Important : The EventType column from DBA.dbo.XESessionsData is can currently have several values which are supported :
	(Note : Could create another table with a FK to define these allowed values and provide a comment on what they are used for)
	- CustomPath : Allows to specify a Path in which to save Logs created, other than default Indus Token Report Path
	- Filter : Several can be defined for one Session (OR is then used). Allows to specify Event conditions. USE explicitly 1=1 for no filter
	- Global_Fields : Can set several values in one row, this allows to add additionnal XE Actions
	- Event_Fields : Can set several values in one row, this allows to add additionnal XE EventFields
	- Event_Type : Event type to be monitored. ONLY ONE can be set
	
GRANT EXECUTE ON TO [nobody]

History:
	2018-03-14 - XMO - Add Multi EventTypes management
	2018-01-29 - XMO - Add Divider DataType management
	2017-08-02 - XMO - Add Minor expiry rule
	2017-07-05 - XMO - Don't call refresh proc at all when not needed. To reduce process time. Removed @DropPrevSession
	2017-07-04 - XMO - Major update. Session are not stopped & restarted on every call. The same log file is kept. Only drops are doing something
	2016-11-18 - XMO - Force drop on expiry and do less full refreshes. 
	2016-09-23 - XMO - Permit no WHERE condition in some cases
	2016-03-16 - XMO - Fix and Change token from Report to new XEvents Token
	2016-03-14 - XMO - Add multi sessions refresh from XESessionsData
	2016-02-24 - XMO - Addition of Event_type for other events. And Event_field. Only one event per session can be setup
	2016-01-05 - XMO - Creation. Only supports Rpcs completed Events
*/

CREATE PROCEDURE [dbo].[XE_RefreshSession]
(
	@SessionName VARCHAR(128) = NULL
	,@Debug BIT = 0
)
AS
BEGIN TRY


-- If no session name is specificed, the proc will try to automatically find sessions to start from XESessionsData table
IF (@SessionName IS NULL) BEGIN
	DECLARE @RefreshSessions TABLE (SessionName VARCHAR (128))
	INSERT INTO @RefreshSessions(SessionName)
	SELECT DISTINCT SessionName
	FROM XESessionsData WITH(NOLOCK)
	WHERE	(
		DataType = 'Event_Type'
		AND	Value IS NOT NULL
	)
	OR ExpiryDate <= GETDATE()	

	DECLARE @LastChangeDate DATETIME2(0) =(
		SELECT Max(LastChangeDate) 
		FROM(
			--Check XESessionsData modification
			SELECT MAX(last_user_update) AS LastChangeDate
			FROM sys.dm_db_index_usage_stats AS IXStats WITH(NOLOCK)
			WHERE	object_id = OBJECT_ID('dbo.XESessionsData')
			AND		IXStats.Database_ID = DB_ID()
			UNION ALL
			--Check proc last update
			SELECT TOP 1 modify_date AS LastChangeDate
			FROM sys.procedures AS p WITH(NOLOCK)
			WHERE	object_id = @@PROCID
			UNION ALL 
			--Refresh once every day anyway to avoid having xelfiles too old
			SELECT {t'00:00:00'} AS LastChangeDate
		) AS LastChanges
	)

	IF @Debug = 1 
		SELECT s.*,dm.create_time AS Session_create_time, @LastChangeDate AS ForceUpdateMinDate
		FROM @RefreshSessions AS s
		JOIN sys.dm_xe_sessions AS dm WITH(NOLOCK) ON dm.name =s.SessionName

	--Remove sessions which don't need refreshing
	DELETE s 
	FROM @RefreshSessions AS s
	JOIN sys.dm_xe_sessions AS dm WITH(NOLOCK) ON dm.name =s.SessionName
	WHERE dm.create_time> @LastChangeDate

	--Call recursively this proc for each session. Which will do the actual refresh.
	WHILE EXISTS(SELECT 1 FROM @RefreshSessions) BEGIN
		SELECT TOP (1) @SessionName = SessionName
		FROM @RefreshSessions 
		-- Recursive call for each session found
		EXEC XE_RefreshSession @SessionName = @SessionName
		, @debug = @debug
	
		DELETE @RefreshSessions WHERE @SessionName = SessionName
	END
END
-- Single session refresh
ELSE BEGIN
	DECLARE @ExpiringSession BIT = 0 --> Check if at least one field is expiring

	--Check if the session is at least partly expiring, in which case the expired conf line is deleted and the SessionRefresh is forced
	IF EXISTS(SELECT 1 FROM DBA.dbo.XESessionsData WHERE ExpiryDate <= GETDATE() AND SessionName = @SessionName) BEGIN
		SET @ExpiringSession = 1

		--Delete obsolete filters
		If @Debug = 0
			DELETE DBA.dbo.XESessionsData WHERE ExpiryDate <= GETDATE() AND SessionName = @SessionName
		ELSE
			SELECT DataType AS ExpyringConfigurations, Value, ExpiryDate FROM DBA.dbo.XESessionsData WHERE ExpiryDate <= GETDATE() AND SessionName = @SessionName
	END

	--Check if no one is watching live data for this session to not cut his viewage (unless his session is expiring)
	IF @ExpiringSession = 0 
		AND EXISTS( 
			SELECT t.target_data FROM sys.dm_xe_sessions AS s WITH(NOLOCK)
			INNER JOIN sys.dm_xe_session_targets AS t WITH(NOLOCK) ON t.event_session_address = s.address
			WHERE	s.name = @SessionName
			AND		t.target_name = 'event_stream' 
			) 
	BEGIN
		SELECT 'Abort session '+@SessionName+' refresh because someone is watching live data for it.' AS ABORT
		--Do a blank update of XSessionData to reset next auto drop attempt
		UPDATE TOP(1) XSessionData SET SessionName = SessionName FROM XSessionData WHERE SessionName = @SessionName
		RETURN 
	END

	DECLARE @SQL_DropSession	NVARCHAR(max) = ''
	DECLARE @SQL_CreateSession	NVARCHAR(max) = ''
	DECLARE @SQL_RestartSession	NVARCHAR(max) = ''

	DECLARE @Is_Running_Session BIT
	DECLARE @Is_Existing_Session BIT 

	SET @Is_Existing_Session = IIF (EXISTS(SELECT * FROM sys.server_event_sessions WITH(NOLOCK) WHERE name=@SessionName), 1, 0)
	IF @Is_Existing_Session = 1
		SET @Is_Running_Session = IIF (EXISTS(SELECT * FROM sys.dm_xe_sessions WITH(NOLOCK) WHERE name=@SessionName), 1, 0)
	ELSE
		SET @Is_Running_Session = 0

	--Drop the session if it exists and if it's running
	IF @Is_Existing_Session = 1
		SET @SQL_DropSession = '
		DROP EVENT SESSION ['+@SessionName+'] ON SERVER
		SELECT ''Droping previous session.'' AS INFO
		'

	--SELECT The Session Data corresponding to the sessionName
	SELECT *
	INTO #SessionData
	FROM DBA.dbo.XESessionsData WITH(NOLOCK)
	WHERE SessionName = @SessionName

	--Checking if at least one Event_type and one Filter was found (no filter must be explicitly set to 1=1 or null
	IF (NOT EXISTS (SELECT 1 FROM #SessionData WHERE DataType = 'Event_Type')
		OR
		NOT EXISTS (SELECT 1 FROM #SessionData WHERE DataType = 'Filter')
		) BEGIN
		SELECT 'Minimum configuration not found in DBA.dbo.XESessionsData corresponding to '+@SessionName
			+CASE WHEN @Is_Existing_Session = 1 THEN '
			Previous session stopped and Dropped!'  ELSE '' END
			AS [ERROR																		]
		If @Debug = 0 AND @Is_Existing_Session = 1
			EXEC sp_executesql @SQL_DropSession
		RETURN
	END
	
	--Create session Query
	IF (1 = 1)
	BEGIN
		--Searching Custom Value for the folder path to store logs. Or take Indus Value (Path_report)
		DECLARE @XelDirPath VARCHAR(200) = (SELECT value FROM #SessionData WHERE DataType = 'CustomPath')
		DELETE FROM #SessionData WHERE DataType = 'CustomPath'
		IF (@XelDirPath IS NULL)
			SET @XelDirPath = CAST((SELECT TOP 1 TokenValue FROM dba.Indus.Tokens WITH(NOLOCK) WHERE TokenName = 'Path_XEvents') AS VARCHAR(150))

		DECLARE @XelFilePath VARCHAR(200)=@XelDirPath+@SessionName+'.xel'
	
		SET @SQL_CreateSession ='
		CREATE EVENT SESSION ['+@SessionName+'] ON SERVER '

		DECLARE @Event_Type VARCHAR(512)
		WHILE EXISTS (SELECT 1 FROM #SessionData WHERE DataType = 'Event_Type')
		BEGIN
			SELECT TOP 1 @Event_Type = Value FROM #SessionData WHERE DataType = 'Event_Type'

			SET @SQL_CreateSession +=' ADD EVENT '+@Event_Type
			+' ( 
			'
	 
			-- Adding Event Fields
			SET @SQL_CreateSession+= ISNULL(
			' SET '+(SELECT value FROM #SessionData WHERE DataType = 'Event_Fields')
			, ' ' ) --> if NULL, no particular Event Field will be added

			-- Adding global Fields
			SET @SQL_CreateSession+= ISNULL(
			' ACTION(
			'+(SELECT value FROM #SessionData WHERE DataType = 'Global_Fields')
			+')'
			, '' ) --> if NULL, no particular Action/ Global Field will be added

			--If at least one filter active
			IF EXISTS(SELECT 1 FROM #SessionData WHERE DataType = 'Filter' AND value IS NOT NULL AND replace(value, ' ', '') != '1=1')
			BEGIN
				SET @SQL_CreateSession+= '
					WHERE (
				'
				
				DECLARE @Filters TABLE (Value VARCHAR(2048))
				INSERT INTO @Filters
				SELECT Value FROM #SessionData WHERE DataType = 'Filter'

				--Adding the different filters
				WHILE (EXISTS(SELECT 1 FROM @Filters))
				BEGIN
					--Get the filter string
					DECLARE @FilterValue VARCHAR(2048)
					SELECT TOP 1 @FilterValue = value	FROM @Filters

					--Add it to the query
					SET @SQL_CreateSession +='('+@FilterValue+') OR'
					DELETE FROM @Filters WHERE value = @FilterValue
				END

				--Removing OR
				SET	@SQL_CreateSession = LEFT(@SQL_CreateSession, len(@SQL_CreateSession)-2)

				SET @SQL_CreateSession +='
					)
				'
			END-- End If at least one filter active

			IF EXISTS(SELECT 1 FROM #SessionData WHERE DataType = 'Divider' AND value IS NOT NULL AND value != '1')
			BEGIN
				DECLARE @DividerFilter VARCHAR(100) 
				SELECT @DividerFilter =' package0.divides_by_uint64(package0.counter,'+value+') '
				FROM #SessionData WHERE DataType = 'Divider'	ORDER BY ID

				IF @SQL_CreateSession NOT LIKE '%WHERE %'
					SET @SQL_CreateSession+= '
					WHERE '+@DividerFilter
				ELSE 
					SET @SQL_CreateSession+= ' AND '+@DividerFilter
			END --End  Filter Divider
			 
			 DELETE FROM #SessionData WHERE DataType = 'Event_Type' AND value = @Event_Type
			 SET @SQL_CreateSession +='
			 ),'
		END --End for each event_type

		SET @SQL_CreateSession = SUBSTRING(@SQL_CreateSession, 0, LEN(@SQL_CreateSession)-1) --remove last coma

		SET @SQL_CreateSession +='
		)
		ADD TARGET package0.event_file(
		SET filename		= '''+@XelFilePath+'''
			,max_file_size=(2)
			,max_rollover_files=(5000)
		)
		WITH (
			MAX_MEMORY=4096 KB
			,EVENT_RETENTION_MODE=ALLOW_MULTIPLE_EVENT_LOSS
			,MAX_DISPATCH_LATENCY='+ISNULL((SELECT value FROM #SessionData WHERE DataType = 'MaxDispatchLatency'), '30')+' SECONDS
			,MAX_EVENT_SIZE=0 KB
			,MEMORY_PARTITION_MODE=NONE
		)
	SELECT ''Creating session.'' AS INFO
	'
	END --> END of Creation of create session query
	
	--Restart session
	SET @SQL_RestartSession='
	ALTER EVENT SESSION ['+@SessionName+']
	ON SERVER
		STATE = START
	
	SELECT ''Starting session.'' AS INFO
	'
	
	--Agregate for full 'recreate session statement'
	SET @SQL_CreateSession = @SQL_DropSession+@SQL_CreateSession+@SQL_RestartSession
	
	If @Debug = 0 AND  @SQL_CreateSession != ''
		EXEC sp_executesql @SQL_CreateSession
	ELSE
		SELECT @SQL_CreateSession AS [Query generated]
	
	If @Debug = 1
	BEGIN
		--Just some help infos for the user, totally not mandatory
		DECLARE @AutoImportTable VARCHAR(128) = (SELECT value FROM #SessionData WHERE DataType = 'ImportTable')
		IF @AutoImportTable IS NULL	BEGIN
			-- Help the user to import the logs using the importLogs Function
			DECLARE @ImportLogsStr VARCHAR(2000) = 
				'EXEC DBA.dbo.XE_ImportSessionLogs
				@SessionName='''+@SessionName+''' 
				,@DestinationTable=''DBA.temp.yourXELogs_Table'' --> Replace this with your actual table name of destination
				,@MaxLogFilesToImport=999 
				,@ForceFilePath='''+@XelDirPath+@SessionName+'.xel''
				,@NbArchivesToKeep = 0
				,@Debug = 0
				'
			SELECT @ImportLogsStr AS [HELP - Manual Import Query]

			SELECT '! You can also add this session to the auto import process by configuring the Import table with XE_ConfigureSession'
				AS [INFO																				]
			UNION
			SELECT 'EXEC DBA.dbo.XE_ConfigureSession @SessionName='''+@SessionName+''' ,@ConfType=''ImportTable'' ,@Value=''DBA.temp.yourXELogs_Table'''
		END
		ELSE
			SELECT 'This session is already configured for its logs to be automatically imported to the table :'+@AutoImportTable
				AS [INFO																				]
	END

END
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

