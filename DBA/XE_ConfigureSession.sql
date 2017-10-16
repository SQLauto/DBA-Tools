IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'XE_ConfigureSession')
      DROP PROCEDURE [dbo].[XE_ConfigureSession]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : This is a helper to add/edit Xevents sessions conf into XeSessionsData Table

Description:
Use it without parameters to get some interactive help

Possible ConfTypes :
('ExpiryDate', 'Event_Type', 'Event_Fields', 'Global_Fields', 'Filter', 'ImportTable', 'StatsTable', 'CustomPath', 'DeleteOlderThan', 'MaxDispatchLatency', 'EmptyFileSize')
	
Possible Actions :
	-[Force]Add
	-[Force]Replace
	-[Force]Remove

exec dbo.XE_ConfigureSession @SessionName = @SessionName, @ConfType = @ConfType, @Value = @Value, @ExpiryDate= @ExpiryDate, @Action = @Action, @Debug = @Debug
History:
	2016-11-21 - XMO - Added EmptyFileSize conf
	2016-11-21 - XMO - Added MaxDispatchLatency conf
	2016-05-26 - XMO - Corrected some mistakes
	2016-05-13 - XMO - Changed actions organization
	2016-04-08 - XMO - Add DeleteOlderThan ConfType to allow for custom log cleanup
	2016-04-05 - XMO - Add Session Stop when conf fully removed
	2016-04-04 - XMO - Add a few fixes
	2016-03-25 - XMO - Big Update, add removals, updates, reorganized stuff
	2016-03-23 - XMO - Several additions, help messages and ExpiryDate
	2016-03-17 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[XE_ConfigureSession]
(
	@SessionName VARCHAR(128) = NULL
	,@ConfType VARCHAR(128) = NULL
	,@Value VARCHAR(MAX) = NULL
	,@ExpiryDate DATETIME2(0) = {d'1900-01-01'}
	,@Action VARCHAR(128) = 'Add'
	,@Debug BIT = 0
)
AS
BEGIN TRY

	--Set Default ExpiryDate
	IF @ExpiryDate = {d'1900-01-01'} BEGIN
		IF EXISTS(SELECT 1 FROM XeSessionsData WHERE SessionName = @SessionName)
			SELECT TOP 1 @ExpiryDate = ExpiryDate FROM XeSessionsData WHERE SessionName = @SessionName
		ELSE 
			SET @ExpiryDate = DATEADD(day, 30, GETDATE())
	END


	--Replace possible actions synonyms
	SET @Action = REPLACE(@Action, 'Delete', 'Remove')
	SET @Action = REPLACE(@Action, 'Update', 'Replace')
	--Check that the Action is recognized
	IF @Action NOT LIKE '%Add' 
		AND @Action NOT LIKE '%Replace'
		AND @Action NOT LIKE '%Remove'
	BEGIN
	SELECT 'Unrecognized Action Type used. EXIT.' AS ABORT; RETURN
	END

	
	--Search for XEvents Sessions configured and not configured'Expiry
	SELECT DISTINCT  
		s_sys.name AS [Existing sessions on server]
		, s.SessionName AS [Sessions configured in DBA Table]
		, s.DataType
		, s.Value
	INTO #Sessions
	FROM XESessionsData as s WITH(NOLOCK)
	FULL JOIN sys.server_event_sessions AS s_sys WITH(NOLOCK) ON s_sys.name = s.SessionName
	WHERE s_sys.name IS NULL OR s_sys.name NOT IN ('system_health', 'AlwaysOn_health')
	
	DECLARE @SessionConfigured BIT = CASE WHEN EXISTS(SELECT 1 SessionName FROM #Sessions WHERE [Sessions configured in DBA Table] = @SessionName )THEN 1 ELSE 0 END


	IF @SessionName IS NULL BEGIN
		SELECT 'This Proc can be used to configure New or pre-existing XEvents Sessions' AS [INFO											]
		SELECT	'Please specify a SessionName to continue' AS ERROR
				,'EXEC DBA.dbo.XE_ConfigureSession @SessionName=''''' AS HELP

		SELECT DISTINCT [Existing sessions on server], [Sessions configured in DBA Table] FROM #Sessions 

		RETURN
	END
	ELSE IF @ConfType IS NULL AND @Action NOT LIKE '%Remove' BEGIN
		
		DECLARE @StatsTableName VARCHAR(128) 
		DECLARE @ImportTableName VARCHAR(128) 

		--IF the session is already in the config
		IF EXISTS (SELECT 1 SessionName FROM #Sessions WHERE [Sessions configured in DBA Table] = @SessionName ) BEGIN
			SELECT 'Existing Conf found for the session : '+@SessionName AS [INFO									]
			SELECT * FROM XESessionsData WHERE SessionName = @SessionName

			SELECT @StatsTableName	= value FROM XESessionsData WHERE SessionName = @SessionName AND DataType = 'StatsTable'
			SELECT @ImportTableName = value FROM XESessionsData WHERE SessionName = @SessionName AND DataType = 'ImportTable'

		END
		ELSE
			SELECT	'Looks like the session '+@SessionName+' is not yet configured' AS [HELP											]	
		

		--IF Tables not configured, try to find existing ones that could match
		IF @StatsTableName IS NULL
			SET @StatsTableName = 'temp.'+(SELECT TOP 1 name FROM sys.tables WHERE schema_id = SCHEMA_ID('temp') AND name LIKE 'XEStats%_'+@SessionName) 
		IF @ImportTableName IS NULL
			SET @ImportTableName= 'temp.'+(SELECT TOP 1 name  FROM sys.tables WHERE schema_id = SCHEMA_ID('temp') AND name = 'XELogs_'+@SessionName) 


		--IF the session exists on the server
		IF EXISTS (SELECT 1 SessionName FROM #Sessions WHERE [Existing sessions on server] = @SessionName ) BEGIN
			DECLARE @event_session_id INT = (SELECT event_session_id FROM sys.server_event_sessions AS s WITH(NOLOCK) WHERE s.name = @sessionName)
			DECLARE @ExecString VARCHAR(512) = 'EXEC DBA.dbo.XE_ConfigureSession @SessionName='''+@SessionName+''' '
			 
			SELECT	1 AS [Order], 'The session '+@SessionName+' exists on the server' AS [HELP																			]
			UNION
			SELECT	2, '--If you want to copy the current server configuration, use the following commands : (Default ExpiryDate)'
			UNION
			SELECT	3, @ExecString+',@ConfType=''Event_Type'' ,@Value='''+e.package+'.'+e.name+''''
			FROM sys.server_event_session_events AS e WITH(NOLOCK)
			WHERE event_session_id=@event_session_id
			UNION
			SELECT 4, @ExecString+',@ConfType=''Event_Fields'' ,@Value='''+f.name+'='+CAST(f.value AS VARCHAR(256))+''''
			FROM sys.server_event_session_fields AS f WITH(NOLOCK)
			WHERE event_session_id=@event_session_id
			AND	f.name NOT IN('filename','max_file_size', 'max_rollover_files')
			UNION
			SELECT 5, @ExecString+',@ConfType=''Global_Fields'' ,@Value='''+a.package+'.'+a.name+''''
			FROM sys.server_event_session_actions AS a WITH(NOLOCK)
			WHERE event_session_id=@event_session_id
			UNION
			SELECT 6, @ExecString+',@ConfType=''Filter'' ,@Value='''+REPLACE(e.predicate, '''', '''''')+''''
			FROM sys.server_event_session_events AS e WITH(NOLOCK)
			WHERE event_session_id=@event_session_id
			AND e.predicate IS NOT NULL
			UNION
			SELECT 7,
				CASE WHEN  @ImportTableName IS NOT NULL
					THEN	@ExecString+',@ConfType=''ImportTable'' ,@Value='''+@ImportTableName+''''
				ELSE
					'-- Don''t forget to create and configure a table to Import the Logs, like DBA.temp.XELogs_'+@SessionName+'
					-- Once the session is running, you can use :
					-- EXEC DBA.dbo.XE_ConfigureTable @SessionName='''+@SessionName+''''
				END
			UNION
			SELECT 8,
				CASE WHEN  @StatsTableName IS NOT NULL
					THEN	@ExecString+',@ConfType=''StatsTable'' ,@Value='''+@StatsTableName+''''
				ELSE
					'-- No agregation StatsTable found. (optional)'
				END
			UNION
			SELECT 10, '-- Add this to update the ExpiryDate for the whole session (use NULL for no ExpiryDate)' 
			UNION
			SELECT 10, @ExecString+',@ConfType=''ExpiryDate'' ,@Value=''DATEADD(day, 30, GETDATE())'''

		END
		
	END
	--Conf Removal management
	ELSE IF @ConfType IS NULL AND @Action LIKE '%Remove' BEGIN
		IF @SessionConfigured = 1 BEGIN
			SELECT '! Wargning, existing Conf found for the session : '+@SessionName AS WARNING
			SELECT * FROM XESessionsData WHERE SessionName = @SessionName
			IF @Action LIKE 'Force%' BEGIN
				SELECT 'Removing all current conf for this session' AS INFO
				DELETE XESessionsData  WHERE SessionName = @SessionName
				
				SELECT 'Configuration removed' AS SUCESS
				--Sopping the session
				EXEC XE_RefreshSession @SessionName
			END
			ELSE
				SELECT 'IF you''re sure you want to REMOVE ALL the current conf for this session, please use @Action=''ForceRemove'' '
				AS	[HELP													]
				UNION
				SELECT 'If you want to only remove a specific conf, use @ConfType = ...'
		END
		ELSE
			SELECT	'No conf found for this session :'+@SessionName AS [INFO											]	

	END
	--ExpiryDate update management
	ELSE IF @ConfType = 'ExpiryDate'
	BEGIN
		--Update the Session's expiry date for all it's conf params
		DECLARE @query_update_ExpiryDate NVARCHAR(MAX) = '
		DECLARE @ExpiryDate DATETIME2 = '+ISNULL(@Value, 'NULL')+'
		UPDATE XESessionsData SET ExpiryDate = @ExpiryDate
		WHERE SessionName = '''+@SessionName+''''
		
		exec sp_executesql @query_update_ExpiryDate

		SELECT @SessionName+' ExpiryDate set to '
			+ISNULL(CAST((SELECT TOP 1 ExpiryDate FROM XeSessionsData WHERE SessionName = @SessionName) AS VARCHAR(50)), 'NULL')
			AS [SUCCESS																	]
	END
	--Specific Conf ADD/UPDATE/DELETE Management
	ELSE IF @ConfType IN('Event_Type', 'Event_Fields', 'Global_Fields', 'Filter', 'ImportTable', 'StatsTable', 'CustomPath', 'DeleteOlderThan', 'MaxDispatchLatency', 'EmptyFileSize')
	BEGIN
		IF @ConfType IN( 'Global_Fields' , 'Event_Fields' )	AND @Action != 'Remove' BEGIN
			IF @Action = 'Add'
				IF NOT EXISTS(SELECT 1 FROM XESessionsData WITH(NOLOCK) WHERE SessionName = @SessionName AND DataType = @ConfType) BEGIN
					INSERT INTO XESessionsData (SessionName,DataType,Value,ExpiryDate)
					VALUES (@SessionName, @ConfType, @Value, @ExpiryDate)

					SELECT 'Conf Added successfully : '+@ConfType+' '+@Value AS [SUCCESS																		]
				END
				ELSE IF EXISTS(SELECT 1 FROM XESessionsData WITH(NOLOCK) WHERE SessionName = @SessionName AND DataType = @ConfType AND value LIKE '%'+@value+'%')
					SELECT 'This '+@ConfType+' : '+@value+' already exists for this session' AS ABORT
				ELSE BEGIN
					UPDATE XESessionsData 
					SET Value = Value+','+@value
					,ExpiryDate = @ExpiryDAte
					WHERE SessionName = @SessionName AND DataType = @ConfType
					
					SELECT 'Conf Added successfully : '+@ConfType+' '+@Value AS [SUCCESS																		]
				END
			ELSE	
				 SELECT 'Other actions not implemented for '+@ConfType+', you can use @Action=''Remove'' or @Action=''Add'''
				
		END
		ELSE BEGIN
			DECLARE @ConfAlreadyExists BIT = 0
			DECLARE @ValueAlreadyExists BIT = 0

			SELECT	@ConfAlreadyExists = CASE WHEN ExistingConf.value IS NOT NULL THEN 1 ELSE 0 END
				,	@ValueAlreadyExists= CASE WHEN ExistingValue.value IS NOT NULL THEN 1 ELSE 0 END
			FROM XESessionsData AS ExistingConf WITH(NOLOCK)
			LEFT JOIN XESessionsData AS ExistingValue WITH(NOLOCK)
				ON ExistingValue.SessionName = @SessionName 
				AND ExistingValue.DataType = @ConfType
				AND ExistingValue.Value = @Value
			WHERE	ExistingConf.SessionName = @SessionName 
			AND		ExistingConf.DataType = @ConfType 

			IF @Debug = 1
			SELECT @ConfAlreadyExists AS ConfAlreadyExists,
				@ValueAlreadyExists AS ValueAlreadyExists

			-- In case of Table conf, we check if the table exists
			IF (@ConfType = 'ImportTable' OR @ConfType = 'StatsTable') AND @Action NOT LIKE '%Remove' BEGIN
			
				--Splitting the Table value into DB+schema+name
					DECLARE @ReverseString varchar(max)=reverse(@value)+'...'
					DECLARE @TableName  sysname	= reverse(LEFT(@ReverseString, charindex('.',@ReverseString) -1))
					SET @ReverseString = SUBSTRING(@ReverseString, charindex('.',@ReverseString) +1, LEN(@ReverseString))
					DECLARE @SchemaName sysname = reverse(LEFT(@ReverseString, charindex('.',@ReverseString) -1))
					SET @ReverseString = SUBSTRING(@ReverseString, charindex('.',@ReverseString) +1, LEN(@ReverseString))
					DECLARE @DbName		sysname	= reverse(LEFT(@ReverseString, charindex('.',@ReverseString) -1))


				IF NOT EXISTS (SELECT 1 FROM sys.tables 
								WHERE schema_id = SCHEMA_ID(case @SchemaName WHEN '' THEN 'dbo' ELSE @SchemaName END) 
								AND name = @TableName) 
					AND @Action NOT LIKE 'Force%'
				BEGIN
					SELECT 'Table : '+@value+' Not found in DBA Database (can''t use another). Check schema? Or to Force, use @Action=''ForceAdd''' AS ERROR
					RETURN
				END
			END --Check on Table existance
			
			-- adding a conf which doesn't exist yet
			IF (@Action LIKE '%Add' OR (@Action LIKE '%Replace' AND @ConfAlreadyExists = 0)) BEGIN
				IF @Value IS NULL BEGIN
					SELECT 'Can''t set NULL value for this conf' AS [ABORT								]
					RETURN
				END
				IF @ConfAlreadyExists = 0 OR (@ConfType= 'Filter' AND @ConfAlreadyExists = 1 AND @ValueAlreadyExists = 0)
				BEGIN
					INSERT INTO XESessionsData (SessionName,DataType,Value,ExpiryDate)
					VALUES (@SessionName, @ConfType, @Value, @ExpiryDate)

					SELECT 'Conf Added successfully : '+@ConfType+' '+@Value AS [SUCCESS												]
				END
				ELSE IF @ConfAlreadyExists = 1 AND @ValueAlreadyExists = 1
					SELECT 'Conf Already added : '+@ConfType+' '+@Value AS [ABORT														]
				ELSE IF @ConfAlreadyExists = 1 AND @ValueAlreadyExists = 0
					BEGIN
					SELECT 'This Conf : '+@ConfType+' '+@Value+' already exists for this session' AS [ABORT								]
					UNION
					SELECT 'Use @Action =''Replace'' to force an update'
				END
				ELSE
					SELECT 'Impossible case ? See code ERROR 42'
			END
			ELSE IF (@Action LIKE '%Replace') BEGIN
				IF @Value IS NULL BEGIN
					SELECT 'Can''t set NULL value for this conf' AS [ABORT								]
					RETURN
				END
				-- @ConfAlreadyExists can't be at 0 AS it would match previous case
				IF @ConfType = 'Filter' BEGIN
					EXEC dbo.XE_ConfigureSession @SessionName = @SessionName, @ConfType = @ConfType, @Value = NULL  , @ExpiryDate= @ExpiryDate, @Action = 'Remove', @Debug = @Debug
					EXEC dbo.XE_ConfigureSession @SessionName = @SessionName, @ConfType = @ConfType, @Value = @Value, @ExpiryDate= @ExpiryDate, @Action = 'Add', @Debug = @Debug
				END
				ELSE BEGIN
					UPDATE XESessionsData 
					SET SessionName=@SessionName
						,DataType=@ConfType
						,Value=@Value
						,ExpiryDate=@ExpiryDate
					WHERE SessionName = @SessionName AND DataType = @ConfType
				END
				SELECT 'Existing Conf replaced successfully : '+@ConfType+' '+@Value AS [SUCCESS						]
			END
			-- Remove/Delete
			ELSE IF @Action LIKE '%Remove'
			BEGIN
				IF @ConfAlreadyExists = 1 AND( @ValueAlreadyExists = 1 OR @value IS NULL) BEGIN

					DELETE XESessionsData 
					WHERE SessionName = @SessionName
					AND DataType = @ConfType
					AND (@Value IS NULL OR value = @Value)
				
					SELECT 'Conf Removed successfully for the session : '+@ConfType+' '+isnull(@Value, '') AS [SUCCESS							]
				END
				ELSE
					SELECT 'This Conf was not found for removal : '+@ConfType+' '+isnull(@Value, '') AS [ABORT									]	
			END
			ELSE
				SELECT 'Impossible case ? See code ERROR 666'
			
		END

	END
	ELSE 
		SELECT 'ConfType/Session not recognized please refer to the proc description' AS [ERROR													]
		
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [dbo].[XE_ConfigureSession] TO [public];
GO

