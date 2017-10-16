IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'XE_ConfigureTable')
      DROP PROCEDURE [dbo].[XE_ConfigureTable]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : This is a helper to Create your XEvents Logs storing Table


Description: The XESession needs to be existing and running !
	The proc finds the fields that will be captured by the session and suggests also a possible ColumnType
	It automatically suggests adding an EventType and timestamp


History:
	2016-05-12 - XMO - Add Columns descriptions via extended properties
	2016-03-17 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[XE_ConfigureTable]
(
	@SessionName VARCHAR(128)
	,@Debug BIT = 0
)
AS
BEGIN TRY

DECLARE @event_session_address varbinary(8) = (SELECT address FROM sys.dm_xe_sessions AS s  WHERE s.name = @sessionName)

IF @event_session_address IS NULL BEGIN
	SELECT 'The Session '+@SessionName+' wasn''t found. It needs to be RUNNING and on the same INST' AS ERROR
	RETURN
END

DECLARE @Table_Creation VARCHAR(max)

SET @Table_Creation ='
CREATE TABLE DBA.temp.XELogs_'+@sessionName+'(
ID INT IDENTITY(1,1)	--> Can be removed as it''s not the clustered column by default
,EventType VARCHAR(56)	--> Can be removed if you want to store only one EventType
,timestamp DATETIME2(0) --> Time of the event in UTC
'

SELECT
	c.name
	,CASE 
		WHEN map.name IS NOT NULL AND map.max >= map.count*2 THEN 'VARCHAR(10)' --> Then it's probably a map bitmask like 0x00000002
		WHEN map.name IS NOT NULL AND map.max < map.count*2 AND map.max < 128 THEN 'uint8' --> Then it's a mapping id  tinyint
		WHEN map.name IS NOT NULL AND map.max > map.count*2 AND map.max >= 128 THEN 'uint16' --> Then it's a mapping id  tinyint
		ELSE c.type_name
	END AS type
	,c.description +ISNULL(' ('+LEFT(map_descr.map_values, LEN(map_descr.map_values) - 2)+')', '') AS description

INTO #columns
FROM sys.dm_xe_session_events AS e 
INNER JOIN sys.dm_xe_object_columns AS c  ON c.object_name = e.event_name
OUTER APPLY (
	SELECT max(map.map_key)AS max, count(*) -1 AS count, map.name
	FROM sys.dm_xe_map_values AS map  
	WHERE map.name = c.type_name
	AND	c.object_package_guid = map.object_package_guid
	GROUP BY map.name
) AS map
OUTER APPLY (
	SELECT LEFT((
	SELECT 
		CAST(map.map_key AS VARCHAR(3)) + ':' +map.map_value+' ,'
		AS [text()]
	FROM sys.dm_xe_map_values AS map  
	WHERE map.name = c.type_name
	AND	c.object_package_guid = map.object_package_guid	
	FOR XML PATH('')
	), 200) AS map_values
) AS map_descr
WHERE e.event_session_address = @event_session_address
	AND c.column_type = 'data' --NOT IN ('customizable', 'readonly')

UNION

SELECT 
	action_name AS name
	,o.type_name AS Type
	,o.description
FROM  
sys.dm_xe_session_event_actions AS a 
INNER JOIN sys.dm_xe_objects AS o  ON o.name = a.action_name
WHERE o.object_type = 'action'
	AND	a.event_session_address = @event_session_address

SET @Table_Creation +=
(
SELECT  ','+name+' '+
	CASE 
		WHEN type LIKE '%int64'			THEN 'BIGINT'
		WHEN type LIKE '%int32'			THEN 'INT'
		WHEN type LIKE '%int16'			THEN 'SMALLINT'
		WHEN type LIKE '%int8'			THEN 'TINYINT'
		WHEN type LIKE 'unicode_string' THEN 'VARCHAR(512)'
		WHEN type LIKE 'binary_data'	THEN 'BIT' --> Not sure
		WHEN type LIKE 'boolean'		THEN 'BIT' --> Not sure
		WHEN type LIKE 'xml'			THEN 'VARCHAR(512)' --> Not sure
		ELSE type
	END
	+CHAR(9)+' -- ' +ISNULL(description, '')
	+CHAR(10)
	AS [text()]
FROM #columns AS columns
ORDER BY name
FOR XML PATH('')
)

--Remove last coma
SET @Table_Creation +='
)
WITH(DATA_COMPRESSION=PAGE)

EXEC DBA.Tools.AddIndex @Table=''DBA.temp.XELogs_'+@sessionName+''', @Columns=''timestamp'', @index_type=''CLUSTERED''

-- In Case of intended Stats agregation. Exemple for Rpc_completed but need to adapt the columns
-- EXEC DBA.Tools.AddIndex @Table=''DBA.temp.XELogs_'+@sessionName+''', @Columns=''object_name, database_name, timestamp'', @Included_Columns=''duration''

EXEC DBA.Tools.AddDescr @Table=''DBA.temp.XELogs_'+@sessionName+''', @descr =''Table To store XEvents Logs. Imported via DBA.dbo.XE_ImportSessionLogs''

EXEC DBA.Tools.AddDescr @Table=''DBA.temp.XELogs_'+@sessionName+''', @column= ''timestamp'', @descr =''Time of the event in UTC''
'

SET @Table_Creation +=
(
SELECT  'EXEC DBA.Tools.AddDescr @Table=''DBA.temp.XELogs_'+@sessionName+''', @column= '''+columns.name+''', @descr ='''+columns.description+''''+
	+CHAR(10)
	AS [text()]
FROM #columns AS columns
WHERE description IS NOT NULL
ORDER BY name
FOR XML PATH('')
)

SELECT @Table_Creation AS Suggested_Table_Creation_Query
	
DROP TABLE #columns

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [dbo].[XE_ConfigureTable] TO [public];
GO

