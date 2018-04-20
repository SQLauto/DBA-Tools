IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'XE_LogsToStatsMinutes')
      DROP PROCEDURE [dbo].[XE_LogsToStatsMinutes]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*
Title : Xevents Session Logs agregate into stats table. And delete old logs from origin table

Description:
	Agregates a logs table dynamically, based on the destination table columns, in a range of xx minutes (timestamp)
	- Not null columns will be used in the GROUP BY
	- Number types columns will be agregated by SUM/MIN/MAX/AVG
	- Varchar types columns which are nullable will need be managed via a cross apply where only the first of each group is kept. (order by @cross_order parameter)
	- Timestamp and Count colums have specific managements

Multi Tables usage : 
	- The function can be called without @LogsTable and @StatsTable parameters in order to recursively call itself for each of sessions tables found in XESessionsData

Parameters :
	-- @LogsTable		: Origin table for the data we want to agregate
	-- @StatsTable		: Destination table for the data
	-- @cross_order		: This is the defining ordering column if we want to have non agregate & non grouped columns which are still appearing in destination table
	-- @minutes_interval : This is the interval of time for which to agregate our logs
	-- @DeleteOlderThan : A VARCHAR value which has to be castable as a datetime2. All logs stricly older than the date specified will be deleted from the nonAgregated Table
	-- @Divider : If filled, it supposes a divider has been applied to the XEvent session, meaning it captured 1 out of x events. We can multiply back the stats to this number to get accurate stats
	-- @debug : if set to 1, will deactivate all writing actions like insert/delete

Debug :
exec [dbo].[XE_LogsToStatsMinutes] 'temp.XELogs_Rpc', 'temp.XEStats10min_Rpc', @cross_order = 'duration DESC', @minutes_interval = 10, @debug = 1

History:
	2018-03-02 - XMO - Special short statement rule
	2018-01-29 - XMO - Add Divider DataType management
	2017-07-04 - XMO - Delay import by 1 minute to allow for late dispatch latency
	2016-12-07 - XMO - nb_secs column modified for post agregate
	2016-11-22 - XMO - Added nb_secs column
	2016-09-06 - XMO - Fixed Infinite delete with no stats tables
	2016-04-08 - XMO - Removed @DeleteOlderThan default value. To not delete by default
	2016-03-11 - XMO - Add multi sessions import (with XESessionsData). Recursive
	2016-02-11 - XMO - Fixed issue when not using cross apply
	2016-02-08 - XMO - Adding smarter management of agregate suffixes
	2016-02-01 - XMO - Force auto convert SUMs types to avoid arithmetic overflows 
	2016-01-21 - XMO - Update on the timestamp for optimization 
	2016-01-08 - XMO - Adding @DeleteOlderThan as new param
	2016-01-05 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[XE_LogsToStatsMinutes]
(
	@LogsTable sysname = NULL
	,@StatsTable sysname = NULL
	,@cross_order sysname = 'default'
	,@minutes_interval varchar(6) = NULL
	,@Divider VARCHAR(5) = NULL
	,@DeleteOlderThan VARCHAR(256) = NULL
	,@debug BIT = 0
)
AS

BEGIN TRY
-- To Agregate Stats for ALL the sessions found in the table XESessionsData, don't use any @LogsTable or @StatsTable
-- This will recursively call the present proc for each session found
IF (@LogsTable IS NULL AND  @StatsTable IS NULL) BEGIN
	DECLARE @ImportSessions TABLE (ImportTable VARCHAR (128), StatsTable VARCHAR (128), DeleteOlderThan VARCHAR(256), Divider VARCHAR(5))
	DECLARE @ImportTable VARCHAR (128)
	-- (ImportTable as the same signification here as LogsTable)

	INSERT INTO @ImportSessions(ImportTable, StatsTable, DeleteOlderThan, Divider)
	SELECT DISTINCT ImportTable.Value
		, StatsTable.Value
		, CASE WHEN DeleteOlderThan.DataType IS NULL THEN @DeleteOlderThan ELSE DeleteOlderThan.Value END AS DeleteOlderThan
		, ISNULL(Divider.Value, '1') AS Divider 
	FROM XESessionsData AS ImportTable WITH(NOLOCK)
	LEFT JOIN XESessionsData AS StatsTable WITH(NOLOCK) ON ImportTable.SessionName = StatsTable.SessionName AND StatsTable.DataType = 'StatsTable' 
	LEFT JOIN XESessionsData AS DeleteOlderThan WITH(NOLOCK) ON ImportTable.SessionName = DeleteOlderThan.SessionName AND DeleteOlderThan.DataType = 'DeleteOlderThan'
	LEFT JOIN XESessionsData AS Divider WITH(NOLOCK) ON ImportTable.SessionName = Divider.SessionName AND Divider.DataType = 'Divider' 
	WHERE ImportTable.DataType = 'ImportTable'
	AND (StatsTable.value IS NOT NULL
		OR
		DeleteOlderThan.value IS NOT NULL
		)
	
	IF @Debug = 1 
		SELECT * FROM @ImportSessions

	WHILE EXISTS(SELECT 1 FROM @ImportSessions) BEGIN
		SELECT TOP (1) @ImportTable = ImportTable, @StatsTable = StatsTable, @DeleteOlderThan = DeleteOlderThan, @Divider = Divider
		FROM @ImportSessions 
		-- Recursive call for each session found
		EXEC XE_LogsToStatsMinutes @LogsTable = @ImportTable
		, @StatsTable = @StatsTable
		, @cross_order = @cross_order
		, @minutes_interval = @minutes_interval
		, @DeleteOlderThan = @DeleteOlderThan
		, @Divider = @Divider
		, @debug = @debug

		DELETE @ImportSessions WHERE ImportTable = @ImportTable and ISNULL(StatsTable, 'NoStats') = ISNULL(@StatsTable, 'NoStats')
	END
END
ELSE IF(@LogsTable IS NOT NULL ) BEGIN
	IF (@StatsTable IS NOT NULL ) BEGIN
		--Get the Names of the columns of the DestinationTable in parameter. Also the type of these column.
		--The Column names and types are defining which agregate action to apply
		IF OBJECT_ID('tempdb..#StatsTableColumns') IS NOT NULL DROP TABLE #StatsTableColumns
		SELECT	
			c.column_id,
			c.name AS column_name
			,CASE
				WHEN (t.name LIKE '%VARCHAR' OR t.name LIKE '%binary') AND c.max_length = -1  THEN t.name+'(max)'
				WHEN t.name IN ('CHAR', 'VARCHAR', 'BINARY', 'VARBINARY')  THEN t.name+'('+CAST(c.max_length   AS VARCHAR(4))+')' 
				WHEN t.name = 'NVARCHAR' THEN t.name+'('+CAST(c.max_length/2 AS VARCHAR(4))+')' 
				WHEN t.name = 'DATETIME2' THEN t.name+'('+CAST(c.scale AS VARCHAR(1))+')'
				WHEN t.name = 'DECIMAL' THEN t.name+'('+CAST(c.precision AS VARCHAR(2))+','+CAST(c.scale AS VARCHAR(2))+')'
				ELSE t.name
			END AS column_type
			,c.is_nullable
			,c.is_computed
			,CASE
				WHEN c_logs.name IS NULL THEN 0
				ELSE 1
			END AS had_equivalent
			,semi_match.previous_column_match 
		INTO #StatsTableColumns
		FROM sys.columns AS c WITH(NOLOCK)
		INNER JOIN sys.types AS t WITH(NOLOCK) ON t.system_type_id = c.system_type_id
												AND t.user_type_id = c.user_type_id
		LEFT JOIN sys.columns AS c_logs WITH(NOLOCK) ON c.name = c_logs.name AND c_logs.object_id = OBJECT_ID(@LogsTable)
		OUTER APPLY ( -- select eventual matching_name which is contained by the new name like distinct_colname / colname_sum (max one)
			SELECT TOP 1 
				semi_match.name AS previous_column_match 
			FROM sys.columns AS semi_match WITH(NOLOCK) 
			WHERE c.name LIKE '%'+semi_match.name+'%' 
			AND semi_match.object_id = OBJECT_ID(@LogsTable)
			ORDER BY CASE WHEN c.name=semi_match.name THEN 999 ELSE LEN(semi_match.name) END DESC --Take preferably perfect match or the longest match
		) AS semi_match
		WHERE	c.object_id = OBJECT_ID(@StatsTable)
		ORDER BY column_id
	
		IF @@ROWCOUNT = 0 BEGIN
			SELECT ('Logs Table '+@LogsTable+' or stats table '+@StatsTable+' not found!') AS ERROR ; RETURN
		END

		IF @Debug = 1
			SELECT * FROM #StatsTableColumns WITH(NOLOCK);

		--Fill Divider if null but needed, Only used for manual call to this function
		IF @Divider IS NULL BEGIN
			IF EXISTS (SELECT 1 FROM #StatsTableColumns WHERE column_name = 'multiplier') BEGIN
				IF Object_id('XESessionsData') IS NOT NULL BEGIN
					SELECT TOP 1 @Divider = Value FROM XESessionsData WHERE DataType = 'Divider' 
					AND SessionName = (SELECT TOP 1 SessionName FROM XESessionsData WHERE DataType = 'StatsTable' AND Value =@StatsTable) 
					SET @Divider = ISNULL(@Divider,1)
				END
			END
		END
		IF @Cross_order = 'default' BEGIN
			IF EXISTS ( SELECT 1 FROM #StatsTableColumns WHERE is_nullable = 0 AND had_equivalent = 1) --> at least one cross order condition
				AND EXISTS  ( SELECT 1 FROM #StatsTableColumns WHERE is_nullable = 1 AND had_equivalent = 1 --> at least one cross order target (like a varchar nullable)
					AND column_type NOT LIKE '%INT%'
					AND column_name NOT LIKE 'timestamp')
				SET @cross_order = 'Duration DESC'
			ELSE
				SET @cross_order = NULL
		END
		--Declaring the different parts of the full Query
		DECLARE @query_stats_prefix			NVARCHAR(max)='' --> To paste before the query
				,@query_stats_CTE 			NVARCHAR(max)='' --> The Agregate select which is done a 'with' statement
				,@query_stats_CTE_groupby	NVARCHAR(max)='' --> GROUP BY conditions within the Agregate select
				,@query_stats_insert		NVARCHAR(max)='' --> Defines the Insert statement
				,@query_stats_suffix_sel	NVARCHAR(max)='' --> The select which is done in the second part of the query. Will be * if it stays at null
				,@query_stats_cross_sel		NVARCHAR(max)=NULL --> Sets up the cross apply on the second part. Will be ignored and stay at null if not needed
				,@query_stats_cross_where	NVARCHAR(max)=NULL --> Sets up the cross apply on the second part. Will be ignored and stay at null if not needed

		IF (@minutes_interval IS NULL)
			SET @minutes_interval = SUBSTRING(@StatsTable, PATINDEX('%[0-9]%min%', @StatsTable), CHARINDEX('min', @StatsTable) - PATINDEX('%[0-9]%min%', @StatsTable) )

		-- @Min is the starting date. Next 10 minutes range, after the previous max date in the stats table. Can be null if empty, which is managed later
		SET @query_stats_prefix +='
		DECLARE @Min DATETIME2(0) = ISNULL((SELECT dateadd(mi, datediff(mi,0, MAX(timestamp)) / '+@minutes_interval+' * '+@minutes_interval+' +'+@minutes_interval+', 0) FROM '+@StatsTable+'), {d''1900-01-01''});
		--@MaxDate is delayed by 1 minute if the log is less than one minute old (to avoid losing logs due to max dispatch latency on parallel sessions)
		DECLARE @Max DATETIME2(0) = (SELECT dateadd(mi, datediff(mi,0, DATEADD(minute, CASE WHEN MAX(timestamp) > DATEADD(mi, -1, GETUTCDATE()) THEN -1 ELSE 0 END , MAX(timestamp))) / '+@minutes_interval+' * '+@minutes_interval+', 0)  FROM '+@LogsTable+');
		IF @Min = @Max
			RETURN
		'
		--Create a string for the timestamp agregate formula which will be used several times in the query
		DECLARE @str_agregate_timestamp varchar(200) = 'dateadd(mi, datediff(mi,0, timestamp) / '+@minutes_interval+' * '+@minutes_interval+', 0)'

		--Preparing the different parts of the query before the loop through Columns
		SET @query_stats_CTE+='
		;WITH stats_agregate AS(
		SELECT
		'
		SET @query_stats_CTE_groupby+='
		GROUP BY
		'
		SET @query_stats_insert+='
		INSERT INTO '+@StatsTable+'(
		'

		SET @query_stats_suffix_sel ='
		SELECT
		'

		IF @cross_order IS NOT NULL
		BEGIN
			--CROSS APPLY replaced by an OUTER APPLY. Didn't change the names though
			SET @query_stats_cross_sel='
			OUTER APPLY(
			SELECT TOP 1
			'
			SET @query_stats_cross_where='
			FROM '+@LogsTable+' AS L WITH(NOLOCK)
			WHERE L.timestamp BETWEEN stats_a.timestamp_min AND stats_a.timestamp_max
			'
		END

		--Complete the dynamic query with each column of the @StatsTable
		DECLARE @i TINYINT = 0;
		WHILE EXISTS(SELECT * FROM #StatsTableColumns  ORDER BY column_id OFFSET @i ROWS) BEGIN
			DECLARE @column_name sysname
					, @column_type sysname;
			DECLARE @is_nullable bit
					, @is_computed bit
					, @had_equivalent bit
					, @previous_column_match sysname
			SELECT @column_name = column_name
				, @column_type = column_type
				, @is_nullable = is_nullable
				, @is_computed = is_computed
				, @had_equivalent = had_equivalent
				, @previous_column_match = previous_column_match
			FROM #StatsTableColumns  ORDER BY column_id OFFSET @i ROWS FETCH NEXT 1 ROWS ONLY

			SET @i+=1
			
			-- We'll ignore the ID column and the computed columns
			IF @column_name = 'id' OR @is_computed = 1
				CONTINUE

			--Special column to calculate number of distinct seconds for sessions only running for short amounts of time
			--Note : => To be removed once not used in production tables. Replaced by Divider / Multiplier option
			IF @column_name='nb_secs'
			BEGIN
				--Get the number of seconds per group
				IF  @had_equivalent = 0
					SET @query_stats_CTE +='COUNT(DISTINCT CAST(timestamp AS DATETIME2(0))) AS nb_secs'+'
					,'
				ELSE IF @had_equivalent = 1
					SET @query_stats_CTE +='SUM(nb_secs) AS nb_secs'+'
					,'

				SET @query_stats_insert += @column_name+'
			,'
				--Only keep the largest numer
				SET @query_stats_suffix_sel += 'MAX(nb_secs) OVER(PARTITION BY timestamp) AS nb_secs'+'
			,'
				CONTINUE
			END		

			--Special Column, used if the initial session had a filter which kept only 1 event out of every X events. Insert that divider to multiply back as real stats
			IF @column_name='multiplier'
			BEGIN
				--Get the number of seconds per group
				IF  @had_equivalent = 0
					SET @query_stats_CTE +=@Divider+' AS multiplier'+'
					,'
				ELSE IF @had_equivalent = 1
					SET @query_stats_CTE +='AVG(multiplier) AS multiplier'+'
					,'

				SET @query_stats_insert += @column_name+'
			,'
				--Only keep the largest numer
				SET @query_stats_suffix_sel += 'multiplier'+'
			,'
				CONTINUE
			END		

			--Update Insert Statement with current column name
			SET @query_stats_insert += @column_name+'
			,'
			SET @query_stats_suffix_sel += @column_name+'
			,'

			--Then treat the SELECT statement

			--If the column is timestamp, this is WHERE groups of '+@minutes_interval+' minutes are done
			IF @column_name='timestamp'
			BEGIN
				SET @query_stats_CTE += @str_agregate_timestamp+' AS timestamp
				,MIN(timestamp) AS timestamp_min
				,MAX(timestamp) AS timestamp_max
				,'
				SET @query_stats_CTE_groupby+=@str_agregate_timestamp+'
				,'
				CONTINUE
			END
			-- If it's a count, we count, unless a count column already existed, then it's a sum(count) done later
			ELSE IF @column_name = 'count' AND @had_equivalent = 0
			BEGIN
				SET @query_stats_CTE +='COUNT(*) AS count'+'
				,'
				CONTINUE
			END
			--If the column is 'statement' but with a non max value, it is replaced by the following which susbtrings the result(left)
			ELSE IF @column_name = 'statement' AND @column_type NOT LIKE ('%VARCHAR(MAX)') BEGIN 						
				DECLARE @statement_max_length VARCHAR(5) = replace(replace(replace(@column_type, 'varchar(', ''),')', ''), 'n', '')-1
				SET @query_stats_cross_sel+='LEFT(statement, '+@statement_max_length+') AS statement
				,'
				CONTINUE
			END

			--If the column is not nullable, then we suppose it's a GROUP BY column
			IF @is_nullable = 0 AND @column_name!='timestamp'
			BEGIN
				SET @query_stats_CTE +=@column_name+'
				,'
				SET @query_stats_CTE_groupby+=@column_name+'
				,'
				SET @query_stats_cross_where+='
				AND stats_a.'+@column_name+' = L.'+@column_name+'
				'
				CONTINUE
			END

			--If the column is of type INT, BIGINT... the sum is calculated by default, or the _min, _max, depending on the suffix (_avg if calculated is ignored above)
			--If the Logs table had a matching _min, _max, _avg etc column .. then that column will be used for the agregate automatically
			ELSE IF  @previous_column_match IS NOT NULL
				AND (@column_type LIKE '%INT%' OR @column_type LIKE '%DEC%' OR @column_type LIKE 'DATE%')
			BEGIN
				IF @column_name LIKE '%_max'
					SET @query_stats_CTE +='MAX('+@previous_column_match+') AS '+@column_name+'
					,'
				ELSE IF @column_name LIKE '%_min'
					SET @query_stats_CTE +='MIN('+@previous_column_match+') AS '+@column_name+'
					,'
				ELSE IF @column_name LIKE '%_avg' AND  @column_type NOT LIKE 'DATE%'
					SET @query_stats_CTE +='AVG('+@previous_column_match+') AS '+@column_name+'
					,'
				ELSE IF (@column_name LIKE '%_sum' OR @had_equivalent = 1) AND  @column_type NOT LIKE 'DATE%' -- A SUM is done by default 
					SET @query_stats_CTE +='SUM(CAST('+@previous_column_match+' AS '+@column_type+')) AS '+@column_name+'
					,'
				CONTINUE
			END

			--If the column is a varchar type but is nullable, we suppose it's not meant to be grouped by so it's added to the cross apply select
			ELSE
				SET @query_stats_cross_sel+=@column_name+'
				,'
		END

		--removing last commas (,)
		SET @query_stats_CTE			= LEFT(@query_stats_CTE		, len(@query_stats_CTE)-1)
		SET @query_stats_insert			= LEFT(@query_stats_insert		, len(@query_stats_insert)-1)+')'
		SET @query_stats_CTE_groupby	= LEFT(@query_stats_CTE_groupby, len(@query_stats_CTE_groupby)-1)
		SET @query_stats_suffix_sel		= LEFT(@query_stats_suffix_sel	, len(@query_stats_suffix_sel)-1)
		SET @query_stats_cross_sel		= LEFT(@query_stats_cross_sel	, len(@query_stats_cross_sel)-1)

		--Adding the FROM in the with
		SET @query_stats_CTE += '
		FROM '+@LogsTable+' AS L WITH(NOLOCK) '


		--Adding the time conditions on the agregate first SELECT and close the WITH ()
		SET @query_stats_CTE += '
		WHERE   timestamp >= @Min
		AND		timestamp < @Max
		'
		+@query_stats_CTE_groupby
		+'
		)'

		--Adding the ordering condition for the CROSS data
		SET @query_stats_cross_where+='
		ORDER BY '+@cross_order+'
		) AS cross_data'

		SET @query_stats_suffix_sel = @query_stats_suffix_sel+'
		FROM stats_agregate AS stats_a'

		--Agregate the suffix query (after the insert)
		IF ( @query_stats_cross_sel IS NOT NULL AND @query_stats_cross_where IS NOT NULL)
			SET @query_stats_suffix_sel = @query_stats_suffix_sel+@query_stats_cross_sel+@query_stats_cross_where

		--Create the full query from the different parts
		DECLARE @SQL_query NVARCHAR(max) =
		@query_stats_prefix
		+@query_stats_CTE
		+@query_stats_insert
		+@query_stats_suffix_sel


		IF @Debug = 1
			SELECT @SQL_query
		ELSE
			EXEC sp_executesql @SQL_query
	END -- END If Stats table is not null

	IF @DeleteOlderThan IS NOT NULL BEGIN
		-- Delete/Purge Logs already agregated and older than the @DeleteOlderThan date
		-- Done in batch as there can be quite a few
		DECLARE @Batch INT = 10000;
		WHILE @Batch = 10000
		BEGIN
			DECLARE @SQL NVARCHAR(max) = '
			DELETE  TOP ('+CAST(@BATCH AS VARCHAR(10))+') FROM '+@LogsTable+'
			WHERE	timestamp < CAST('+@DeleteOlderThan+' AS DATETIME2)'

			IF @Debug = 1
				SELECT @SQL AS 'NonExecuted_Delete'
			ELSE
				EXEC sp_executesql @SQL

			SET @Batch = @@ROWCOUNT;

			IF @Batch = 10000 --> important when there are X tables with a small amount of rows to delete
				WAITFOR DELAY '00:00:01'
		END
	END
END

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [dbo].[XE_LogsToStatsMinutes] TO [public];
GO