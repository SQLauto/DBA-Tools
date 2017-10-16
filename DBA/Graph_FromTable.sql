IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'Graph_FromTable')
      DROP PROCEDURE [dbo].[Graph_FromTable]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*
Title : Creates a Graph in SSMS automatically using Graph_Render FROM data in an existing table

Description:  This script easily turns Table Data into a Graphic visualisation of type Area 
			It formats a query for the requested data in a way that can be correctly displayed with the Graph_Render proc

Requirements :
	This procs needs the proc Graph_Render in order to work

Parameters :
	It takes a table name in parameter and X_axis and Y axis column,
	In addition to a group column to get different objects visualized on the same graph
	It has a top condition to only select x objects of the group order by ... (in param)
	Also a restriction range which is smartly recalculated if it's a datetime	

Alternate Use :
	Display a simple Pie Chart with Only a Y column for values and Group_Column, no @Top_count or @Sort_order managed

History:
	2017-01-18 - XMO - Added @X_Interval _Type to round dates, also better external tables support
	2017-01-11 - XMO - Added Legend column and rank by default
	2017-01-09 - XMO - Removed temp tables to a simple a select. Cross apply moved to the Graph_Render
	2016-12-20 - XMO - Moved the select logic to temp tables to avoid long plans
	2016-09-09 - XMO - Add Pie visual for Two columns visuals
	2016-07-04 - XMO - Fix multi years date support for X axis
	2016-01-20 - XMO - Use the finest time interval all the time
	2016-01-18 - XMO - Add Max entries as parameter
	2016-01-13 - XMO - Creation
*/

CREATE PROCEDURE [dbo].[Graph_FromTable]
		 @Table			sysname
		,@X_column		sysname = NULL -- IF no X Column a PIE type GRaph will be created instead of AREA
		,@Y_column		sysname = 1 -- By default, does a SUM(1) 
		,@Group_Column	sysname	-- Set a non column name to force a single value as group
		,@Restrictions	VARCHAR(max) = '' -- EXEMPLE : 'timestamp = {d''2016-01-01''} ' // 'timestamp BETWEEN {d''2016-01-01''} AND {d''2016-01-07''} '
		,@Top_count		VARCHAR(3) = 12
		,@Sort_condition VARCHAR(128) = 'default' -- Specify which order to chose to determine the TOP X slected objects . By Default (SUM(Y_column))
		,@Sort_order	 VARCHAR(4) = 'DESC'
		,@Legend_Column	sysname	= 'default'
		,@Y_DownScaled	INT = 0 -- By default = 0 which means auto calculate best value
		,@Max_entries	INT = 50000 -- The Max number of results to be displayed in order to limit the rendering time
		,@Round_X		SMALLINT = NULL -- Use Round function to round the X value and smooth the graph. Exemple @Round_X=2 to round to two decimals 
		,@X_Interval	SMALLINT = NULL --For dates X_columns, you can specify a minumum interval (for agregated results)
		,@X_Interval_Type	VARCHAR(10) = 'minute' --Can be second, minute(or mi), hour, day ...
		,@Graph_Title	NVARCHAR(128) = NULL
		,@Debug			BIT = 0 -- Displays additional infos about what the proc is doing
AS
BEGIN TRY

IF @Restrictions != ''
	SET @Restrictions = ' WHERE '+@Restrictions+' '

IF @Legend_Column = 'default' OR @Legend_Column IS NULL
	SET @Legend_Column = @Group_Column

DECLARE @SQL NVARCHAR(max)
		,@ParmDefinition nvarchar(128)
		,@GraphTitle nvarchar(128) = ''
		DECLARE @OriginalObjectName SYSNAME = @Table

DECLARE @SchemaName SYSNAME
	, @DbName SYSNAME
	, @ObjectFullName SYSNAME
	, @ObjectName SYSNAME --> will be Table name

SELECT @DbName = [db_name]
	,@SchemaName = [schema_name]
	,@ObjectName = [object_name]
	,@ObjectFullName = [object_fullname] --> the schema and fullname will eventually be updated after the first query
	FROM DBA.dbo.FormatObjectName (@Table)


DECLARE @X_format NVARCHAR(512) = 'X_column' 

DECLARE @X_Column_Type SYSNAME 
	, @Group_Column_Type SYSNAME
	, @Legend_Column_Type SYSNAME
	, @Y_Column_Type SYSNAME

DECLARE @SQL_FindColumnType NVARCHAR(max) = 'USE '+@DbName+' ; 
	SELECT @Column_TypeOUT = t.name
	FROM  sys.columns AS c WITH(NOLOCK)
	INNER JOIN sys.types AS t WITH(NOLOCK) ON t.system_type_id = c.system_type_id AND t.user_type_id = c.user_type_id
	WHERE c.name = @column_name
	AND c.object_id = OBJECT_id(@ObjectFullName)'
DECLARE @Params_FindColumnType NVARCHAR(500) =N'@Column_TypeOUT sysname OUTPUT
		,@column_name sysname
		,@ObjectFullName sysname'


EXEC sp_executesql @SQL_FindColumnType
		,@Params_FindColumnType
		,@Column_TypeOUT  = @X_Column_Type OUTPUT
		,@column_name  = @X_Column 
		,@ObjectFullName = @ObjectFullName


EXEC sp_executesql @SQL_FindColumnType
		,@Params_FindColumnType
		,@Column_TypeOUT  = @Group_Column_Type OUTPUT
		,@column_name  = @Group_column 
		,@ObjectFullName = @ObjectFullName

EXEC sp_executesql @SQL_FindColumnType
		,@Params_FindColumnType
		,@Column_TypeOUT  = @Legend_Column_Type OUTPUT
		,@column_name  = @Group_column 
		,@ObjectFullName = @ObjectFullName
		
EXEC sp_executesql @SQL_FindColumnType
		,@Params_FindColumnType
		,@Column_TypeOUT  = @Y_Column_Type OUTPUT
		,@column_name  = @Y_Column 
		,@ObjectFullName = @ObjectFullName

	
IF @Debug = 1
	SELECT @X_Column_Type AS X_Column_Type, @Group_Column_Type AS Group_Column_Type , @Legend_Column_Type AS Legend_Column_Type, @Y_Column_Type AS Y_Column_Type
	
--Force 1 (which does a count) if count column doesn't exist as Y column
IF  @Y_column = 'count' AND @Y_column_Type IS NULL
	SET @Y_column = 1
	
-- Formating Y axis column
IF @Y_DownScaled = 0 
BEGIN
	SET @Y_DownScaled = 1
	DECLARE @Y_max BIGINT
	SET @ParmDefinition ='@Y_max BIGINT OUTPUT';

	SET @SQL = 'SELECT @Y_max=MAX('+@Y_column+') FROM '+@ObjectFullName+' WITH(NOLOCK) '+@Restrictions
	
	EXEC sp_executesql @SQL
		,@ParmDefinition
		,@Y_max=@Y_max OUTPUT

	WHILE @Y_max>=10000 BEGIN
		SET @Y_DownScaled *=10
		SET @Y_max/=10
	END
	
	IF @Debug = 1
		PRINT('Downscale of '+CAST(@Y_DownScaled AS VARCHAR(MAX))+' calulated for Y')
END

-- Formating X axis column
-- If it's a date/time, it is smartly formated to have a readable X axis
IF @X_Column_Type LIKE '%DATE%'
BEGIN
	DECLARE @X_min DATETIME2
			,@X_max DATETIME2
	SET @ParmDefinition ='@X_min DATETIME2 OUTPUT,@X_max DATETIME2 OUTPUT';

	SET @SQL = 'SELECT @X_min=MIN('+@X_column+') ,@X_max=MAX('+@X_column+') FROM '+@ObjectFullName+' WITH(NOLOCK) '+@Restrictions
	
	EXEC sp_executesql @SQL
		,@ParmDefinition
		,@X_min=@X_min OUTPUT
		,@X_max=@X_max OUTPUT
	
	DECLARE @seconds	VARCHAR(1000)	= '(CAST( FORMAT(X_column,''ss.ff'') AS FLOAT ))'
	DECLARE @minutes	VARCHAR(1000)	= '(CAST(FORMAT(X_column,''mm'') AS FLOAT) + '+@seconds+'/60)'
	DECLARE @hours		VARCHAR(1000)	= '(CAST(FORMAT(X_column,''HH'') AS FLOAT) + '+@minutes+'/60)'
	DECLARE @days		VARCHAR(1000)	= '(CAST(FORMAT(X_column,''dd'') AS FLOAT) + '+@hours+'/24)'
	DECLARE @months		VARCHAR(1000)	= '(CAST(FORMAT(X_column,''MM'') AS FLOAT) + '+@days+'/30.5)'
	--Not very precise with month due to 30.5 days approx
	DECLARE @years		VARCHAR(1000)	= '(CAST(DATEDIFF(s, (X_column), DATEFROMPARTS(YEAR((X_column)), 1, 1)) as FLOAT) / -31536000. + CAST(FORMAT(X_column, ''yy'') AS FLOAT) )'
	--Year changed due to GraphRender not managing to well big numbers like 2017.x

	IF  FORMAT(@X_min,'yyyy-MM-dd HH:mm') = FORMAT(@X_max,'yyyy-MM-dd HH:mm') BEGIN --same minute
		SET @X_format = @seconds
		SET @GraphTitle += 'Minute Graph for '+FORMAT(@X_min,'yyyy-MM-dd HH:mm')
	END
	ELSE IF FORMAT(@X_min,'yyyy-MM-dd HH') = FORMAT(@X_max,'yyyy-MM-dd HH') BEGIN --same hour
		SET @X_format = @minutes
		SET @GraphTitle += 'Hour Graph for '+FORMAT(@X_min,'yyyy-MM-dd HH')
	END
	ELSE IF FORMAT(@X_min,'yyyy-MM-dd') = FORMAT(@X_max,'yyyy-MM-dd') BEGIN --same day
		SET @X_format = @hours
		SET @GraphTitle += 'Day Graph for '+FORMAT(@X_min,'yyyy-MM-dd')
	END
	ELSE IF FORMAT(@X_min,'yyyy-MM') = FORMAT(@X_max,'yyyy-MM') BEGIN --same month
		SET @X_format = @days
		SET @GraphTitle += 'Month Graph for '+FORMAT(@X_min,'yyyy-MM')
	END
	ELSE IF FORMAT(@X_min,'yyyy') = FORMAT(@X_max,'yyyy') BEGIN --same year
		SET @X_format = @months
		SET @GraphTitle += 'Year Graph for '+FORMAT(@X_min,'yyyy')
	END
	ELSE BEGIN
		SET @X_format = @years
		SET @GraphTitle += 'Years Graph from '+FORMAT(@X_min,'yyyy')+ ' to '+FORMAT(@X_max,'yyyy')
	END
	--ELSE
	--	SET @X_format = '-DATEDIFF(SECOND, GETDATE(),X_column)'

	
	IF @X_Interval IS NOT NULL BEGIN
		--SET agregation By interval formula
		SET @X_format = REPLACE(@X_format,'X_column', 'DATEADD('+@X_Interval_Type+', DATEDIFF('+@X_Interval_Type+', 0, X_column)/'+CAST(@X_Interval AS SYSNAME)+' * '+CAST(@X_Interval AS SYSNAME)+', 0) ')
		/*
		--Fill the intervals table for empty intervals
		CREATE TABLE #X_AllIntervals (X_Date DATETIME2 PRIMARY KEY)
		INSERT INTO #X_AllIntervals SELECT @X_min
		DECLARE @SQL_Intervals NVARCHAR(1000)=  N'
			INSERT INTO #X_AllIntervals
			SELECT TOP 1 DATEADD('+@X_Interval_Type+', '+CAST(@X_Interval AS VARCHAR(10))+', X_Date)
			FROM #X_AllIntervals
			ORDER BY X_Date DESC'

		WHILE (SELECT MAX(X_Date) FROM #X_AllIntervals)<@X_max
			EXEC sp_executesql @SQL_Intervals
			*/
	END
END

IF @Debug = 1
	PRINT('Range for X axis :'+CAST(@X_min AS VARCHAR(MAX))+' -  '+CAST(@X_max AS VARCHAR(MAX)))

-- Parameter to limit the number of intervals to display by rounding up the X axis values
IF @Round_X IS NOT NULL
	SET @X_format= 'ROUND('+@X_format+', '+CAST(@Round_X AS VARCHAR(10))+')'

IF @Sort_condition = 'default' OR @Sort_condition IS NULL
BEGIN
	--IF @Y_column_Type IS NOT NULL
		SET @Sort_condition = ' SUM('+@Y_column+') '
	--ELSE 
		--SET @Sort_condition =@Y_column_Type
END

	
----------------------------------------------
--Generate SQL for For PIE GRAPH (no X AXIS)
----------------------------------------------
IF @X_column IS NULL
BEGIN
--No need for TOP() or ORDER BY as the PIE is automatically toping and ordering in the Graph_Render
SET @SQL = '
SELECT -- DISTINCT TOP '+CAST(@Max_entries as VARCHAR(20))+'
	'+@Group_Column+' AS Group_Column
	,SUM('+@Y_column+') AS Y_column 
FROM '+@Table+' WITH(NOLOCK)
'+@Restrictions+'
GROUP BY '+@Group_Column+'
--ORDER BY '+@Sort_condition+' '+@Sort_order+', Group_Column
'

SET @GraphTitle+= @Y_column + ' per '+@Group_Column
IF @Graph_Title IS NOT NULL 
	SET @GraphTitle = @Graph_Title

IF @Debug = 1 BEGIN
	SELECT @SQL
	EXEC sp_executesql @SQL
	SELECT 'EXEC dbo.Graph_Render @SQL= @SQL, @Graph_Type = ''PIE'', @Title = '''+@GraphTitle+'''' AS Display
END

EXEC dbo.Graph_Render @SQL= @SQL, @Graph_Type = 'PIE', @Title = @GraphTitle
END

----------------------------------------------
--Generate SQL for For AREA GRAPH (classic with X Axis)
----------------------------------------------
ELSE BEGIN

SET @SQL = '
SELECT '+
	 CASE WHEN @Legend_Column_Type IS NOT NULL 
		THEN 'CASE WHEN MAX(Ranking) OVER() > 1 THEN CAST(Ranking AS VARCHAR(3))+'': '' ELSE '''' END +'+ @Legend_Column 
		ELSE ''''+@Legend_Column+'''' 
	END +' AS Legend
	 ,'+REPLACE(@X_format,'X_column', @X_column)+' AS X
	 ,ISNULL('+@Y_column+', 0)/'+CAST(@Y_DownScaled AS VARCHAR(20))+'.  AS Y
	FROM '+@Table+' AS vals WITH(NOLOCK) 
	'+ CASE WHEN @Group_column_type IS NOT NULL THEN '
	JOIN (SELECT TOP '+@Top_count+' 
				ROW_NUMBER() OVER(ORDER BY '+@Sort_condition+' '+@Sort_order+') AS Ranking
				,'+@Group_column+' AS Group_column
			FROM '+@Table+' WITH(NOLOCK) 
			'+@Restrictions+'  
			GROUP BY '+@Group_column+' 
			ORDER BY Ranking) 
	 AS Tops ON Tops.Group_column = '+@Group_column+' 
	 ' ELSE '
	 CROSS APPLY (SELECT  1 AS Ranking ,''NoRealGroupColumn'' AS Group_column) AS Tops
	 ' END -- Not join needed when no real group column
	 +@Restrictions+'  
	 ORDER BY tops.Ranking
'

SET @GraphTitle+= ' '+CASE WHEN  @Group_column_type IS NOT NULL THEN 'TOP '+@Top_count+' '+@Group_Column+' order by '+@Sort_condition+' '+@Sort_order+'.' ELSE @Group_column END +' '
SET @GraphTitle+= CASE WHEN @X_Interval IS NOT NULL THEN 'Per '+CAST(@X_Interval AS VARCHAR(10))+' '+@X_Interval_Type+'. ' ELSE '' END
SET @GraphTitle+= 'Y: '+CASE WHEN @Y_column = '1' THEN 'count' ELSE @Y_column END+CASE WHEN @Y_DownScaled != 1 THEN '/'+FORMAT(@Y_DownScaled,'##,##0') ELSE '' END+' '+@Sort_order

IF @Graph_Title IS NOT NULL 
	SET @GraphTitle = @Graph_Title

IF @Debug = 1 BEGIN
	SELECT @SQL+'

	DECLARE @SQL NVARCHAR(max) = '' '+REPLACE (@SQL, '''', '''''')+'''
	'
	 +'EXEC dbo.Graph_Render @SQL= @SQL, @Graph_Type = ''AREA'', @Title = '''+@GraphTitle+'''' AS Display
END

EXEC dbo.Graph_Render @SQL= @SQL, @Graph_Type = 'AREA', @Title = @GraphTitle

END

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [dbo].[Graph_FromTable] TO [public];
GO
