IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'Graph_Render')
      DROP PROCEDURE [dbo].[Graph_Render]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Renders/display graphs from results of specific SELECT statements within SSMS

Description:  This script creates stored procedures and functions that turn a 
              given SELECT statement into a Graph or a drawing representing 
              selected data. Supports Graph types LINE, COLUMN, BAR, PIE, and 
              AREA, and a special type PAINT for free-style drawings.

Requirements :
	This procs needs the functions Graph_WKT_Letter and Graph_WKT_String created in the same database

Parameters :
	Described Below

History:
	2017-01-09 - XMO - Replaced the original Cross apply logic to better adapt our usual scenario 
	2016-05-24 - XMO - Change select and display only geo results
	2016-02-09 - XMO - Increase Title max length
	2016-01-11 - XMO - Creation from internet proc dbo.Chart by Andrew Zanevsky
*/

CREATE PROCEDURE [dbo].[Graph_Render]
	@SQL         varchar(max),           -- A query selecting: [for PIE Graphs] SeriesName,Y or [for all other Graph types] SeriesName,X,Y 
                                             -- For example: 'SELECT ProductName, FiscalYear, SalesAmount FROM Sales ORDER BY ProductName, FiscalYear'
	@Graph_Type  varchar(20)   = 'AREA', -- LINE, AREA, COLUMN, BAR, PIE, or PAINT (any other value is treated as LINE)
	@Title       varchar(256)  = '',     -- Graph title (NULL or '' = no title)
	@Y_Base      float         = NULL,   -- Optional parameter for AREA and COLUMN Graphs: base value for the Y axis. NULL = the lesser of min(Y) or 0.
	@Y_Scale     float         = NULL,   -- 1 grid unit on the Y axis is worth @Y_Scale units of data in the Y column of the given SELECT query:
                                             -- NULL = let the procedure automatically scale the image for optimal presentation on a typical screen; 
                                             -- 1 = no scaling; >1 = reduce the height of the image; (0...1) = extend the height of the image; 0 = causes an error;
                                             -- <0 - flip the image upside down and scale it the same way as ABS(@Y_Scale) would scale it.
	@Scaling25   bit           = 1,      -- Only used if automatic scaling is allowed (@Y_Scale=NULL):
                                             -- 0 = only scale to powers of 10; 1 = scale to 10^n, 2*10^n, or 5*10^n
	@Col_Width   float         = 0.6,    -- For COLUMN Graphs, defines the width of each column as a fraction of 1 
                                             -- (e.g. 1 = no gaps between columns; 0.6 = each column fills 60% of the width leaving 40% gaps between columns)
	@All_Other   float         = 0.02,   -- For PIE Graphs, all values smaller than this fraction of the total are grouped into one "All Other" pie slice.
	@Legend      bit           = NULL,   -- Print legend (series names provided by the given SELECT query)? 1=yes/0=no. Default: 0 for PAINT Graphs, 1 for other types.
	@Legend_Over bit           = 0,      -- For BAR Graphs, defines if the procedure should print series names inside bars instead of on the right side. 1=yes/0=no
	@Y_Grid      bit           = NULL,   -- Print Y grid values on the Y axis? 1=yes/0=no. Default: 0 for PAINT Graphs, 1 for other types.
                                             -- Note that automatically printed grid values on the X axis correspond to actual X values provided by the given 
                                             -- SELECT query, but grid values on the Y axis only match actual Y values provided by the query when the @Y_Scale=1 
                                             -- or when @Y_Scale=NULL and auto-scaling chooses the scale of 1. Therefore, it is recommended to use @Y_Grid=1,
                                             -- because it makes the procedure print real Y grid values.
	@Skip_Colors varchar(1000) = -- Comma-separated list ofcolor numbers to skip (exec dbo.Graph_Colors to get the color Graph with their ids)
'1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,67,68,69,70,71,72,73,74,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,161,162,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,181,182,183,184,185,186,187,188,189,190,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,232,233,234,235,237,238,239,240,241,242,243,245,246,247,248,249,250,251,252,253,254,255,256'
	,@Show_WKT    bit           = 0,      -- Print WKT string? 1=yes/0=no  This may be useful if you want to embed the graphics into another procedure.
                                             -- Note that when @Skip_Colors is not NULL or blank, then the resulting WKT will include lines for skipped colors. 
                                             -- These lines will be all the same and generally shorter than WKT representing actual data series from the given SELECT.
	@Debug       int           = 0       -- Reserved for system maintenance
AS
BEGIN TRY
SET NOCOUNT ON;

DECLARE @Min_X			 float, 
		@Max_X			 float, 
		@Min_Y			 float, 
		@Max_Y			 float,
		@Zero_Y			 float,
		@Y_Base_Use		 float,
		@X_Count		 int, 
		@Col_Half		 float, 
		@Scale			 varchar(20),
		@N				 int,
		@First			 bit,
		@Char			 char(1),
		@New_Char		 char(1),
		@Len			 int,
		@Series_#		 int,
		@Series_Legend	 varchar(100),
		@Series_Count	 int,
		@Pie_Segment	 float,
		@Legend_Scale_X	 float,
		@Legend_Scale_Y	 float,
		@Legend_Offset_X float,
		@Legend_Offset_Y float,
		@Step			 float,
		@Step_Str		 varchar(100),
		@Y_Legend		 varchar(100),
		@Y				 float,
		@G				 varchar(max),
		@Break			 int,
		@Color			 int,
		@Skip_Color_WKT  varchar(max),
		@X0              float,
		@Y0              float,
		@X1              float,
		@Y1              float;

SET @Legend = COALESCE( @Legend, CASE WHEN @Graph_Type = 'PAINT' THEN 0 ELSE 1 END );
SET @Y_Grid = COALESCE( @Y_Grid, CASE WHEN @Graph_Type = 'PAINT' THEN 0 ELSE 1 END );

CREATE TABLE #Data ( 
	OrderBy	int IDENTITY(1,1), 
	Legend	varchar(100) NULL, 
	X		float NULL, 
	Y		float NULL, 
	Y0		float NULL
);
CREATE NONCLUSTERED INDEX IX_#Data_X ON #Data(X) WITH (FILLFACTOR = 100) 

CREATE TABLE #Legend ( 
	ID			int IDENTITY(1,1), 
	Legend		varchar(100) NOT NULL PRIMARY KEY CLUSTERED,
	Pie_Segment	float NULL
);
CREATE TABLE #Area ( 
	Legend	varchar(100) NOT NULL,
	N		int NOT NULL,
	X		float NOT NULL, 
	Y		float NOT NULL
);
CREATE CLUSTERED INDEX IX_#Area_N ON #Area(N) WITH (FILLFACTOR = 100) 
CREATE NONCLUSTERED INDEX IX_#Area_X ON #Area(X) WITH (FILLFACTOR = 100) 

CREATE TABLE #Pie ( 
	OrderBy	int IDENTITY(1,1), 
	Legend	varchar(100) NULL, 
	Y		float NULL
);


--This is WHERE the result of the sql is inserted into data tables

IF @Graph_Type = 'PIE' EXEC( 'insert #Pie ( Legend, Y ) ' + @SQL );
ELSE EXEC( 'insert #Data ( Legend, X, Y ) ' + @SQL );
IF @@ROWCOUNT = 0 BEGIN
	SELECT 'No Results found to display' AS ABORT
	RETURN;
END

IF @Debug = 1 SELECT Debug = '#Data', * FROM #Data;

INSERT	#Legend ( Legend )
SELECT	Legend = COALESCE( Legend, '' )
FROM	#Data
GROUP BY Legend
ORDER BY MIN( OrderBy );

SELECT	@Series_Count = @@ROWCOUNT;

IF @Debug = 1 SELECT Debug = '#Legend', * FROM #Legend;

IF @Graph_Type = 'PIE' SELECT @Min_X = -1, @Max_X = 1;
ELSE SELECT	@Min_X   = MIN( X ), 
			@Max_X   = MAX( X + CASE WHEN @Graph_Type = 'BAR' THEN Y ELSE 0 END ),
			@X_Count = COUNT( DISTINCT X )
	 FROM	#Data;

IF @Graph_Type = 'AREA' BEGIN;
	INSERT	#Area ( Legend, N, X, Y )
	SELECT	Legend, ID, X, SUM( Y )--replaced the max by a sum
	FROM (	SELECT	l.Legend, 
					l.ID, 
					x.X, 
					ISNULL(d.Y,0) AS Y
					/* Replaced this calculation method
					Y = COALESCE( d.Y, ( 
								  SELECT  TOP 1 dd.Y 
								  FROM	  #Data dd 
								  WHERE   dd.Legend = l.Legend 
								  AND	  dd.X < x.X 
								  ORDER BY dd.X DESC ), 
								  0 )
								  */
			FROM ( SELECT DISTINCT X FROM #Data ) x
			CROSS JOIN #Legend l
			LEFT JOIN  #Data d
				ON	d.X = x.X
				AND d.Legend = l.Legend
		 ) z
	GROUP BY Legend, ID, X;

	IF @Debug = 1 SELECT Debug = '#Area', * FROM #Area;

	TRUNCATE TABLE #Data;

	WITH Area1 ( N, Legend, X, Y, Part ) AS (
		SELECT	N, Legend, X, Y, Part = 1
		FROM	#Area 
		WHERE	N > 0
		UNION ALL
		SELECT	a.N, l.Legend, a.X, a.Y, Part = -1
		FROM	#Area a 
		JOIN	#Legend l ON a.N = l.ID - 1
	)
	INSERT	#Data ( Legend, X, Y )
	SELECT	a.Legend, a.X, Y = SUM( B.Y )
	FROM	Area1 a
	JOIN	Area1 b ON b.N <= a.N AND b.X = a.X
	WHERE	b.Part = 1
	GROUP BY a.Legend, a.X, a.Part
	ORDER BY a.Legend, a.Part DESC, a.Part * a.X;

	INSERT	#Data ( Legend, X, Y )
	SELECT	Legend, CASE WHEN N = 0 THEN X0 ELSE X1 END, 0
	FROM	   ( SELECT TOP 1 Legend FROM #Legend ORDER BY ID ) l
	CROSS JOIN ( SELECT X0 = MIN( X ), X1 = MAX( X ) FROM #Area ) x
	CROSS JOIN ( SELECT N = 0 UNION ALL SELECT N = 1 ) n
	ORDER BY N DESC;

	IF @Debug = 1 SELECT Debug = '#Data', * FROM #Data;
END;
ELSE IF @Graph_Type = 'COLUMN' BEGIN;
	UPDATE	d1
	SET		Y0 = (	SELECT	SUM( Y )
					FROM	#Data d0
					JOIN	#Legend l0 ON l0.Legend = d0.Legend
					WHERE	l0.ID < l1.ID
					AND		d0.X = d1.X
				 )
	FROM	#Data d1
	JOIN	#Legend l1 ON l1.Legend = d1.Legend
	WHERE	l1.ID > 1;

	IF @Debug = 1 SELECT Debug = '#Data', * FROM #Data;
END;
ELSE IF @Graph_Type = 'PIE' BEGIN;
	DECLARE @Sum float, @All_Other_Sum float, @All_Other_Count int;

	SELECT	@Sum = SUM( Y ) FROM #Pie;
	SELECT	@All_Other_Sum = SUM( Y ), 
			@All_Other_Count = COUNT(*) 
	FROM	#Pie 
	WHERE	Y / @Sum < @All_Other;

	IF @All_Other_Count > 1 BEGIN;
		DELETE #Pie WHERE Y / @Sum < @All_Other;
		INSERT #Pie ( Legend, Y ) VALUES ( 'All Other', @All_Other_Sum );
	END;

	UPDATE #Pie SET Y = @Sum / 1000. WHERE Y < @Sum / 1000.;

	IF @Debug = 1 SELECT [#Pie] = '#Pie', * FROM #Pie;

	IF @Debug > 0
			SELECT	a.Legend,
					a.OrderBy,
					Y1 = ROUND( COALESCE( SUM( b.Y / @Sum * 1000. ), 0 ), 0 ),
					Y2 = ROUND( ( COALESCE( SUM( b.Y ), 0 ) + a.Y ) / @Sum * 1000., 0 )
			FROM	#Pie a
			LEFT JOIN #Pie b ON b.OrderBy < a.OrderBy
			GROUP BY a.Legend, a.OrderBy, a.Y;

	INSERT	#Data ( Legend, X, Y )
	SELECT	r.Legend, 
			CASE WHEN n.N IN ( -1, 1001 ) THEN 0 ELSE n.X END,
			CASE WHEN n.N IN ( -1, 1001 ) THEN 0 ELSE n.Y END
	FROM (	SELECT	a.Legend,
					a.OrderBy,
					Y1 = ROUND( COALESCE( SUM( b.Y / @Sum * 1000. ), 0 ), 0 ),
					Y2 = ROUND( ( COALESCE( SUM( b.Y ), 0 ) + a.Y ) / @Sum * 1000., 0 )
			FROM	#Pie a
			LEFT JOIN #Pie b ON b.OrderBy < a.OrderBy
			GROUP BY a.Legend, a.OrderBy, a.Y
		 ) r
	JOIN (	SELECT	TOP 1003 N, X = SIN( N * PI() * .002 ), Y = COS( N * PI() * .002 )
			FROM (	SELECT	N = A + B + C + D + E + F + G + H + I + J - 1
					FROM	   ( SELECT A = 0 UNION ALL SELECT A =   1 ) a
					CROSS JOIN ( SELECT B = 0 UNION ALL SELECT B =   2 ) b
					CROSS JOIN ( SELECT C = 0 UNION ALL SELECT C =   4 ) c
					CROSS JOIN ( SELECT D = 0 UNION ALL SELECT D =   8 ) d
					CROSS JOIN ( SELECT E = 0 UNION ALL SELECT E =  16 ) e
					CROSS JOIN ( SELECT F = 0 UNION ALL SELECT F =  32 ) f
					CROSS JOIN ( SELECT G = 0 UNION ALL SELECT G =  64 ) g
					CROSS JOIN ( SELECT H = 0 UNION ALL SELECT H = 128 ) h
					CROSS JOIN ( SELECT I = 0 UNION ALL SELECT I = 256 ) i
					CROSS JOIN ( SELECT J = 0 UNION ALL SELECT J = 512 ) j
				 ) N
			ORDER BY N
		 ) n ON ( r.Y1 >= 1000 AND n.N BETWEEN 999  AND 1000     ) -- handle too-thin slices
			 OR ( r.Y1  = r.Y2 AND n.N BETWEEN r.Y1 AND r.Y1 + 1 ) -- handle too-thin slices
			 OR n.N BETWEEN r.Y1 and r.Y2
			 OR n.N IN ( -1, 1001 ) -- start and end at the center
	ORDER BY n.N, r.OrderBy;

	IF @Debug = 1 SELECT Debug = '#Data', * FROM #Data;

	SET @Y_Scale = 1;

	INSERT	#Legend ( Legend, Pie_Segment ) 
	SELECT	a.Legend,
			Pie_Segment = PI() * ( 2.5 - ( COALESCE( SUM( b.Y ), 0 ) + a.Y * .5 ) / @Sum * 2. )
	FROM	#Pie a
	LEFT JOIN #Pie b ON b.OrderBy < a.OrderBy
	GROUP BY a.Legend, a.OrderBy, a.Y
	ORDER BY a.OrderBy;
END;

SELECT	@Y_Base_Use = CASE WHEN COALESCE( @Y_Base, Min_Y ) >= Max_Y THEN Max_Y - ( Max_Y - Min_Y ) * .01 ELSE COALESCE( @Y_Base, Min_Y ) END
FROM	( SELECT Min_Y = MIN( CASE WHEN Y0 < Y THEN Y0 ELSE Y END ), Max_Y = MAX( Y ) FROM #Data ) d;

IF @Y_Base_Use > 0 AND @Y_Base IS NULL SET @Y_Base_Use = 0;

IF @Debug = 1 SELECT [@Y_Base_Use] = @Y_Base_Use;
IF @Debug = 1 SELECT [#Legend] = '#Legend', * FROM #Legend;

SELECT	@Min_Y = CASE WHEN @Graph_Type = 'BAR' THEN 0
					  WHEN @Graph_Type in ( 'COLUMN', 'AREA' ) THEN @Y_Base_Use
					  ELSE MIN( Y )
				 END, 
		@Max_Y = CASE WHEN @Graph_Type = 'BAR' THEN @Series_Count
					  WHEN @Graph_Type in ( 'COLUMN', 'AREA' ) THEN CASE WHEN MAX( COALESCE( Y0, 0 ) + Y ) < @Y_Base_Use THEN @Y_Base_Use ELSE MAX( COALESCE( Y0, 0 ) + Y ) END
					  ELSE MAX( Y )
				 END
FROM	#Data;

IF @Debug > 0 SELECT 'debug 1', [@Min_X] = @Min_X, [@Max_X] = @Max_X, [@Min_Y] = @Min_Y, [@Max_Y] = @Max_Y, [@X_Count] = @X_Count, [@Y_Scale] = @Y_Scale;

IF @Y_Scale IS NULL BEGIN;
	SET @Scale = CAST( ( @Max_Y - @Min_Y ) / 
				 CASE WHEN @Max_X - @Min_X = 0 
					  THEN 1 
					  ELSE @Max_X - @Min_X 
				 END * 4. AS varchar(30) );

	SELECT	@N     = 1, 
			@First = 1, 
			@Char  = LEFT( @Scale, 1 ), 
			@Len   = LEN( @Scale );

	WHILE @N <= @Len AND @Char <> 'e' BEGIN;
		IF @Char LIKE '[1-9]' BEGIN;
			SET @Scale = STUFF( @Scale, @N, 1, 
						CASE WHEN @First = 0 THEN '0' 
							 WHEN @Scaling25 = 0 THEN '1'
							 ELSE SUBSTRING( '122255555', CAST( @Char AS int ), 1 )
						END );
			SET @First = 0;
		END;
		SET @N = @N + 1;
		IF @N <= @Len SET @Char = SUBSTRING( @Scale, @N, 1 );
	END;

	SET @Y_Scale = CAST( @Scale AS float );
END;

IF @Y_Scale = 0 BEGIN --THEN no data
	SELECT 'No Data in Y column' AS ABORT
	RETURN;
END

IF @Graph_Type = 'COLUMN' BEGIN;
	DECLARE @Distinct_X TABLE ( 
		N int	NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED, 
		X float	NOT NULL 
	);
	
	INSERT	@Distinct_X ( X ) 
	SELECT	DISTINCT X 
	FROM	#Data 
	WHERE	X IS NOT NULL;
	
	SELECT	@Col_Half = MIN( b.X - a.X )
	FROM	@Distinct_X a
	JOIN	@Distinct_X b ON a.N = b.N - 1;

	IF @Debug > 0 SELECT * FROM @Distinct_X;
	
	SET @Col_Half = COALESCE( NULLIF( @Col_Half, 0 ), ( @Max_X - @Min_X + 1 ) / @X_Count ) * @Col_Width * 0.5;
END;

SET @Zero_Y = ( @Max_Y - @Min_Y ) * .0001;

IF @Debug > 0 SELECT 'Y scale:', @Y_Scale;

SELECT	@X0 = CASE @Graph_Type WHEN 'PIE' THEN -1 WHEN 'COLUMN' THEN @Min_X - @Col_Half ELSE @Min_X END,
		@X1 = CASE @Graph_Type WHEN 'PIE' THEN  1 WHEN 'COLUMN' THEN @Max_X + @Col_Half ELSE @Max_X END,
		@Y0 = CASE @Graph_Type WHEN 'PIE' THEN -1 ELSE @Min_Y / @Y_Scale END,
		@Y1 = CASE @Graph_Type WHEN 'PIE' THEN  1 ELSE @Max_Y / @Y_Scale END;

SET @Legend_Scale_X  = CASE WHEN @Graph_Type = 'BAR' AND ( @Max_Y - @Min_Y ) * .05 > .7 THEN .7 
							ELSE ( @Max_Y - @Min_Y ) * .05 
					   END /  IIF(@Y_Scale=0, 1,@Y_Scale);
SET @Legend_Scale_Y  = @Legend_Scale_X;
SET @Legend_Offset_X = CASE WHEN @Graph_Type = 'PIE' THEN @X1 * 1.05
							WHEN @Graph_Type = 'BAR' AND @Legend_Over = 1 THEN @X0 + ( @X1 - @X0 ) * .02
							ELSE @X1 + ( @X1 - @X0 ) * .05
					   END;
SET @Legend_Offset_Y = CASE @Graph_Type WHEN 'BAR' THEN -.5 * ( 1. / @Y_Scale + @Legend_Scale_Y )
										WHEN 'AREA' THEN @Y0 - 1.5 * @Legend_Scale_Y 
										ELSE @Y1 + @Legend_Scale_Y * .5 
					   END;

IF @Debug > 0 SELECT 'debug 2', [@Legend_Scale_X] = @Legend_Scale_X, [@Legend_Scale_Y] = @Legend_Scale_Y, [@Legend_Offset_X] = @Legend_Offset_X, [@Legend_Offset_Y] = @Legend_Offset_Y, [@X0] = @X0, [@X1] = @X1, [@Y0] = @Y0, [@Y1] = @Y1, [@Col_Half] = @Col_Half;

DECLARE Series_Cursor CURSOR
FOR SELECT	 ID, Legend, Pie_Segment - CASE WHEN Pie_Segment > 2 * PI() THEN 2 * PI() ELSE 0 END 
	FROM	 #Legend 
	ORDER BY ID;

OPEN Series_Cursor;

FETCH NEXT FROM Series_Cursor INTO @Series_#, @Series_Legend, @Pie_Segment;

CREATE TABLE #FinalGraph ( G varchar(max) not null, ID int identity(1,1) primary key clustered );

SET @Color = 0;

SET @Skip_Color_WKT = CASE WHEN @Graph_Type = 'LINE' THEN 'LINESTRING(' ELSE 'POLYGON((' END + 
					  CAST( @X0 AS varchar(30) ) + ' ' + CAST( @Y0 AS varchar(30) ) + ', ' + 
					  CAST( @X0 AS varchar(30) ) + ' ' + CAST( @Y0 + ( @Y1 - @Y0 ) * .0001 AS varchar(30) ) + ', ' + 
					  CAST( @X0 + ( @X1 - @X0 ) * .000025 AS varchar(30) ) + ' ' + CAST( @Y0 AS varchar(30) ) + ', ' + 
					  CAST( @X0 AS varchar(30) ) + ' ' + CAST( @Y0 AS varchar(30) ) + 
					  CASE WHEN @Graph_Type = 'LINE' THEN ')' ELSE '))' END;

WHILE @@FETCH_STATUS = 0 BEGIN;
	SET @Color = @Color + 1;

	WHILE CHARINDEX( ',' + CAST( @Color AS varchar(30) ) + ',', ',' + @Skip_Colors + ',' ) > 0 BEGIN;
		SET @Color = @Color + 1;
		INSERT #FinalGraph ( G ) VALUES( @Skip_Color_WKT );
	END;
	
	IF @Debug > 0 SELECT 'debug 3', [@Series_#] = @Series_#, [@Series_Legend] = @Series_Legend, [@Color] = @Color;

	SET @G = (
		SELECT	CASE @Graph_Type 
					 WHEN 'BAR' THEN
							',((' + 
							CAST( d.X AS varchar(30) ) + ' ' +
							CAST( ( l.ID - 0.9 ) / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X AS varchar(30) ) + ' ' +
							CAST( ( l.ID - 0.1 ) / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X + case when d.Y = 0 then @Zero_Y else d.Y end AS varchar(30) ) + ' ' + 
							CAST( ( l.ID - 0.1 ) / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X + case when d.Y = 0 then @Zero_Y else d.Y end AS varchar(30) ) + ' ' + 
							CAST( ( l.ID - 0.9 ) / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X AS varchar(30) ) + ' ' + 
							CAST( ( l.ID - 0.9 ) / @Y_Scale AS varchar(30) ) + '))'
					 WHEN 'COLUMN' THEN 
							',((' + 
							CAST( d.X + @Col_Half AS varchar(30) ) + ' ' +
							CAST( CASE WHEN COALESCE( d.Y0, 0 ) < @Y_Base_Use THEN @Y_Base_Use ELSE COALESCE( d.Y0, 0 ) END / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X - @Col_Half AS varchar(30) ) + ' ' +
							CAST( CASE WHEN COALESCE( d.Y0, 0 ) < @Y_Base_Use THEN @Y_Base_Use ELSE COALESCE( d.Y0, 0 ) END / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X - @Col_Half AS varchar(30) ) + ' ' +
							CAST( CASE WHEN COALESCE( d.Y0, 0 ) + d.Y <= @Y_Base_Use THEN @Y_Base_Use + @Zero_Y ELSE COALESCE( d.Y0, 0 ) + d.Y + @Zero_Y END / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X + @Col_Half AS varchar(30) ) + ' ' +
							CAST( CASE WHEN COALESCE( d.Y0, 0 ) + d.Y <= @Y_Base_Use THEN @Y_Base_Use + @Zero_Y ELSE COALESCE( d.Y0, 0 ) + d.Y + @Zero_Y END / @Y_Scale AS varchar(30) ) + ',' +
							CAST( d.X + @Col_Half AS varchar(30) ) + ' ' +
							CAST( CASE WHEN COALESCE( d.Y0, 0 ) < @Y_Base_Use THEN @Y_Base_Use ELSE COALESCE( d.Y0, 0 ) END / @Y_Scale AS varchar(30) ) + '))'
					 ELSE ',' + 
							CAST( d.X AS varchar(30) ) + ' ' + 
							CAST( CASE WHEN @Graph_Type = 'AREA' AND d.Y <= @Y_Base_Use THEN @Y_Base_Use + @Zero_Y ELSE d.Y END / @Y_Scale AS varchar(30) )
				END
		FROM	  #Data d
		LEFT JOIN #Legend l ON l.Legend = COALESCE( d.Legend, '' )
		WHERE	  l.ID = @Series_#
		ORDER BY  d.OrderBy
		FOR XML PATH('') );

	IF @Graph_Type IN ( 'COLUMN', 'BAR' ) 
		SET @G = STUFF( @G, 1, 1, 'MULTIPOLYGON(' ) + ')';
	ELSE IF @Graph_Type IN ( 'AREA', 'PIE', 'PAINT' )
		SET @G = 'MULTIPOLYGON(((' + STUFF( @G, 1, 1, '' ) + LEFT( @G, CHARINDEX( ',', @G, 2 ) - 1 ) + ')))';
	ELSE /*IF @Graph_Type = 'LINE'*/ 
		SET @G = STUFF( @G, 1, 1, 'MULTILINESTRING((' ) + '))';

	SET @Legend_Offset_Y = @Legend_Offset_Y + 
							CASE WHEN @Graph_Type = 'BAR' THEN 1. / @Y_Scale 
								 WHEN @Graph_Type = 'AREA' THEN 1.5 * @Legend_Scale_Y 
								 ELSE -1.5 * @Legend_Scale_Y
							END;

	IF @Graph_Type = 'PIE' AND @Legend_Over = 1 
		SELECT	@Legend_Offset_Y = 1.05 * SIN( @Pie_Segment ) - CASE WHEN @Pie_Segment > PI() THEN @Legend_Scale_Y ELSE 0 END,
				@Legend_Offset_X = 1.05 * COS( @Pie_Segment ) - CASE WHEN @Pie_Segment BETWEEN PI() * .5 and PI() * 1.5 THEN @Legend_Scale_X * .571 * ( 2 + LEN( @Series_Legend ) ) ELSE 0 END;
	
	IF @Debug > 0 SELECT 'debug 4', [@G] = @G, [@Legend_Offset_X] = @Legend_Offset_X, [@Legend_Offset_Y] = @Legend_Offset_Y, [@Pie_Segment] = @Pie_Segment, [@Legend] = @Legend, [Add Legend] = case when @G LIKE 'MULTIPOLYGON%' then 'yes' else 'no' end, [Legend WKT] = dbo.Graph_WKT_String( @Series_Legend, @Legend_Scale_X, @Legend_Offset_X, @Legend_Scale_Y, @Legend_Offset_Y );

	INSERT #FinalGraph ( G )
	VALUES( CASE WHEN @Legend = 0 /*OR @G NOT LIKE 'MULTIPOLYGON%'*/ THEN @G
				 ELSE STUFF( @G, case when @Graph_Type = 'LINE' then 17 else 15 end, 0, 
							 dbo.Graph_WKT_String( CASE WHEN @Graph_Type = 'BAR' THEN @Series_Legend
												  WHEN @Graph_Type = 'PIE' AND @Legend_Over = 1 AND @Pie_Segment BETWEEN PI() * .5 and PI() * 1.5 THEN @Series_Legend + ' ' + CHAR(8)
												  ELSE CHAR(8) + ' ' + @Series_Legend
											 END, 
											 @Legend_Scale_X, @Legend_Offset_X, @Legend_Scale_Y, @Legend_Offset_Y ) + ',' )
			END
		  );

	FETCH NEXT FROM Series_Cursor INTO @Series_#, @Series_Legend, @Pie_Segment;
END;

CLOSE Series_Cursor;
DEALLOCATE Series_Cursor;

SET @G = NULL;

-- Add Y grid axis values:
IF @Graph_Type IN ( 'LINE', 'AREA', 'COLUMN', 'PAINT' ) AND @Y_Grid = 1 BEGIN;
	SET @Step_Str = CAST( ( @Max_Y - @Min_Y ) / 8. AS varchar(30) );
	SET @N = 0;
	SET @First = 1;
	SET @Len = LEN( @Step_Str );

	IF @Debug > 0 SELECT 'debug 5', [@N] = @N, [@Len] = @Len, [@Step_Str] = @Step_Str;

	WHILE @N < @Len BEGIN;
		SET @N = @N + 1;
		SET @Char = SUBSTRING( @Step_Str, @N, 1 );
		IF @Char = 'e' BREAK;
		IF @Char LIKE '[1-9]' BEGIN;
			IF @FIRST = 1 BEGIN;
				SET @New_Char = SUBSTRING( '1255500000', CAST( @Char AS INT ) + 
								CASE WHEN SUBSTRING( @Step_Str + '0',  @N + 1, 1 ) LIKE '[1-9]' 
									   OR SUBSTRING( @Step_Str + '00', @N + 1, 2 ) LIKE '.[1-9]' 
									 THEN 1 ELSE 0 
								END, 1 );
				SET @Step_Str = STUFF( @Step_Str, @N, 1, REPLACE( @New_Char, '0', '1' ) );
				SET @First = 0;
			END;
			ELSE SET @Step_Str = STUFF( @Step_Str, @N, 1, '0' );
		END;
	END;

	SET @Step = CAST( @Step_Str AS FLOAT ) * CASE WHEN @New_Char = '0' THEN 10 ELSE 1 END;
	SET @Legend_Offset_X = @Min_X - CASE WHEN @Graph_Type = 'COLUMN' THEN @Col_Half ELSE 0 END;
	SET @Y = FLOOR( @Min_Y / @Step ) * @Step;

	WHILE @Y <= @Max_Y + @Step * .5 BEGIN;
		SET @Y_Legend = REPLACE( REPLACE( REPLACE( REPLACE( CAST( @Y AS varchar(30) ), 
						'E+0', 'E+' ), 'E+0', 'E+' ), 'E-0', 'E-' ), 'E-0', 'E-' ) + ' -';
		SET @G = COALESCE( @G + ',', 'MULTIPOLYGON((' ) + 
				 dbo.Graph_WKT_String( @Y_Legend, 
								 @Legend_Scale_Y, 
								 @Legend_Offset_X - LEN( @Y_Legend ) * @Legend_Scale_Y * .571, 
								 @Legend_Scale_Y, 
								 CAST( @Y / @Y_Scale AS float ) - @Legend_Scale_Y * .5 );
		SET @Y = @Y + @Step;
	END;
END;

-- Print Graph title:
SET @Title = RTRIM( LTRIM( @Title ) );

IF @Debug > 0 SELECT '@Title=', '"' + @Title + '"';

IF COALESCE( @Title, '' ) <> '' BEGIN;
	IF LEN( @Title ) > 55 BEGIN;
		SET @Break = CHARINDEX( ' ', @Title, LEN( @Title ) / 2 );

		IF @Break > 0 BEGIN;
			IF @Debug > 0 SELECT 'debug 6', [@Title] = @Title, [@Break] = @Break, [@text] = LEFT( @Title, @Break - 1 ), [@xs] = @Legend_Scale_Y * 1.5, [@xo] = ( @Min_X + @Max_X + CASE WHEN @Graph_Type = 'COLUMN' THEN 1 ELSE 0 END - @Legend_Scale_Y * .8565 * ( @Break - 1 ) ) * .5, [@ys] = @Legend_Scale_Y * 1.5, [@yo] = @Max_Y + ( @Max_Y - @Min_Y ) * .1 + @Legend_Scale_Y * 2.25;

			SET @G = COALESCE( @G + ',', 'MULTIPOLYGON((' ) + 
					 dbo.Graph_WKT_String( RTRIM( LEFT( @Title, @Break - 1 ) ), 
									 @Legend_Scale_Y * 1.5, 
									 ( @X0 + @X1 - @Legend_Scale_Y * .8565 * LEN( RTRIM( LEFT( @Title, @Break - 1 ) ) ) ) * .5, 
									 @Legend_Scale_Y * 1.5, 
									 ( @Y1 + ( @Y1 - @Y0 ) * .1 ) + @Legend_Scale_Y * 2.25 );

			SET @Title = LTRIM( STUFF( @Title, 1, @Break, '' ) );

			IF @Debug > 0 SELECT 'debug 7', [@Title] = @Title;
		END;
	END;

	IF @Debug > 0 SELECT 'debug 7', [@text] = @Title, [@xs] = @Legend_Scale_Y * 1.5, [@xo] = ( @X0 + @X1 - @Legend_Scale_Y * .8565 * LEN( @Title ) ) * .5, [@ys] = @Legend_Scale_Y * 1.5, [@yo] = ( @Y1 + ( @Y1 - @Y0 ) * .1 );
	
	SET @G = COALESCE( @G + ',', 'MULTIPOLYGON((' ) + 
			 dbo.Graph_WKT_String( @Title, 
							 @Legend_Scale_Y * 1.5, 
							 ( @X0 + @X1 - @Legend_Scale_Y * .8565 * LEN( @Title ) ) * .5, 
							 @Legend_Scale_Y * 1.5, 
							 ( @Y1 + ( @Y1 - @Y0 ) * .1 ) );
END;

IF @Debug > 0 SELECT 'debug 9', [@G] = @G;

IF @G IS NOT NULL BEGIN;
	SET @G = @G + '))';
	INSERT #FinalGraph ( G ) VALUES( @G );
	--IF @Show_WKT = 1 PRINT @G;
END;

IF @Show_WKT = 1 SELECT * FROM #FinalGraph;

SELECT geometry::STGeomFromText( G, 0 ) As Spatial_results_generated FROM #FinalGraph;
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

GRANT EXECUTE ON [dbo].[Graph_Render] TO [public];
GO
