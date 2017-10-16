IF EXISTS(SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID('dbo') AND name = 'Graph_WKT_String' AND type IN ('FN', 'TF', 'IF'))
      DROP FUNCTION [dbo].[Graph_WKT_String]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Return a WKT string to produce an image of a given text string.

Description: This is an utilitary function for graph creation usage (Geometry graph created into SSMS via Spacial Results)

Parameters :
@text varchar(100), -- a string of ASCII characters to convert to WKT
@xs   float,        -- horizontal scale (defines the width of a single character on the grid)
@xo   float,        -- horizontal origin (X coordinate of the lower left corner of the string on the grid)
@ys   float,        -- vertial scale (defines the height of a single character on the grid)
@yo   float         -- vertial origin (Y coordinate of the lower left corner of the string on the grid)

Return :
		Return a WKT string to produce an image of a given text string.
		This uses dbo.Graph_WKT_Letter function for each letter
History :
	2016-01-11 - XMO - Creation from Copy Paste of internet function WKT_String
*/

CREATE FUNCTION [dbo].[Graph_WKT_String](
	@text varchar(100)
	,@xs   float      
	,@xo   float      
	,@ys   float      
	,@yo   float        
)
RETURNS VARCHAR(max)
AS
BEGIN
	DECLARE @n int, @len int, @g varchar(max), @l varchar(max);
	SET @len = LEN( @text );
	SET @n = 0;
	WHILE @n < @len BEGIN;
		SET @n = @n + 1;
		SET @l = dbo.Graph_WKT_Letter( SUBSTRING( @text, @n, 1 ), @xs, @xo, @ys, @yo );
		IF @l IS NOT NULL SET @g = COALESCE( @g + ',', '' ) + @l;
		SET @xo = @xo + .571 * @xs;
	END;
	RETURN( @g );
END
GO

GRANT EXECUTE ON [dbo].[Graph_WKT_String] TO [public];
GO