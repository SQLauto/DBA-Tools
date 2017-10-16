IF EXISTS(SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID('dbo') AND name = 'Convert_XMLtoHTML' AND type IN ('FN', 'TF', 'IF'))
      DROP FUNCTION [dbo].[Convert_XMLtoHTML]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : XML to HTML text

Description :	
	This function will format a SQL XML result output, into a nice looking HTML table

	Note : Could add an auto replace for irregular chars in column names etc
Parameters :
	- @XMLInput : This needs to be a VARCHAR(max) result of a SELECT [...] FOR XML RAW, ELEMENTS XSINIL
Return :
	- The returned HTML is a VARCHAR(max) with style integrated (could be a parameter later?) and a regular <table> with dynamic headers

History :
	2016-02-10 - XMO - Moved function from Betclick_Stats to DBA. Name changed from XMLtoHTML
	2015-09-28 - XMO - Create function for mail report purposes
*/
CREATE FUNCTION [dbo].[Convert_XMLtoHTML]
(
	 @XMLInput VARCHAR(MAX)
)
RETURNS VARCHAR(MAX)
AS BEGIN
	IF @XMLInput != ''	BEGIN
	
		DECLARE @HTML VARCHAR(MAX);
		SET @HTML =N'
		<style type="text/css">
			table { 
			color: #333;
			font-family: Helvetica, Arial, sans-serif;
			font-size: 9pt;
			border-collapse:collapse; border-spacing: 0; 
			}
			td,th{border: 1px solid #CCC;padding: 4px 8px 4px 8px;transition: all 0.3s;}
			th{background: #DFDFDF; font-weight: bold;}
			td{background: #FEFEFE;text-align: center;}
			tr:nth-child(even) td { background: #F1F1F1; }
			tr:nth-child(odd) td { background: #FEFEFE; }
			tr td:hover { background: #777; color: #FFF; }
		</style>
			'
		SET @HTML += '<br><table><tr>'
	
		SET @XMLInput = REPLACE (@XMLInput, '<row xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">', '<row>') 

		DECLARE @TableHeads VARCHAR(1000) = SUBSTRING(@XMLInput, 6, CHARINDEX( '</row>' , @XMLInput ) -6 ) --Keep First row without the <row/>
	
		-- Init Headers
		WHILE @TableHeads != '' BEGIN
			IF CHARINDEX ('xsi:nil="true"', LEFT(@TableHeads, CHARINDEX ('>', @TableHeads))) !=0 BEGIN--NULL value COLUMN
				SET @HTML+='<th>'+SUBSTRING(@TableHeads, 2, CHARINDEX (' xsi:nil="true"', @TableHeads) -2)+'</th>'
				SET @TableHeads = RIGHT(@TableHeads, LEN(@TableHeads) - CHARINDEX ('>', @TableHeads))
			END
			ELSE BEGIN
				SET @HTML+='<th>'+SUBSTRING(@TableHeads, 2, CHARINDEX ('>', @TableHeads) -2)+'</th>'

				SET @TableHeads = RIGHT(@TableHeads, LEN(@TableHeads) - CHARINDEX ('>', @TableHeads))
				SET @TableHeads = RIGHT(@TableHeads, LEN(@TableHeads) - CHARINDEX ('>', @TableHeads))
			END
		END
		SET @HTML +='</tr>
		'

		-- Parse results
		WHILE @XMLInput != '' BEGIN -- Each Row
			SET @HTML +='<tr>'
			SET @XMLInput =  RIGHT(@XMLInput, LEN(@XMLInput) - 5) -- Remove <row>
		
			WHILE LEFT(@XMLInput, 6) != '</row>' BEGIN -- Each Column
				IF CHARINDEX ('xsi:nil="true"', LEFT(@XMLInput, CHARINDEX ('>', @XMLInput))) !=0 BEGIN--NULL value COLUMN
					SET @HTML+='<td></td>'
					SET @XMLInput = RIGHT(@XMLInput, LEN(@XMLInput) - CHARINDEX ('>', @XMLInput) )
				END
				ELSE BEGIN
					SET @HTML+='<td>'+SUBSTRING(@XMLInput,  CHARINDEX ('>', @XMLInput)+1, CHARINDEX ('</', @XMLInput)-CHARINDEX ('>', @XMLInput)-1)+'</td>'
					SET @XMLInput = RIGHT(@XMLInput,  LEN(@XMLInput) - CHARINDEX ('>', @XMLInput))
					SET @XMLInput = RIGHT(@XMLInput,  LEN(@XMLInput) - CHARINDEX ('>', @XMLInput))
				END

			END
		
			SET @HTML +='</tr>
			'
			SET @XMLInput =  RIGHT(@XMLInput, LEN(@XMLInput) - 6) -- Remove </row>
		END
		SET @HTML +='</table><br>'
		SET @XMLInput = @HTML

	END


	RETURN @HTML
END
GO

