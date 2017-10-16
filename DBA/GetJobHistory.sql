IF EXISTS(SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID('dbo') AND name = 'GetJobHistory' AND type IN ('FN', 'TF', 'IF'))
      DROP FUNCTION [dbo].[GetJobHistory]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*

Description: Returns an easy view table of the last Steps Executions details for a given Job Name, from latest exec to earliest, up to @MinDate

(First occurence of each jobs only displays the full step run (step_id 0) )

--Test :
SELECT * FROM DBA..GetJobHistory ('%',DATEADD(mi, -10, GETDATE()))

History:
	2017-06-13 - XMO - Fix run_duration to seconds and not displayed first occurence
	2017-04-06 - XMO - Fix @MinDate filtering
	2017-03-21 - XMO - Creation
*/

CREATE FUNCTION [dbo].[GetJobHistory]
(	
	@job_name sysname = '' --> Can match several names as a LIKE
	,@MinDate DATETIME2(0) = {d'1900-01-01'} -- MinDate up to which the History will be retrieved.
)
RETURNS @j_history TABLE (
	RunOcc		INT NOT NULL -- The Occurence of the last job execution (DESC), RunOcc = 1 means all the steps in the last job exec 
	,job_name	sysname	NOT NULL
	,step_name	VARCHAR(200) NOT NULL
	,Date_Start	datetime2(0)	NOT NULL
	,Date_End	 AS DATEADD(second, run_duration, Date_Start)
	,step_id	int	NOT NULL -- Id of the step (Step 0 is full job run)
	,message	varchar(4000)	NULL -- Error/ Warning / Print message returned
	,run_status	int	NOT NULL -- 1 = OK , 0 = KO
	,run_duration	int	NOT NULL -- In seconds
)
AS
BEGIN
	IF @MinDate IS NULL 
		SELECT @MinDate = {d'1900-01-01'}
	
	DECLARE @job_last_execs TABLE(
		job_id			uniqueidentifier NOT NULL
		,instance_id	int NOT NULL
		,prev_instance_id INT NULL
		PRIMARY KEY (job_id, instance_id)
	)
	
	--Get all the steps 0 (job outcome) for jobs matching the name given, and within the @MinDate up to now
	INSERT INTO @job_last_execs(
		job_id
		,instance_id
		,prev_instance_id
	)
	SELECT 
	  j.job_id
	 , h.instance_id
	 , ISNULL(MIN(h.instance_id) OVER (
		PARTITION BY j.job_id
		ORDER BY h.instance_id
		ROWS BETWEEN 1 PRECEDING  AND 1 PRECEDING
		), 0) AS prev_instance_id
	FROM msdb.dbo.sysjobhistory h WITH(NOLOCK)
	JOIN msdb.dbo.sysjobs AS j WITH(NOLOCK) ON h.job_id = j.job_id
	WHERE j.name LIKE @job_name
	AND h.step_id = 0
	AND( h.run_date > CAST(CONVERT(CHAR(8), @MinDate, 112) AS INT) --The run day is bigger than the @MinDate day
		OR( h.run_date =  CAST(CONVERT(CHAR(8), @MinDate, 112) AS INT) --Or it's the same day but the time value is bigger
			AND  h.run_time >= CAST(REPLACE(CONVERT(CHAR(8), @MinDate , 108), ':', '') AS INT))
		)


	DECLARE @Min_instance_id INT = (SELECT MIN(instance_id) FROM @job_last_execs)

	--From previous extract, get step details and save them in returned table  @j_history
	IF @Min_instance_id IS NOT NULL
	INSERT INTO @j_history(
		RunOcc
		,job_name
		,step_name
		,Date_Start
		,step_id
		,message
		,run_status
		,run_duration
	)
	SELECT
		DENSE_RANK() OVER(PARTITION BY j.job_id ORDER BY j.instance_id DESC) AS RunOcc
		,j_name.name AS job_name	
		,j_steps.step_name
		,cast (msdb.dbo.agent_datetime(j_steps.run_date,j_steps.run_time) as datetime2(0)) AS Date_Start			
		,j_steps.step_id		
		, CASE WHEN j_steps.message LIKE 'Executed as user: %\SQLAgent. The step succeeded.' THEN ''
			WHEN j_steps.message LIKE 'The Job succeeded.%' THEN ''
			WHEN j_steps.message LIKE 'Executed as user: %\SQLAgent. Warning: Null value is eliminated%' THEN ''
			ELSE REPLACE(j_steps.message, 'Executed as user: ', '')
			END AS error_message	
		,j_steps.run_status		
		,CASE WHEN j_steps.run_duration <= 60 THEN j_steps.run_duration
		ELSE (j_steps.run_duration/10000 * 60 * 60) + -- hours as seconds
			(j_steps.run_duration/100%100 * 60) + --minutes as seconds
			(j_steps.run_duration%100 )  --seconds
		END AS run_duration --Seconds

	FROM @job_last_execs AS j
	JOIN msdb.dbo.sysjobs AS j_name WITH(NOLOCK) ON j.job_id = j_name.job_id
	JOIN  msdb.dbo.sysjobhistory AS j_steps WITH(NOLOCK)
		ON j_steps.instance_id > @Min_instance_id-1
		AND j_steps.instance_id > j.prev_instance_id 
		AND	j_steps.instance_id <= j.instance_id
		AND j_steps.job_id = j.job_id
	
	ORDER BY j_steps.instance_id  DESC

	RETURN
END
GO
