IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'PowerBi_XE_StatsRpcs')
      DROP PROCEDURE [dbo].[PowerBi_XE_StatsRpcs]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Returns a restricted Select of XEvents stats, used by PowerBi to display stats

Description:
Only returns latest 7 days procs.
Only keep procs which are in the TOP 12 of either:
	-Duration
	-Cpu
	-Logical Reads
	-Writes
(in the last 30 minutes stats)

Policicy override:
GRANT EXECUTE ON [dbo].[PowerBi_XE_StatsRpcs] TO [nobody];

History:
	2018-03-15 - XMO - Adapt to new Rpcs tables
	2017-08-21 - XMO - Fix Rank calculation
	2017-04-11 - XMO - Go Back to UTC timestamp
	2017-04-10 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[PowerBi_XE_StatsRpcs]
(
	@Database_name SYSNAME = 'Betclick'
	,@NbDays TINYINT = 7 --> NbDays up to which to retrieve. However there is a 'hard' limit in the table 
	,@TopRanked TINYINT = 12 --> Only procs/queries in the TopRanked by duration/cpu/reads etc.. will be retrieved
	,@PrecisionMin SMALLINT = 5 --Select precision for the XEtable in which to get the data
)
AS
BEGIN TRY

	IF( OBJECT_ID ('XEStats30min_Rpcs') IS NOT NULL
		AND  OBJECT_ID ('XEStats5min_Rpcs') IS NOT NULL
		)
	BEGIN
		IF @PrecisionMin=5
		BEGIN
		;WITH top_procs AS (
			--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
			SELECT object_name
				, database_name
			FROM (
				--Sub-Select returning procs ranks over several categories (last 30 minutes only)
				SELECT object_name
					,database_name
					,RANK() OVER(ORDER BY SUM(duration) DESC)			AS rank_duration
					,RANK() OVER(ORDER BY SUM(cpu_time) DESC)			AS rank_cpu_time
					,RANK() OVER(ORDER BY SUM(logical_reads) DESC)		AS rank_logical_reads
					,RANK() OVER(ORDER BY SUM(writes) DESC)				AS rank_writes
					,RANK() OVER(ORDER BY SUM(count*multiplier) DESC)	AS rank_count
				FROM XEStats30min_Rpcs as latest WITH (NOLOCK)
				WHERE latest.timestamp > DATEADD(hour, -@NbDays*24/25, GETUTCDATE()) --Top Procs in the latest 25% of the time range
				AND latest.database_name = @Database_name
				GROUP BY  object_name, database_name
			) AS detailed_ranks
			UNPIVOT(Rank_number FOR Rank_type IN(
				 rank_duration
				,rank_cpu_time
				,rank_logical_reads
				,rank_writes
				,rank_count
				 ) 
			) AS all_ranks
			GROUP BY object_name, database_name
			HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top 12 of at least one category
		 )

		 SELECT 
		 timestamp
		 ,XE.object_name
		 ,XE.count*multiplier AS count 
		 ,XE.duration AS duration
		 ,XE.duration_avg
		 ,XE.cpu_time AS cpu_time
		 ,XE.cpu_time_avg
		 ,XE.logical_reads_avg
		 ,XE.writes AS writes
		 ,XE.writes_avg
		 ,XE.logical_reads AS logical_reads
		 FROM XEStats5min_Rpcs as XE WITH (NOLOCK)
		 JOIN top_procs as tp ON XE.object_name = tp.object_name AND XE.database_name = tp.database_name
		 WHERE XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
		 END
		 ELSE IF @PrecisionMin = 30
		 BEGIN
		;WITH top_procs AS (
			--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
			SELECT object_name
				, database_name
			FROM (
				--Sub-Select returning procs ranks over several categories (last 30 minutes only)
				SELECT object_name
					,database_name
					,RANK() OVER(ORDER BY duration DESC)			AS rank_duration
					,RANK() OVER(ORDER BY cpu_time DESC)			AS rank_cpu_time
					,RANK() OVER(ORDER BY logical_reads DESC)		AS rank_logical_reads
					,RANK() OVER(ORDER BY writes DESC)				AS rank_writes
					,RANK() OVER(ORDER BY count*multiplier DESC)	AS rank_count
				FROM XEStats30min_Rpcs as latest WITH (NOLOCK)
				WHERE latest.timestamp > DATEADD(hour, -@NbDays*24/25, GETUTCDATE()) --Top Procs in the latest 25% of the time range
				AND latest.database_name = @Database_name
			) AS detailed_ranks
			UNPIVOT(Rank_number FOR Rank_type IN(
				 rank_duration
				,rank_cpu_time
				,rank_logical_reads
				,rank_writes
				,rank_count
				 ) 
			) AS all_ranks
			GROUP BY object_name, database_name
			HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top 12 of at least one category
		 )

		 SELECT 
		 timestamp
		 ,XE.object_name
		 ,XE.count*XE.multiplier AS count 
		 ,XE.duration AS duration
		 ,XE.duration_avg
		 ,XE.cpu_time AS cpu_time
		 ,XE.cpu_time_avg
		 ,XE.logical_reads_avg
		 ,XE.writes AS writes
		 ,XE.writes_avg
		 ,XE.logical_reads AS logical_reads
		 FROM XEStats30min_Rpcs as XE WITH (NOLOCK)
		 JOIN top_procs as tp ON XE.object_name = tp.object_name AND XE.database_name = tp.database_name
		 WHERE XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
		 END

	END
	ELSE 
		SELECT 'Expected XEStats_Rpcs Tables Not found.' AS Error

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

