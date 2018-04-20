IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'PowerBi_XE_StatsEntity')
      DROP PROCEDURE [dbo].[PowerBi_XE_StatsEntity]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Returns a restricted Select of XEvents stats, used by PowerBi to display stats

Description:
Only returns latest @NbDays days data.
Only keep procs which are in the TOP 10 of either:
	-Duration
	-Cpu
	-Logical Reads
	-Writes
(in the last 30 minutes stats)

@Returned_data = 'Stats' param :
	Will return the details of the stats5min table, with a query_hash_int as identifier
@Returned_data = 'Details' param :
	Links the previous  query_hash_int as identifier to the details of the Entity call

Policicy override:
GRANT EXECUTE ON [dbo].[PowerBi_XE_StatsEntity] TO [nobody];

History:
	2018-04-10 - XMO - Adapt to new table with multiplier
	2017-06-29 - XMO - Add param precisionMin
	2017-06-19 - XMO - Add params and select larger date range for TopCandidates
	2017-04-11 - XMO - Go Back to UTC timestamp
	2017-04-10 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[PowerBi_XE_StatsEntity]
(
	@Returned_data VARCHAR(50) = 'Stats' --Can be 'Stats' or 'Details'
	,@Database_name SYSNAME = 'Casino'
	,@NbDays TINYINT = 7 --> NbDays up to which to retrieve. However there is a 'hard' limit in the table 
	,@TopRanked TINYINT = 10 --> Only procs/queries in the TopRanked by duration/cpu/reads etc.. will be retrieved
	,@PrecisionMin SMALLINT = 5 --Select precision for the XEtable in which to get the data
)
AS
BEGIN TRY

	IF( OBJECT_ID ('XEStats30min_Entity') IS NOT NULL
		AND  OBJECT_ID ('XEStats5min_Entity') IS NOT NULL
		)
	BEGIN
		IF (@Returned_data ='Stats')
		BEGIN
			IF (@PrecisionMin =5)
			BEGIN
				;WITH top_procs AS (
				--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
				SELECT query_hash
					, database_name
				FROM (
					--Sub-Select returning procs ranks over several categories (last 30 minutes only)
					SELECT query_hash
						,database_name
						,RANK() OVER(ORDER BY SUM(duration) DESC)			AS rank_duration
						,RANK() OVER(ORDER BY AVG(duration_avg) DESC)		AS rank_duration_avg
						,RANK() OVER(ORDER BY SUM(cpu_time) DESC)			AS rank_cpu_time
						,RANK() OVER(ORDER BY SUM(logical_reads) DESC)		AS rank_logical_reads
						,RANK() OVER(ORDER BY SUM(writes) DESC)				AS rank_writes
						,RANK() OVER(ORDER BY SUM(count*multiplier) DESC)	AS rank_count
					FROM XEStats30min_Entity as latest WITH (NOLOCK)
					WHERE latest.timestamp > DATEADD(hour, -@NbDays*24/25, GETUTCDATE()) --Top Procs in the latest 25% of the time range
					AND latest.database_name = @Database_name
					GROUP BY query_hash, database_name
				) AS detailed_ranks
				UNPIVOT(Rank_number FOR Rank_type IN(
					 rank_duration
					,rank_cpu_time
					,rank_logical_reads
					,rank_writes
					,rank_count
					 ) 
				) AS all_ranks
				GROUP BY query_hash, database_name
				HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top @TopRanked of at least one category
				 )

				 SELECT 
				 timestamp
				 ,cast (XE.query_hash as bigint) as query_hash_int
				 ,XE.count*multiplier AS count 
				 ,XE.duration AS duration
				 ,XE.duration_avg
				 ,XE.cpu_time AS cpu_time
				 ,XE.cpu_time_avg
				 ,XE.logical_reads_avg
				 ,XE.writes AS writes
				 ,XE.writes_avg
				 ,XE.row_count_avg
				 ,XE.logical_reads AS logical_reads
				 FROM XEStats5min_Entity as XE WITH (NOLOCK)
				 JOIN top_procs as tp ON XE.query_hash = tp.query_hash AND XE.database_name = tp.database_name
				 WHERE XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
			END
			IF (@PrecisionMin =30)
			BEGIN
				;WITH top_procs AS (
				--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
				SELECT query_hash
					, database_name
				FROM (
					--Sub-Select returning procs ranks over several categories (last 30 minutes only)
					SELECT query_hash
						,database_name
						,RANK() OVER(ORDER BY SUM(duration) DESC)			AS rank_duration
						,RANK() OVER(ORDER BY SUM(cpu_time) DESC)			AS rank_cpu_time
						,RANK() OVER(ORDER BY SUM(logical_reads) DESC)		AS rank_logical_reads
						,RANK() OVER(ORDER BY SUM(writes) DESC)				AS rank_writes
						,RANK() OVER(ORDER BY SUM(count*multiplier) DESC)	AS rank_count
					FROM XEStats30min_Entity as latest WITH (NOLOCK)
					WHERE latest.timestamp > DATEADD(hour, -@NbDays*24/25, GETUTCDATE()) --Top Procs in the latest 25% of the time range
					AND latest.database_name = @Database_name
					GROUP BY query_hash, database_name
				) AS detailed_ranks
				UNPIVOT(Rank_number FOR Rank_type IN(
					 rank_duration
					,rank_cpu_time
					,rank_logical_reads
					,rank_writes
					,rank_count
					 ) 
				) AS all_ranks
				GROUP BY query_hash, database_name
				HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top @TopRanked of at least one category
				 )

				 SELECT 
				 timestamp
				 ,cast (XE.query_hash as bigint) as query_hash_int
				 ,XE.count*multiplier AS count 
				 ,XE.duration AS duration
				 ,XE.duration_avg
				 ,XE.cpu_time AS cpu_time
				 ,XE.cpu_time_avg
				 ,XE.logical_reads_avg
				 ,XE.writes AS writes
				 ,XE.writes_avg
				 ,XE.row_count_avg
				 ,XE.logical_reads AS logical_reads
				 FROM XEStats30min_Entity as XE WITH (NOLOCK)
				 JOIN top_procs as tp ON XE.query_hash = tp.query_hash AND XE.database_name = tp.database_name
				 WHERE XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
			END
		END

		
			
		IF (@Returned_data ='Details')
		BEGIN
			 ;WITH top_procs AS (
				--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
				SELECT query_hash
					, database_name
				FROM (
					--Sub-Select returning procs ranks over several categories (last 30 minutes only)
					SELECT query_hash
						,database_name
						,RANK() OVER(ORDER BY SUM(duration) DESC)			AS rank_duration
						,RANK() OVER(ORDER BY SUM(cpu_time) DESC)			AS rank_cpu_time
						,RANK() OVER(ORDER BY SUM(logical_reads) DESC)		AS rank_logical_reads
						,RANK() OVER(ORDER BY SUM(writes) DESC)				AS rank_writes
						,RANK() OVER(ORDER BY SUM(count*multiplier) DESC)	AS rank_count
					FROM XEStats30min_Entity as latest WITH (NOLOCK)
					WHERE latest.timestamp > DATEADD(hour, -@NbDays*24/25, GETUTCDATE()) --Top Procs in the latest 25% of the time range
					AND latest.database_name = @Database_name
					GROUP BY query_hash, database_name
				) AS detailed_ranks
				UNPIVOT(Rank_number FOR Rank_type IN(
					 rank_duration
					,rank_cpu_time
					,rank_logical_reads
					,rank_writes
					,rank_count
					 ) 
				) AS all_ranks
				GROUP BY query_hash, database_name
				HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top @TopRanked of at least one category
			 )
			 SELECT 
				 cast (XE.query_hash as bigint) as query_hash_int
				 ,XE.database_name
				 ,XE.object_name
				 ,XE.query_full
			 FROM XEStats30min_Entity as XE WITH (NOLOCK)
			 JOIN top_procs as tp ON XE.query_hash = tp.query_hash AND XE.database_name = tp.database_name
			 WHERE XE.timestamp = (SELECT TOP 1 timestamp FROM XEStats30min_Entity as latest WITH (NOLOCK) ORDER BY TIMESTAMP DESC)
		 END
	END
	ELSE 
		SELECT 'Expected XEStats_Entity Tables Not found.' AS Error

END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
	THROW;
END CATCH
GO

