IF EXISTS(SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo') AND name = 'PowerBi_XE_Timeouts')
      DROP PROCEDURE [dbo].[PowerBi_XE_Timeouts]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Title : Returns a restricted Select of XEvents stats, used by PowerBi to display stats


@Returned_data = 'Stats' param :
	Will return the details of the stats5min table, with a query_hash_int as identifier
@Returned_data = 'Details' param :
	Links the previous  query_hash_int as identifier to the details of the Entity call

Policicy override:
GRANT EXECUTE ON [dbo].[PowerBi_XE_Timeouts] TO [nobody];

History:
	2017-07-19 - XMO - Fix hour>day error
	2017-06-29 - XMO - Add PrecisionMin and other params
	2017-04-10 - XMO - Creation
*/
CREATE PROCEDURE [dbo].[PowerBi_XE_Timeouts]
(
	@Returned_data VARCHAR(50) = 'Stats' --Can be 'Stats' or 'Details'
	,@Database_name SYSNAME = '%'
	,@NbDays TINYINT = 7 --> NbDays up to which to retrieve. However there is a 'hard' limit in the table 
	,@TopRanked TINYINT = 20 --> Only procs/queries in the TopRanked by duration/cpu/reads etc.. will be retrieved
	,@PrecisionMin SMALLINT = 5 --Select precision for the XEtable in which to get the data
		
)
AS
BEGIN TRY
	IF( OBJECT_ID ('XEStats5min_Timeouts') IS NOT NULL )
	BEGIN
		IF (@Returned_data ='Stats')
		BEGIN
			IF (@PrecisionMin =5)
			BEGIN
				;WITH top_procs AS (
					--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
					SELECT query_hash
						, database_name
						, result_max
					FROM (
						--Sub-Select returning procs ranks over several categories (last 30 minutes only)
						SELECT query_hash
							, database_name
							, result_max
							, RANK() OVER(PARTITION BY result_max ORDER BY SUM(count) DESC)				AS rank_count
						FROM XEStats30min_Timeouts as latest WITH (NOLOCK)
						WHERE latest.timestamp > DATEADD(day, -@NbDays, GETUTCDATE()) --Top Procs in the time range
						AND result_max IS NOT NULL
						AND query_short != ''
						GROUP BY query_hash
							,database_name
							,result_max
					) AS detailed_ranks
					UNPIVOT(Rank_number FOR Rank_type IN(
						rank_count
						 ) 
					) AS all_ranks
					GROUP BY query_hash, database_name,result_max
					HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top @TopRanked of at least one category
				 )

				 SELECT 
				 timestamp
				 ,cast (XE.query_hash as bigint) as query_hash_int
				 ,XE.count AS count 
				 ,XE.result_max AS result_max
				 ,XE.duration AS duration
				 ,XE.cpu_time AS cpu_time
				 ,XE.writes AS writes
				 ,XE.logical_reads AS logical_reads
				 FROM XEStats5min_Timeouts as XE WITH (NOLOCK)
				 JOIN top_procs as tp ON XE.query_hash = tp.query_hash AND XE.database_name = tp.database_name
				 WHERE XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
			END
			IF (@PrecisionMin =30)
			BEGIN
				;WITH top_procs AS (
					--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
					SELECT query_hash
						, database_name
						, result_max
					FROM (
						--Sub-Select returning procs ranks over several categories (last 30 minutes only)
						SELECT query_hash
							, database_name
							, result_max
							, RANK() OVER(PARTITION BY result_max ORDER BY SUM(count) DESC)				AS rank_count
						FROM XEStats30min_Timeouts as latest WITH (NOLOCK)
						WHERE latest.timestamp > DATEADD(day, -@NbDays, GETUTCDATE()) --Top Procs in the time range
						AND result_max IS NOT NULL
						AND query_short != ''
						GROUP BY query_hash
							,database_name
							,result_max
					) AS detailed_ranks
					UNPIVOT(Rank_number FOR Rank_type IN(
						rank_count
						 ) 
					) AS all_ranks
					GROUP BY query_hash, database_name,result_max
					HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top @TopRanked of at least one category
				 )

				 SELECT 
				 timestamp
				 ,cast (XE.query_hash as bigint) as query_hash_int
				 ,XE.count AS count 
				 ,XE.result_max AS result_max
				 ,XE.duration AS duration
				 ,XE.cpu_time AS cpu_time
				 ,XE.writes AS writes
				 ,XE.logical_reads AS logical_reads
				 FROM XEStats30min_Timeouts as XE WITH (NOLOCK)
				 JOIN top_procs as tp ON XE.query_hash = tp.query_hash AND XE.database_name = tp.database_name
				 WHERE XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
			END
		END

		
			
		IF (@Returned_data ='Details')
		BEGIN
			 ;WITH top_procs AS (
				--Only Return Top procs (of several categories like duration, cpu_time, logical_reads)
				SELECT query_hash
				FROM (
					--Sub-Select returning procs ranks over several categories (last 30 minutes only)
					SELECT query_hash
						, database_name
						, result_max
						, RANK() OVER(PARTITION BY result_max ORDER BY SUM(count) DESC)				AS rank_count
					FROM XEStats30min_Timeouts as latest WITH (NOLOCK)
					WHERE latest.timestamp > DATEADD(day, -@NbDays, GETUTCDATE()) --Top Procs in the time range
					AND result_max IS NOT NULL
					GROUP BY query_hash
						,database_name
						,result_max
				) AS detailed_ranks
				UNPIVOT(Rank_number FOR Rank_type IN(
					rank_count
					 ) 
				) AS all_ranks
				GROUP BY query_hash
				HAVING Min(all_ranks.rank_number) <=@TopRanked -- Only keep procs which are in the top @TopRanked of at least one category
			 )
			 SELECT 
				 cast (XE.query_hash as bigint) as query_hash_int
				 ,XE.database_name
				 ,XE.query_short
				 ,XE.query_full
			 FROM top_procs as tp  
			 CROSS APPLY (SELECT TOP 1 
				  XE.query_hash
				 ,XE.database_name
				 ,XE.query_short
				 ,XE.query_full
			 FROM XEStats30min_Timeouts as XE WITH (NOLOCK)
			 WHERE XE.query_hash = tp.query_hash
			 AND XE.timestamp > DATEADD(day, -@NbDays, GETUTCDATE())
			 ) as XE 
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

