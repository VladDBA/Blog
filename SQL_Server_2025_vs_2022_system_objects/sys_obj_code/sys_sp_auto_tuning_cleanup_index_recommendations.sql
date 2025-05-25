use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_auto_tuning_cleanup_index_recommendations
(
	@cutoff_timestamp_utc datetime,
	@affected_recommendations int OUTPUT
)
AS
BEGIN
	exec sys.sp_auto_tuning_validate_executable;

	BEGIN TRAN tran_cleanup_index_recs;
	BEGIN TRY

	IF @cutoff_timestamp_utc IS NULL
		RAISERROR(15725, -1, 13, N'cutoff_timestamp_utc');

	-- Check if the cutoff timestamp is at least 7 days in the past
	-- This is to ensure that we do not delete recommendations that are still recent.
	-- This is only a check for golden bits to allow for testing in non-golden bits.
	IF SERVERPROPERTY('IsNonGolden') IS NULL AND DATEDIFF(DAY, @cutoff_timestamp_utc, GETUTCDATE()) < 7
		RAISERROR(15726, -1, 6, N'The cutoff timestamp must be at least 7 days in the past.');

	SET @affected_recommendations = 0;
	DECLARE @return_status int;
	DECLARE @sql_string nvarchar(max);
	SET @sql_string = N'
	DECLARE @affected_count int = 0;
	-- Terminated States:
	-- Success(4), Reverted(7), Ignored(8), Error(9), Expired(10)

	-- First, delete from related tables to maintain referential integrity
	DELETE q
	FROM sys.ats_index_recommendation_impact_queries q
	INNER JOIN sys.ats_recommendations r ON q.recommendation_id = r.id
	WHERE r.last_refresh_timestamp_utc < @cutoff_timestamp_utc_in
	  AND r.archived = 1
	  AND r.state IN (4, 7, 8, 9, 10);

	DELETE v
	FROM sys.ats_recommended_impact_values v
	INNER JOIN sys.ats_recommendations r ON v.recommendation_id = r.id
	WHERE r.last_refresh_timestamp_utc < @cutoff_timestamp_utc_in
	  AND r.archived = 1
	  AND r.state IN (4, 7, 8, 9, 10);

	-- Delete associated workflows
	DELETE wf
	FROM sys.ats_workflow_fsm wf
	INNER JOIN sys.ats_workflow_recommendation_relation wr ON wf.execution_id = wr.execution_id
	INNER JOIN sys.ats_recommendations r ON wr.recommendation_id = r.id
	WHERE r.last_refresh_timestamp_utc < @cutoff_timestamp_utc_in
	  AND r.archived = 1
	  AND r.state IN (4, 7, 8, 9, 10);

	DELETE wr
	FROM sys.ats_workflow_recommendation_relation wr
	INNER JOIN sys.ats_recommendations r ON wr.recommendation_id = r.id
	WHERE r.last_refresh_timestamp_utc < @cutoff_timestamp_utc_in
	  AND r.archived = 1
	  AND r.state IN (4, 7, 8, 9, 10);

	-- Delete from the index recommendations table
	DELETE ir
	FROM sys.ats_index_recommendations ir
	INNER JOIN sys.ats_recommendations r ON ir.id = r.id
	WHERE r.last_refresh_timestamp_utc < @cutoff_timestamp_utc_in
	  AND r.archived = 1
	  AND r.state IN (4, 7, 8, 9, 10);

	-- Finally, delete from the recommendations table
	DELETE FROM sys.ats_recommendations
	WHERE last_refresh_timestamp_utc < @cutoff_timestamp_utc_in
	  AND archived = 1
	  AND state IN (4, 7, 8, 9, 10);

	SET @affected_count = @@ROWCOUNT;
	SET @affected_recommendations_out = @affected_count;
	';

	DECLARE @params_def nvarchar(4000);
	SET @params_def = N'
	@cutoff_timestamp_utc_in datetime,
	@affected_recommendations_out int OUTPUT
	';

	EXEC @return_status = sp_executesql @sql_string,
		@params_def,
		@cutoff_timestamp_utc_in = @cutoff_timestamp_utc,
		@affected_recommendations_out = @affected_recommendations OUTPUT;

	IF @return_status != 0
	BEGIN
		ROLLBACK TRAN tran_cleanup_index_recs;
		RAISERROR(15723, -1, 9, @return_status);
	END
	ELSE
		COMMIT TRAN tran_cleanup_index_recs;
		RETURN 0
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN tran_cleanup_index_recs;
		THROW;
	END CATCH
END

