SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

-- null value means we do not want to update the corresponding column in internal table
CREATE PROCEDURE sys.sp_auto_tuning_update_create_index_workflow
	@execution_id uniqueidentifier,
	@wf_expected_state int,
	@wf_new_state int=NULL,
	@retry_count int=NULL,
	@properties nvarchar(max)=NULL,
	@recommendation_id bigint=NULL, -- null means there is no update on associated recommendation.
	@expected_state int=NULL,
	@new_state int=NULL,
	@archived bit=NULL,
	@extended_properties nvarchar(max)=NULL,
	@estimated_space_change_cached float=NULL,
	@estimated_action_duration_cached float=NULL,
	@reported_number_of_queries_with_improved_performance_cached int=NULL,
	@reported_number_of_queries_with_regressed_performance_cached int=NULL,
	@reported_cpu_utilization_change_absolute_cached float=NULL,
	@reported_cpu_utilization_change_relative_cached float=NULL,
	@reported_logical_reads_change_absolute_cached float=NULL,
	@reported_logical_reads_change_relative_cached float=NULL,
	@reported_logical_writes_change_absolute_cached float=NULL,
	@reported_logical_writes_change_relative_cached float=NULL,
	@index_size_kb_before_action bigint=NULL,
	@index_size_kb_after_action bigint=NULL
AS
BEGIN
	BEGIN TRAN tran_create_update_workflow;
	BEGIN TRY
	IF @execution_id IS NULL
		RAISERROR(15725, -1, 7, N'execution_id');

	IF @wf_expected_state IS NULL
		RAISERROR(15725, -1, 8, N'wf_expected_state');

	DECLARE @return_status int;
	EXEC @return_status = sys.sp_auto_tuning_update_workflow @execution_id, @wf_expected_state, @wf_new_state, @retry_count, @properties;

	IF @return_status = 0
	BEGIN
		IF @recommendation_id IS NOT NULL
		BEGIN
			-- update create index recommendation associated with the workflow
			EXEC @return_status = sys.sp_auto_tuning_update_index_recommendation @recommendation_id,
				@expected_state,
				@new_state,
				@archived,
				@extended_properties,
				@estimated_space_change_cached,
				@estimated_action_duration_cached,
				@reported_number_of_queries_with_improved_performance_cached,
				@reported_number_of_queries_with_regressed_performance_cached,
				@reported_cpu_utilization_change_absolute_cached,
				@reported_cpu_utilization_change_relative_cached,
				@reported_logical_reads_change_absolute_cached,
				@reported_logical_reads_change_relative_cached,
				@reported_logical_writes_change_absolute_cached,
				@reported_logical_writes_change_relative_cached,
				@index_size_kb_before_action,
				@index_size_kb_after_action
				;
		END
	END
	IF @return_status != 0
	BEGIN
		ROLLBACK TRAN tran_create_update_workflow;
		RAISERROR(15723, -1, 7, @return_status);
	END
	ELSE
		COMMIT TRAN tran_create_update_workflow;
	RETURN 0
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN tran_create_update_workflow;
		THROW;
	END CATCH
END

