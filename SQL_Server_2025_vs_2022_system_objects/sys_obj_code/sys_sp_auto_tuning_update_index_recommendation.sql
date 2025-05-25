use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

-- parameter value is null means we do not want to update the corresponding column in internal table
CREATE PROCEDURE sys.sp_auto_tuning_update_index_recommendation
	@recommendation_id bigint,
	@expected_state int,
	@new_state int,
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
	exec sys.sp_auto_tuning_validate_executable;

	BEGIN TRAN tran_update_recommendation;
	BEGIN TRY
	DECLARE @return_status int;
	DECLARE @sql_string nvarchar(max);
	SET @sql_string = N'
		IF EXISTS(SELECT state FROM sys.ats_recommendations WHERE id=@recommendation_id_in AND state=@expected_state_in)
		BEGIN
			UPDATE sys.ats_recommendations
			SET state=ISNULL(@new_state_in, state),
				last_refresh_timestamp_utc=GETUTCDATE(),
				archived=ISNULL(@archived_in, archived),
				extended_properties=ISNULL(@extended_properties_in, extended_properties)
			WHERE id=@recommendation_id_in;

			UPDATE sys.ats_index_recommendations
			SET estimated_space_change_cached=ISNULL(@estimated_space_change_cached_in, estimated_space_change_cached),
				estimated_action_duration_cached=ISNULL(@estimated_action_duration_cached_in, estimated_action_duration_cached),
				reported_number_of_queries_with_improved_performance_cached=ISNULL(@reported_number_of_queries_with_improved_performance_cached_in, reported_number_of_queries_with_improved_performance_cached),
				reported_number_of_queries_with_regressed_performance_cached=ISNULL(@reported_number_of_queries_with_regressed_performance_cached_in, reported_number_of_queries_with_regressed_performance_cached),
				reported_cpu_utilization_change_absolute_cached=ISNULL(@reported_cpu_utilization_change_absolute_cached_in, reported_cpu_utilization_change_absolute_cached),
				reported_cpu_utilization_change_relative_cached=ISNULL(@reported_cpu_utilization_change_relative_cached_in, reported_cpu_utilization_change_relative_cached),
				reported_logical_reads_change_absolute_cached=ISNULL(@reported_logical_reads_change_absolute_cached_in, reported_logical_reads_change_absolute_cached),
				reported_logical_reads_change_relative_cached=ISNULL(@reported_logical_reads_change_relative_cached_in, reported_logical_reads_change_relative_cached),
				reported_logical_writes_change_absolute_cached=ISNULL(@reported_logical_writes_change_absolute_cached_in, reported_logical_writes_change_absolute_cached),
				reported_logical_writes_change_relative_cached=ISNULL(@reported_logical_writes_change_relative_cached_in, reported_logical_writes_change_relative_cached),
				index_size_kb_before_action=ISNULL(@index_size_kb_before_action_in, index_size_kb_before_action),
				index_size_kb_after_action=ISNULL(@index_size_kb_after_action_in, index_size_kb_after_action)
			WHERE id=@recommendation_id_in;
		END
		ELSE
		BEGIN
			DECLARE @message_error nvarchar(4000) = ''Recommendation ID: ''+ISNULL(CONVERT(nvarchar(128), @recommendation_id_in), ''null'')+'' does not exist or state: ''+ISNULL(CONVERT(nvarchar(128), @expected_state_in), ''null'') + '' does not exist!'';
			RAISERROR(15726, -1, 2, @message_error);
		END
	';

	DECLARE @params_def nvarchar(4000);
	SET @params_def = N'
	@recommendation_id_in bigint,
	@expected_state_in int,
	@new_state_in int,
	@archived_in bit,
	@extended_properties_in nvarchar(max),
	@estimated_space_change_cached_in float,
	@estimated_action_duration_cached_in float,
	@reported_number_of_queries_with_improved_performance_cached_in int,
	@reported_number_of_queries_with_regressed_performance_cached_in int,
	@reported_cpu_utilization_change_absolute_cached_in float,
	@reported_cpu_utilization_change_relative_cached_in float,
	@reported_logical_reads_change_absolute_cached_in float,
	@reported_logical_reads_change_relative_cached_in float,
	@reported_logical_writes_change_absolute_cached_in float,
	@reported_logical_writes_change_relative_cached_in float,
	@index_size_kb_before_action_in bigint,
	@index_size_kb_after_action_in bigint
	';
	EXEC @return_status = sp_executesql @sql_string,
		@params_def,
		@recommendation_id_in=@recommendation_id,
		@expected_state_in=@expected_state,
		@new_state_in=@new_state,
		@archived_in=@archived,
		@extended_properties_in=@extended_properties,
		@estimated_space_change_cached_in=@estimated_space_change_cached,
		@estimated_action_duration_cached_in=@estimated_action_duration_cached,
		@reported_number_of_queries_with_improved_performance_cached_in=@reported_number_of_queries_with_improved_performance_cached,
		@reported_number_of_queries_with_regressed_performance_cached_in=@reported_number_of_queries_with_regressed_performance_cached,
		@reported_cpu_utilization_change_absolute_cached_in=@reported_cpu_utilization_change_absolute_cached,
		@reported_cpu_utilization_change_relative_cached_in=@reported_cpu_utilization_change_relative_cached,
		@reported_logical_reads_change_absolute_cached_in=@reported_logical_reads_change_absolute_cached,
		@reported_logical_reads_change_relative_cached_in=@reported_logical_reads_change_relative_cached,
		@reported_logical_writes_change_absolute_cached_in=@reported_logical_writes_change_absolute_cached,
		@reported_logical_writes_change_relative_cached_in=@reported_logical_writes_change_relative_cached,
		@index_size_kb_before_action_in=@index_size_kb_before_action,
		@index_size_kb_after_action_in=@index_size_kb_after_action
		;
	IF @return_status != 0
	BEGIN
		ROLLBACK TRAN tran_update_recommendation;
		RAISERROR(15723, -1, 5, @return_status);
	END
	ELSE
		COMMIT TRAN tran_update_recommendation;
	RETURN 0
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN tran_update_recommendation;
		THROW;
	END CATCH
END

