SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

-- The original SP of runner has the following two parameters
--	@verification_action_start_time DATETIME = NULL,
--	@verification_action_duration TIME = NULL,
-- which reporting the verification timing, in new design is moved to the properties of
-- verification workflow, so update of those two parameters need to call verification's workflow update.
-- and the per query impact values will be represented as json string in the format:
-- '[{"hash": "0xD76DF8E7AEFC0102", "metric": 1, "before": 31, "after":30}, ...]';
CREATE PROCEDURE sys.sp_auto_tuning_index_recommendation_verification_report
(
	@recommendation_id bigint,
	@expected_ir_state int,
	@execution_id uniqueidentifier,
	@expected_wf_state int,
	@reported_number_of_queries_with_improved_performance int=NULL,
	@reported_number_of_queries_with_regressed_performance int=NULL,
	@reported_cpu_utilization_change_absolute float=NULL,
	@reported_cpu_utilization_change_relative float=NULL,
	@reported_logical_reads_change_absolute float=NULL,
	@reported_logical_reads_change_relative float=NULL,
	@reported_logical_writes_change_absolute float=NULL,
	@reported_logical_writes_change_relative float=NULL,
	@reported_affected_queries_cpu_utilization_change_relative float=NULL,
	@reported_affected_queries_logical_reads_change_relative float=NULL,
	@reported_affected_queries_logical_writes_change_relative float=NULL,
	@verification_progress_percent float = NULL,
	@observed_query_level_impacts_json nvarchar(max)=NULL
)
AS
BEGIN
	exec sys.sp_auto_tuning_validate_executable;

	BEGIN TRAN tran_verification_report;
	BEGIN TRY
	IF @recommendation_id IS NULL
		RAISERROR(15725, -1, 9, N'recommendation_id');

	IF @execution_id IS NULL
		RAISERROR(15725, -1, 10, N'execution_id');

	IF @expected_wf_state IS NULL
		RAISERROR(15725, -1, 11, N'expected_wf_state');

	IF @expected_ir_state IS NULL
		RAISERROR(15725, -1, 12, N'expected_ir_state');

	DECLARE @return_status int;
	DECLARE @sql_string nvarchar(max);
	SET @sql_string = N'
	DECLARE @query_impact_values TABLE(
		[hash] binary(8),
		[metric] smallint,
		[before] float,
		[after] float);

	-- There will at most 10 impact queries, so the input string observed_query_level_impacts_json_in and temp table @query_impact_values
	-- will not be huge.
	IF @observed_query_level_impacts_json_in IS NOT NULL
	BEGIN
		INSERT INTO @query_impact_values([hash], [metric], [before], [after])
		SELECT CONVERT(binary(8), [hash], 1), [metric], [before],  [after]
		FROM OPENJSON(@observed_query_level_impacts_json_in)
		WITH (
			[hash] nvarchar(18),
			[metric] smallint,
			[before] float,
			[after] float
		);
	END
	IF EXISTS(SELECT current_state FROM sys.ats_workflow_fsm WHERE execution_id=@execution_id_in AND current_state=@expected_wf_state_in)
	BEGIN
		IF EXISTS(SELECT state FROM sys.ats_recommendations WHERE id=@recommendation_id_in AND state=@expected_ir_state_in)
		BEGIN
			DECLARE @recommendation_impact_values TABLE (
						[dimension_id] smallint NOT NULL,
						[impact_type_id] tinyint NOT NULL,
						[unit_type_id] smallint,
						[absolute_value]            float,
						[change_value_absolute]     float,
						[change_value_relative]     float);
			-- dimension_id dictionary:
			--		SpaceChange : 0
			--		QueriesWithRegressedPerformance : 1
			--		CpuUtilization : 2
			--		LogicalReads : 3
			--		LogicalWrites : 4
			--		AffectedQueriesCpuUtilization : 5
			--		AffectedQueriesLogicalReads : 6
			--		AffectedQueriesLogicalWrites : 7
			--		VerificationProgress : 8
			--		QueriesWithImprovedPerformance : 9
			-- unit_type_id dictionary:
			--		MegaBytes : 0
			--		CpuCores : 1
			--		ReadsPerSecond : 2
			--		WritesPerSecond : 3
			--		Percent : 4
			--		Count : 5
			-- impact_type_id dictionary:
			--		Observed : 1
			--		Estimated : 0

			IF @reported_number_of_queries_with_regressed_performance_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (1, 5, CONVERT(float, @reported_number_of_queries_with_regressed_performance_in), NULL, NULL, 1)
			END

			IF @reported_cpu_utilization_change_absolute_in IS NOT NULL OR @reported_cpu_utilization_change_relative_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (2, 1, NULL, @reported_cpu_utilization_change_absolute_in, @reported_cpu_utilization_change_relative_in, 1)
			END

			IF @reported_logical_reads_change_absolute_in IS NOT NULL OR @reported_logical_reads_change_relative_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (3, 2, NULL, @reported_logical_reads_change_absolute_in, @reported_logical_reads_change_relative_in, 1)
			END

			IF @reported_logical_writes_change_absolute_in IS NOT NULL OR @reported_logical_writes_change_relative_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (4, 3, NULL, @reported_logical_writes_change_absolute_in, @reported_logical_writes_change_relative_in, 1)
			END

			IF @reported_cpu_utilization_change_absolute_in IS NOT NULL AND @reported_affected_queries_cpu_utilization_change_relative_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (5, 1, NULL, @reported_cpu_utilization_change_absolute_in, @reported_affected_queries_cpu_utilization_change_relative_in, 1)
			END

			IF @reported_logical_reads_change_absolute_in IS NOT NULL AND @reported_affected_queries_logical_reads_change_relative_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (6, 2, NULL, @reported_logical_reads_change_absolute_in, @reported_affected_queries_logical_reads_change_relative_in, 1)
			END

			IF @reported_logical_writes_change_absolute_in IS NOT NULL AND @reported_affected_queries_logical_writes_change_relative_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (7, 3, NULL, @reported_logical_writes_change_absolute_in, @reported_affected_queries_logical_writes_change_relative_in, 1)
			END

			IF @verification_progress_percent_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (8, 4, @verification_progress_percent_in, NULL, NULL, 1)
			END

			IF @reported_number_of_queries_with_improved_performance_in IS NOT NULL
			BEGIN
				INSERT INTO @recommendation_impact_values ([dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (9, 5, CONVERT(float, @reported_number_of_queries_with_improved_performance_in), NULL, NULL, 1)
			END

			-- insert/update the per recommendation impact values
			MERGE sys.ats_recommended_impact_values AS [target]
			USING @recommendation_impact_values AS [source]
			ON ([target].[recommendation_id] = @recommendation_id_in
				AND [target].[dimension_id] = [source].[dimension_id]
				AND [target].[impact_type_id] = [source].[impact_type_id])
			WHEN MATCHED THEN
				UPDATE SET
					[unit_type_id] = [source].[unit_type_id],
					[absolute_value] = [source].[absolute_value],
					[change_value_absolute] = [source].[change_value_absolute],
					[change_value_relative] = [source].[change_value_relative]
			WHEN NOT MATCHED THEN
				INSERT ([recommendation_id], [dimension_id], [unit_type_id], [absolute_value], [change_value_absolute], [change_value_relative], [impact_type_id])
				VALUES (@recommendation_id_in, [source].[dimension_id], [source].[unit_type_id], [source].[absolute_value], [source].[change_value_absolute], [source].[change_value_relative], [source].[impact_type_id]);

			IF EXISTS(SELECT * FROM @query_impact_values)
			BEGIN
				-- insert/update the per query impact values
				MERGE sys.ats_index_recommendation_impact_queries AS [target]
				USING @query_impact_values AS [source]
				ON ([target].[recommendation_id] = @recommendation_id_in
					AND [target].[query_hash] = [source].[hash]
					AND [target].[metric] = [source].[metric])
				WHEN MATCHED THEN
					UPDATE SET
						[avg_metric_before_action] = [source].[before],
						[avg_metric_after_action] = [source].[after]
				WHEN NOT MATCHED THEN
					INSERT ([recommendation_id], [query_hash], [metric], [avg_metric_before_action], [avg_metric_after_action])
					VALUES (@recommendation_id_in, [source].[hash], [source].[metric], [source].[before], [source].[after]);
			END

			UPDATE sys.ats_index_recommendations
			SET
				reported_number_of_queries_with_improved_performance_cached = coalesce(@reported_number_of_queries_with_improved_performance_in, reported_number_of_queries_with_improved_performance_cached),
				reported_number_of_queries_with_regressed_performance_cached = coalesce(@reported_number_of_queries_with_regressed_performance_in, reported_number_of_queries_with_regressed_performance_cached),
				reported_cpu_utilization_change_absolute_cached = coalesce(@reported_cpu_utilization_change_absolute_in, reported_cpu_utilization_change_absolute_cached),
				reported_cpu_utilization_change_relative_cached = coalesce(@reported_cpu_utilization_change_relative_in, reported_cpu_utilization_change_relative_cached),
				reported_logical_reads_change_absolute_cached = coalesce(@reported_logical_reads_change_absolute_in, reported_logical_reads_change_absolute_cached),
				reported_logical_reads_change_relative_cached = coalesce(@reported_logical_reads_change_relative_in, reported_logical_reads_change_relative_cached),
				reported_logical_writes_change_absolute_cached = coalesce(@reported_logical_writes_change_absolute_in, reported_logical_writes_change_absolute_cached),
				reported_logical_writes_change_relative_cached = coalesce(@reported_logical_writes_change_relative_in, reported_logical_writes_change_relative_cached)
			WHERE id = @recommendation_id_in;
		END
		ELSE
		BEGIN
			DECLARE @message2 nvarchar(4000) = ''Recommendation ID:''+ISNULL(CONVERT(nvarchar(128), @recommendation_id_in), ''null'')+'' does not exist or state: ''+ISNULL(CONVERT(nvarchar(128), @expected_ir_state_in), ''null'') + '' does not exists!'';
			RAISERROR(15726, -1, 4, @message2);
		END
	END
	ELSE
	BEGIN
		DECLARE @message1 nvarchar(4000) = ''Workflow ID:''+ISNULL(CONVERT(nvarchar(128), @execution_id_in), ''null'')+'' does not exist or state: ''+ISNULL(CONVERT(nvarchar(128), @expected_wf_state_in), ''null'') + '' does not exists!'';
		RAISERROR(15726, -1, 5, @message1);
	END
	';
	DECLARE @params_def nvarchar(4000);
	SET @params_def = N'
	@recommendation_id_in bigint,
	@expected_ir_state_in int,
	@execution_id_in uniqueidentifier,
	@expected_wf_state_in int,
	@reported_number_of_queries_with_improved_performance_in int,
	@reported_number_of_queries_with_regressed_performance_in int,
	@reported_cpu_utilization_change_absolute_in float,
	@reported_cpu_utilization_change_relative_in float,
	@reported_logical_reads_change_absolute_in float,
	@reported_logical_reads_change_relative_in float,
	@reported_logical_writes_change_absolute_in float,
	@reported_logical_writes_change_relative_in float,
	@reported_affected_queries_cpu_utilization_change_relative_in float,
	@reported_affected_queries_logical_reads_change_relative_in float,
	@reported_affected_queries_logical_writes_change_relative_in float,
	@verification_progress_percent_in float = NULL,
	@observed_query_level_impacts_json_in nvarchar(max)
	';
	EXEC @return_status = sp_executesql @sql_string,
		@params_def,
		@recommendation_id_in=@recommendation_id,
		@expected_ir_state_in=@expected_ir_state,
		@execution_id_in=@execution_id,
		@expected_wf_state_in=@expected_wf_state,
		@reported_number_of_queries_with_improved_performance_in=@reported_number_of_queries_with_improved_performance,
		@reported_number_of_queries_with_regressed_performance_in=@reported_number_of_queries_with_regressed_performance,
		@reported_cpu_utilization_change_absolute_in=@reported_cpu_utilization_change_absolute,
		@reported_cpu_utilization_change_relative_in=@reported_cpu_utilization_change_relative,
		@reported_logical_reads_change_absolute_in=@reported_logical_reads_change_absolute,
		@reported_logical_reads_change_relative_in=@reported_logical_reads_change_relative,
		@reported_logical_writes_change_absolute_in=@reported_logical_writes_change_absolute,
		@reported_logical_writes_change_relative_in=@reported_logical_writes_change_relative,
		@reported_affected_queries_cpu_utilization_change_relative_in=@reported_affected_queries_cpu_utilization_change_relative,
		@reported_affected_queries_logical_reads_change_relative_in=@reported_affected_queries_logical_reads_change_relative,
		@reported_affected_queries_logical_writes_change_relative_in=@reported_affected_queries_logical_writes_change_relative,
		@verification_progress_percent_in=@verification_progress_percent,
		@observed_query_level_impacts_json_in=@observed_query_level_impacts_json;
	IF @return_status != 0
		BEGIN
			ROLLBACK TRAN tran_verification_report;
			RAISERROR(15723, -1, 8, @return_status);
		END
	ELSE
		COMMIT TRAN tran_verification_report;
	RETURN 0
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN tran_verification_report;
		THROW;
	END CATCH
END

