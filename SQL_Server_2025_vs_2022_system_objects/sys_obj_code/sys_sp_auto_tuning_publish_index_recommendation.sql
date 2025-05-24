SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_auto_tuning_publish_index_recommendation
	@index_type tinyint,
	@schema sysname,
	@table sysname,
	@index_columns nvarchar(4000),
	@included_columns nvarchar(max),
	@index_name sysname,
	@baseline_data_collecting_start_time_utc datetime,
	@estimated_score int,
	@estimated_space_change float,
	@recommendation_source smallint,
	@avg_number_of_executions_per_hour_before_actions	nvarchar(max),
	@avg_qo_cost_per_execution_before_actions			nvarchar(max),
	@avg_qo_cost_per_execution_after_actions			nvarchar(max),
	@avg_cpu_cost_per_execution_before_actions			nvarchar(max),
	@avg_cpu_cost_per_execution_after_actions			nvarchar(max),
	@impact_queries varbinary(max),
	@src_execution_id uniqueidentifier,
	@group_id uniqueidentifier,
	@recommendation_id bigint OUTPUT,
	@is_new bit OUTPUT
AS
BEGIN
	exec sys.sp_auto_tuning_validate_executable;
	BEGIN TRAN tran_publish_recommendation;
	BEGIN TRY
	DECLARE @return_status int;
	DECLARE @sql_string nvarchar(max);
	SET @sql_string = N'
	DECLARE @return_value INT = 0;
	DECLARE @existing_state INT = NULL;
	DECLARE @existing_recommendation_id BIGINT = NULL;
	SET @recommendation_id_out=NULL;
	SET @is_new_out=NULL;
	DECLARE @cur_utc DATETIME = GETUTCDATE();

	DECLARE @cur_queries BIGINT;
	DECLARE @cur_query_i BIGINT;
	DECLARE @cur_query_hash BINARY(8);

	-- mark the the state of the index recommendation which has the same table and schema with
	-- the new one but different index columns from state active (0) to state Expired (10)
	UPDATE r
	SET r.[state]=10 /*Expired*/
	FROM sys.ats_recommendations r
		INNER JOIN sys.ats_index_recommendations ir
		ON r.id = ir.id
	WHERE ir.[table]=@table_in
		AND ir.[schema]=@schema_in
		AND r.[state]=0
		AND ir.[index_columns] != @index_columns_in

	SELECT TOP 1 @existing_recommendation_id=ir.id, @existing_state=r.[state]
	FROM sys.ats_recommendations r, sys.ats_index_recommendations ir
	WHERE r.[state] NOT IN (4 /*Success*/, 7 /*Reverted*/, 9/*Error*/)
		AND ir.id = r.id
		AND ir.[table]=@table_in
		AND ir.[schema]=@schema_in
		AND ir.[index_columns]=@index_columns_in
		AND r.[archived]=0

	IF @existing_recommendation_id IS NOT NULL
	BEGIN
		-- There is an existing duplicate index recommendation( we treat the index recommendation to be duplicated as long as the
		-- table, schema and index columns are the same (not including included columns, and this is existing logics).
		SET @recommendation_id_out=@existing_recommendation_id;
		SET @is_new_out=0;
		IF @existing_state = 8 /*Ignored*/
			BEGIN
			-- The duplicate recommendation is in Ignored state but not archived yet, let us wait
			-- for them to be archived, and discard the current recomendation (we do not want to frequently
			-- build same indexes).
			-- Ingored is the recommendation marked by customer that they do not want it.
			SET @recommendation_id_out = NULL;
			END
		ELSE IF @existing_state IN(10 /*expired*/, 0 /*active*/)
			BEGIN

			-- Reactivate the existing state
			UPDATE sys.ats_recommendations
			SET [state]=0 /*Active*/, [last_refresh_timestamp_utc]=@cur_utc
			WHERE [id] = @existing_recommendation_id;

			UPDATE sys.ats_index_recommendations
			SET [index_type] = @index_type_in,
				[index_name] = @index_name_in,
				[included_columns] = @included_columns_in,
				[recommendation_source] = @recommendation_source_in,
				[estimated_space_change_cached] = @estimated_space_change_in,
				[baseline_data_collecting_start_time_utc] = @baseline_data_collecting_start_time_utc_in
			WHERE [id] = @existing_recommendation_id;

			-- Delete old query hash for the query
			DELETE FROM sys.ats_index_recommendation_impact_queries
			WHERE [recommendation_id]=@existing_recommendation_id;

			DELETE FROM sys.ats_recommended_impact_values
			WHERE [recommendation_id]=@existing_recommendation_id;

			END
		ELSE
			BEGIN
				-- This shall not happen, but if it does, let us just fail.
				SET @recommendation_id_out = NULL;
				RAISERROR(15723, 15, 3, 101);
			END
	END
	ELSE
	BEGIN
		-- Insert a new recommendation
		SET @is_new_out = 1;

		INSERT INTO sys.ats_recommendations([state], [type_id], [recommendation_timestamp_utc], [last_refresh_timestamp_utc], [archived], [extended_properties], [group_id])
		VALUES (0 /*for Active*/, 0 /* for create index */, @cur_utc, @cur_utc, 0 /*not archived*/, NULL, @group_id_in);

		-- Get the recommendation id of ats_recommendations
		SET @recommendation_id_out = SCOPE_IDENTITY();

		INSERT INTO sys.ats_index_recommendations
		VALUES(@recommendation_id_out, @index_type_in, @schema_in, @table_in, @index_columns_in, @included_columns_in,
				@index_name_in,
				@estimated_space_change_in,
				NULL, /*estimated_action_duration_cached*/
				NULL, /*reported_number_of_queries_with_improved_performance_cached*/
				NULL, /*reported_number_of_queries_with_regressed_performance_cached*/
				NULL, /*reported_cpu_utilization_change_absolute_cached*/
				NULL, /*reported_cpu_utilization_change_relative_cached*/
				NULL, /*reported_logical_reads_change_absolute_cached*/
				NULL, /*reported_logical_reads_change_relative_cached*/
				NULL, /*reported_logical_writes_change_absolute_cached*/
				NULL, /*reported_logical_writes_change_relative_cached*/
				NULL, /*index_size_kb_before_action*/
				NULL, /*index_size_kb_after_action*/
				@recommendation_source_in,
				@baseline_data_collecting_start_time_utc_in,
				@estimated_score_in);
	END

	IF @recommendation_id_out is not NULL
	BEGIN
		INSERT INTO sys.ats_workflow_recommendation_relation
		VALUES(@src_execution_id_in, @recommendation_id_out);

		CREATE TABLE #impact_query_hashs(query_hash binary(8), ordinal bigint);
		SET @cur_queries = DATALENGTH(@impact_queries_in) / 8;
		SET @cur_query_i = 0;
		WHILE @cur_query_i < @cur_queries
		BEGIN
			SET @cur_query_hash = CONVERT(BINARY(8), SUBSTRING(@impact_queries_in, @cur_query_i * 8 + 1, 8));

			INSERT INTO #impact_query_hashs
			VALUES(@cur_query_hash, @cur_query_i + 1);

			SET @cur_query_i = @cur_query_i + 1;
		END;

		WITH cte_ec(f_value, ordinal) AS
		(SELECT CAST(TRIM(value) AS float) AS f_value, ordinal
		FROM STRING_SPLIT(@avg_number_of_executions_per_hour_before_actions_in, N'','', 1))
		INSERT INTO sys.ats_index_recommendation_impact_queries([recommendation_id], [query_hash], [metric],
		[avg_metric_before_action],[avg_metric_after_action])
		SELECT @recommendation_id_out, iqh.query_hash, 1 /*observed execution count*/, cte_ec.f_value, NULL
		FROM #impact_query_hashs iqh,cte_ec
		WHERE iqh.ordinal= cte_ec.ordinal;

		WITH cte_qb(f_value, ordinal) AS
		(SELECT CAST(TRIM(value) AS float) AS f_value, ordinal
		FROM STRING_SPLIT(@avg_qo_cost_per_execution_before_actions_in, N'','', 1)),
		cte_qa(f_value, ordinal) AS
		(SELECT CAST(TRIM(value) AS float) AS f_value, ordinal
		FROM STRING_SPLIT(@avg_qo_cost_per_execution_after_actions_in, N'','', 1))
		INSERT INTO sys.ats_index_recommendation_impact_queries([recommendation_id], [query_hash], [metric],
		[avg_metric_before_action],[avg_metric_after_action])
		SELECT @recommendation_id_out, iqh.query_hash, 0 /*estimated qo cost*/, cte_qb.f_value,cte_qa.f_value
		FROM #impact_query_hashs iqh, cte_qb, cte_qa
		WHERE iqh.ordinal=cte_qb.ordinal AND cte_qb.ordinal=cte_qa.ordinal;

		WITH cte_qb(f_value, ordinal) AS
		(SELECT CAST(TRIM(value) AS float) AS f_value, ordinal
		FROM STRING_SPLIT(@avg_cpu_cost_per_execution_before_actions_in, N'','', 1)),
		cte_qa(f_value, ordinal) AS
		(SELECT CAST(TRIM(value) AS float) AS f_value, ordinal
		FROM STRING_SPLIT(@avg_cpu_cost_per_execution_after_actions_in, N'','', 1))
		INSERT INTO sys.ats_index_recommendation_impact_queries([recommendation_id], [query_hash], [metric],
		[avg_metric_before_action],[avg_metric_after_action])
		SELECT @recommendation_id_out, iqh.query_hash, 2 /*estimated cpu cost*/, cte_qb.f_value, cte_qa.f_value
		FROM #impact_query_hashs iqh, cte_qb, cte_qa
		WHERE iqh.ordinal=cte_qb.ordinal AND cte_qb.ordinal=cte_qa.ordinal;

		INSERT INTO sys.ats_recommended_impact_values([recommendation_id], [dimension_id], [impact_type_id], [unit_type_id], [absolute_value], [change_value_absolute],[change_value_relative])
		VALUES(@recommendation_id_out, 0 /*SpaceChange*/, 0 /*estimated*/, 0/*megabytes*/, cast(@estimated_space_change_in*1.0/1024 AS FLOAT), NULL, NULL);
	END
	';
	DECLARE @params_def nvarchar(4000);
	SET @params_def = N'@index_type_in tinyint,
	@schema_in sysname,
	@table_in sysname,
	@index_columns_in nvarchar(4000),
	@included_columns_in nvarchar(max),
	@index_name_in sysname,
	@baseline_data_collecting_start_time_utc_in datetime,
	@estimated_score_in int,
	@estimated_space_change_in float,
	@recommendation_source_in smallint,
	@avg_number_of_executions_per_hour_before_actions_in	nvarchar(max),
	@avg_qo_cost_per_execution_before_actions_in			nvarchar(max),
	@avg_qo_cost_per_execution_after_actions_in				nvarchar(max),
	@avg_cpu_cost_per_execution_before_actions_in			nvarchar(max),
	@avg_cpu_cost_per_execution_after_actions_in			nvarchar(max),
	@impact_queries_in varbinary(max),
	@src_execution_id_in uniqueidentifier,
	@group_id_in uniqueidentifier,
	@recommendation_id_out bigint OUTPUT,
	@is_new_out bit OUTPUT
	'
	EXEC @return_status = sp_executesql @sql_string,
		@params_def,
		@index_type_in=@index_type,
		@schema_in=@schema,
		@table_in=@table,
		@index_columns_in=@index_columns,
		@included_columns_in=@included_columns,
		@index_name_in=@index_name,
		@baseline_data_collecting_start_time_utc_in=@baseline_data_collecting_start_time_utc,
		@estimated_score_in=@estimated_score,
		@estimated_space_change_in=@estimated_space_change,
		@recommendation_source_in=@recommendation_source,
		@avg_number_of_executions_per_hour_before_actions_in=@avg_number_of_executions_per_hour_before_actions,
		@avg_qo_cost_per_execution_before_actions_in=@avg_qo_cost_per_execution_before_actions,
		@avg_qo_cost_per_execution_after_actions_in=@avg_qo_cost_per_execution_after_actions,
		@avg_cpu_cost_per_execution_before_actions_in=@avg_cpu_cost_per_execution_before_actions,
		@avg_cpu_cost_per_execution_after_actions_in=@avg_cpu_cost_per_execution_after_actions,
		@impact_queries_in=@impact_queries,
		@src_execution_id_in=@src_execution_id,
		@group_id_in=@group_id,
		@recommendation_id_out = @recommendation_id OUTPUT,
		@is_new_out=@is_new OUTPUT;
	IF @return_status != 0
	BEGIN
		ROLLBACK TRAN tran_publish_recommendation;
		RAISERROR(15723, -1, 1, @return_status);
	END
	ELSE
		COMMIT TRAN tran_publish_recommendation;
	RETURN 0
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN tran_publish_recommendation;
		THROW;
	END CATCH
END

