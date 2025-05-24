SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.query_store_plan AS
	SELECT 
		plan_id,
		query_id,
		plan_group_id,
		convert(nvarchar(32), 
		REPLACE(STR((engine_version & 0xff00000000 ) / 0x100000000), ' ', '') + '.'
		+ REPLACE(STR((engine_version & 0xff000000 ) / 0x1000000), ' ', '') + '.'
		+ REPLACE(STR((engine_version & 0xffff00 ) / 0x100), ' ', '') + '.'
		+ REPLACE(STR(engine_version & 0xff), ' ', '')
		) as engine_version,
		compatibility_level,
		query_plan_hash,
		cast(showplanxmldecompress(query_plan) as nvarchar(max)) as query_plan,
		is_online_index_plan,
		is_trivial_plan,
		is_parallel_plan,
		is_forced_plan,
		is_natively_compiled,
		force_failure_count,
		last_force_failure_reason,
		convert(nvarchar(128), CASE last_force_failure_reason
			WHEN 0 THEN N'NONE'
			WHEN NULL THEN N'NONE'
			WHEN 3617 THEN N'COMPILATION_ABORTED_BY_CLIENT'
			WHEN 8637 THEN N'ONLINE_INDEX_BUILD'
			WHEN 8675 THEN N'OPTIMIZATION_REPLAY_FAILED'
			WHEN 8683 THEN N'INVALID_STARJOIN'
			WHEN 8684 THEN N'TIME_OUT'
			WHEN 8689 THEN N'NO_DB'
			WHEN 8690 THEN N'HINT_CONFLICT'
			WHEN 8691 THEN N'SETOPT_CONFLICT'
			WHEN 8694 THEN N'DQ_NO_FORCING_SUPPORTED'
			WHEN 8698 THEN N'NO_PLAN'
			WHEN 8712 THEN N'NO_INDEX'
			WHEN 8713 THEN N'VIEW_COMPILE_FAILED'
			ELSE N'GENERAL_FAILURE'
			END) COLLATE Latin1_General_CI_AS_KS_WS as last_force_failure_reason_desc,
		count_compiles,
		initial_compile_start_time,
		last_compile_start_time,
		last_execution_time,
		CASE WHEN count_compiles = 0 THEN NULL ELSE convert(float, total_compile_duration) / count_compiles END as avg_compile_duration,
		last_compile_duration,
		plan_forcing_type,
		p.name as plan_forcing_type_desc,
		has_compile_replay_script,
		is_optimized_plan_forcing_disabled,
		plan_type,
		t.name as plan_type_desc
	FROM sys.plan_persist_plan_merged
	LEFT JOIN  sys.syspalvalues p ON p.class = 'PFT' AND p.value = plan_forcing_type
	LEFT JOIN  sys.syspalvalues t ON t.class = 'PTD' AND t.value = plan_type


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.query_store_plan AS
	SELECT 
		plan_id,
		query_id,
		plan_group_id,
		convert(nvarchar(32), 
		REPLACE(STR((engine_version & 0xff00000000 ) / 0x100000000), ' ', '') + '.'
		+ REPLACE(STR((engine_version & 0xff000000 ) / 0x1000000), ' ', '') + '.'
		+ REPLACE(STR((engine_version & 0xffff00 ) / 0x100), ' ', '') + '.'
		+ REPLACE(STR(engine_version & 0xff), ' ', '')
		) as engine_version,
		compatibility_level,
		query_plan_hash,
		cast(showplanxmldecompress(query_plan) as nvarchar(max)) as query_plan,
		is_online_index_plan,
		is_trivial_plan,
		is_parallel_plan,
		is_forced_plan,
		is_natively_compiled,
		force_failure_count,
		last_force_failure_reason,
		convert(nvarchar(128), CASE last_force_failure_reason
			WHEN 0 THEN N'NONE'
			WHEN NULL THEN N'NONE'
			WHEN 8637 THEN N'ONLINE_INDEX_BUILD'
			WHEN 8675 THEN N'OPTIMIZATION_REPLAY_FAILED'
			WHEN 8683 THEN N'INVALID_STARJOIN'
			WHEN 8684 THEN N'TIME_OUT'
			WHEN 8689 THEN N'NO_DB'
			WHEN 8690 THEN N'HINT_CONFLICT'
			WHEN 8691 THEN N'SETOPT_CONFLICT'
			WHEN 8694 THEN N'DQ_NO_FORCING_SUPPORTED'
			WHEN 8698 THEN N'NO_PLAN'
			WHEN 8712 THEN N'NO_INDEX'
			WHEN 8713 THEN N'VIEW_COMPILE_FAILED'
			ELSE N'GENERAL_FAILURE'
			END) COLLATE Latin1_General_CI_AS_KS_WS as last_force_failure_reason_desc,
		count_compiles,
		initial_compile_start_time,
		last_compile_start_time,
		last_execution_time,
		CASE WHEN count_compiles = 0 THEN NULL ELSE convert(float, total_compile_duration) / count_compiles END as avg_compile_duration,
		last_compile_duration,
		plan_forcing_type,
		p.name as plan_forcing_type_desc,
		has_compile_replay_script,
		is_optimized_plan_forcing_disabled,
		plan_type,
		t.name as plan_type_desc
	FROM sys.plan_persist_plan_merged
	LEFT JOIN  sys.syspalvalues p ON p.class = 'PFT' AND p.value = plan_forcing_type
	LEFT JOIN  sys.syspalvalues t ON t.class = 'PTD' AND t.value = plan_type

