use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

CREATE VIEW sys.query_store_plan_feedback AS
	SELECT
		plan_feedback_id,
		plan_id,
		feature_id,
		feature_desc,
		feedback_data_json(feature_id, feedback_data) as feedback_data,
		state,
		state_desc,
		create_time,
		last_updated_time
	FROM sys.plan_persist_plan_feedback_in_memory where plan_feedback_id < -1
	UNION ALL
	SELECT
		PF.plan_feedback_id,
		PF.plan_id,
		PF.feature_id,
		convert(nvarchar(60), CASE PF.feature_id
			WHEN 1 THEN 'CE Feedback'			-- CE Feedback
			WHEN 2 THEN 'Memory Grant Feedback'	-- MG Feedback
			WHEN 3 THEN 'DOP Feedback'			-- DOP Feedback
			WHEN 4 THEN 'LAQ Feedback'			-- LAQ Feedback
			ELSE 'Invalid Feedback'
			END) COLLATE Latin1_General_CI_AS_KS_WS as feature_desc,
		IIF(PFM.feedback_data is NULL,
			feedback_data_json(PF.feature_id, PF.feedback_data),
			feedback_data_json(PFM.feature_id, PFM.feedback_data)) as feedback_data,
		IIF(PFM.state is NULL, PF.state, PFM.state) as state,
		convert(nvarchar(60), CASE PF.state
			WHEN 0 THEN 'NO_FEEDBACK'
			WHEN 1 THEN 'NO_RECOMMENDATION'
			WHEN 2 THEN 'PENDING_VALIDATION'
			when 3 THEN 'IN_VALIDATION'
			WHEN 4 THEN 'VERIFICATION_REGRESSED'
			WHEN 5 THEN 'VERIFICATION_PASSED'
			WHEN 6 THEN 'ROLLEDBACK_BY_APRC'
			WHEN 7 THEN 'FEEDBACK_VALID'
			WHEN 8 THEN 'FEEDBACK_INVALID'
			ELSE 'INVALID_VALUE'
			END) COLLATE Latin1_General_CI_AS_KS_WS as state_desc,
		PF.create_time,
		IIF(PFM.last_updated_time is NULL, PF.last_updated_time, PFM.last_updated_time) as last_updated_time
	-- NOLOCK to prevent potential deadlock between QDS_STATEMENT_STABILITY lock and index locks
	FROM sys.plan_persist_plan_feedback PF WITH (NOLOCK)
	LEFT OUTER JOIN sys.plan_persist_plan_feedback_in_memory PFM ON
		PF.plan_feedback_id = PFM.plan_feedback_id


/*====  SQL Server 2022 version  ====*/

CREATE VIEW sys.query_store_plan_feedback AS
	SELECT
		plan_feedback_id,
		plan_id,
		feature_id,
		feature_desc,
		feedback_data_json(feature_id, feedback_data) as feedback_data,
		state,
		state_desc,
		create_time,
		last_updated_time
	FROM sys.plan_persist_plan_feedback_in_memory where plan_feedback_id < -1
	UNION ALL
	SELECT
		PF.plan_feedback_id,
		PF.plan_id,
		PF.feature_id,
		convert(nvarchar(60), CASE PF.feature_id
			WHEN 1 THEN 'CE Feedback'			-- CE Feedback
			WHEN 2 THEN 'Memory Grant Feedback'	-- MG Feedback
			WHEN 3 THEN 'DOP Feedback'			-- DOP Feedback
			ELSE 'Invalid Feedback'
			END) COLLATE Latin1_General_CI_AS_KS_WS as feature_desc,
		IIF(PFM.feedback_data is NULL,
			feedback_data_json(PF.feature_id, PF.feedback_data),
			feedback_data_json(PFM.feature_id, PFM.feedback_data)) as feedback_data,
		IIF(PFM.state is NULL, PF.state, PFM.state) as state,
		convert(nvarchar(60), CASE PF.state
			WHEN 0 THEN 'NO_FEEDBACK'
			WHEN 1 THEN 'NO_RECOMMENDATION'
			WHEN 2 THEN 'PENDING_VALIDATION'
			when 3 THEN 'IN_VALIDATION'
			WHEN 4 THEN 'VERIFICATION_REGRESSED'
			WHEN 5 THEN 'VERIFICATION_PASSED'
			WHEN 6 THEN 'ROLLEDBACK_BY_APRC'
			WHEN 7 THEN 'FEEDBACK_VALID'
			WHEN 8 THEN 'FEEDBACK_INVALID'
			ELSE 'INVALID_VALUE'
			END) COLLATE Latin1_General_CI_AS_KS_WS as state_desc,
		PF.create_time,
		IIF(PFM.last_updated_time is NULL, PF.last_updated_time, PFM.last_updated_time) as last_updated_time
	-- NOLOCK to prevent potential deadlock between QDS_STATEMENT_STABILITY lock and index locks
	FROM sys.plan_persist_plan_feedback PF WITH (NOLOCK)
	LEFT OUTER JOIN sys.plan_persist_plan_feedback_in_memory PFM ON
		PF.plan_feedback_id = PFM.plan_feedback_id

