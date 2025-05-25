SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.sysquery_store_query_hints_2019 AS
	SELECT
		QH.query_hint_id,
		QH.query_id,
		QH.query_hints as query_hint_text,
		QH.last_query_hint_failure_reason,
		convert(nvarchar(128), CASE QH.last_query_hint_failure_reason
			WHEN 0 THEN N'NONE'
			WHEN NULL THEN N'NONE'
			WHEN 309 THEN N'XMLIDX_IN_HINTS'
			WHEN 321 THEN N'INVALID_TABLE_HINT'
			WHEN 1017 THEN N'DUPLICATE_HINTS'
			WHEN 1042 THEN N'CONFLICTING_OPTIMIZER_HINTS'
			WHEN 1047 THEN N'CONFLICTING_LOCKING_HINTS'
			WHEN 8622 THEN N'NO_PLAN'
			ELSE N'GENERAL_FAILURE'
			END) COLLATE Latin1_General_CI_AS_KS_WS as last_query_hint_failure_reason_desc,
		QH.query_hint_failure_count,
		QH.query_hints_flags as source,
		convert(nvarchar(128), CASE QH.query_hints_flags
			WHEN 1 THEN 'CE feedback'		-- HS_CEFeedback
			WHEN 2 THEN 'DOP feedback'		-- HS_DOPFeedback
			ELSE 'User'
			END) COLLATE Latin1_General_CI_AS_KS_WS as source_desc,
		QH.comment
	-- NOLOCK to prevent potential deadlock between QDS_STATEMENT_STABILITY lock and index locks
	FROM sys.plan_persist_query_hints QH WITH (NOLOCK)
	-- join with STVF enables view to have same security definitions as STVF; plan will remove it since it is empty table
	LEFT OUTER JOIN (
		SELECT TOP 0 
			query_hint_id,
			query_id,
			query_hints_flags
		FROM OpenRowset(TABLE QUERY_STORE_QUERY_HINTS_IN_MEM)) QHM ON
	QHM.query_hint_id = QH.query_hint_id

