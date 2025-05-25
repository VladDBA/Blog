SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.sysquery_context_settings_2016 AS
	SELECT
		context_settings_id,
		CONVERT(varbinary(8), set_options) AS 'set_options',
		language_id,
		date_format,
		date_first,
		CONVERT(varbinary(2), status) AS 'status',
		required_cursor_options,
		acceptable_cursor_options,
		merge_action_type,
		default_schema_id,
		is_replication_specific,
		CONVERT(varbinary(1), status2) AS 'is_contained'
	FROM (
		SELECT * FROM sys.plan_persist_context_settings
		UNION ALL
		SELECT TOP 0 * FROM OpenRowSet(TABLE QUERY_STORE_CONTEXT_SETTINGS)
	) AS ContextSettings

