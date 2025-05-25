use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.dm_db_internal_auto_tuning_workflows
AS
SELECT w.[execution_id],
	w.[current_state],
	w.[create_date_utc],
	w.[last_update_date_utc],
	w.[workflow_type_id],
	w.[retry_count],
	w.[derived_from_id],
	w.[properties],
	wr.[recommendation_id]
FROM OPENROWSET(TABLE DM_DB_INTERNAL_ATS_WORKFLOW_FSM) AS w
	LEFT OUTER JOIN
	OPENROWSET(TABLE DM_DB_INTERNAL_ATS_WORKFLOW_RECOMMENDATION_RELATION) AS wr
	ON w.[execution_id] = wr.[execution_id]

