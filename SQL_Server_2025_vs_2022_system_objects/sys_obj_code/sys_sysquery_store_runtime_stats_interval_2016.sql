use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.sysquery_store_runtime_stats_interval_2016 AS
	SELECT	*
	FROM sys.plan_persist_runtime_stats_interval
	UNION ALL
	SELECT	TOP 0 *
	FROM OpenRowSet(TABLE QUERY_STORE_RUNTIME_STATS_INTERVAL)

