use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_os_performance_counters AS
	SELECT
		object_name, counter_name, instance_name, cntr_value, cntr_type
	FROM OpenRowSet(TABLE SYS_PERFORMANCE_COUNTERS)
	UNION All
	SELECT
		object_name, counter_name, instance_name, cntr_value, cntr_type
	FROM OpenRowset(TABLE XTP_PERFORMANCE_COUNTERS)
	UNION ALL
	SELECT
		object_name, counter_name, instance_name, cntr_value, cntr_type
	FROM OpenRowset(TABLE SYS_PDH_COUNTERS)


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_os_performance_counters AS
	SELECT
		object_name, counter_name, instance_name, cntr_value, cntr_type
	FROM OpenRowSet(TABLE SYS_PERFORMANCE_COUNTERS)
	UNION All
	SELECT
		object_name, counter_name, instance_name, cntr_value, cntr_type
	FROM OpenRowset(TABLE XTP_PERFORMANCE_COUNTERS)

