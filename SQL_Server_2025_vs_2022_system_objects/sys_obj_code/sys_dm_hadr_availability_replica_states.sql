SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_hadr_availability_replica_states AS
	SELECT
		ars.replica_id,
		ars.group_id,
		ars.is_local,
		ars.role,
		ars.role_desc,
		ars.operational_state,
		ars.operational_state_desc,
		ars.connected_state,
		ars.connected_state_desc,
		recovery_health = CASE
			WHEN dbs.database_state_aggr IS NULL THEN NULL
			WHEN ars.configured_database_count > dbs.database_count THEN CAST (0 AS TINYINT)	-- ONLINE_IN_PROGRESS
			WHEN dbs.database_state_aggr = 0 THEN CAST (1 AS TINYINT)							-- ONLINE
			ELSE CAST (0 AS TINYINT) END,														-- ONLINE_IN_PROGRESS
		recovery_health_desc = CASE
			WHEN dbs.database_state_aggr IS NULL THEN CAST (NULL AS NVARCHAR(60))
			WHEN ars.configured_database_count > dbs.database_count THEN CAST ('ONLINE_IN_PROGRESS' AS NVARCHAR(60))
			WHEN dbs.database_state_aggr = 0 THEN CAST ('ONLINE' AS NVARCHAR(60))
			ELSE CAST ('ONLINE_IN_PROGRESS' AS NVARCHAR(60)) END,
		synchronization_health = CASE
			WHEN (war.availability_mode = 4 AND ars.connected_state = 1) THEN CAST (2 AS TINYINT) -- Configuration-only, always healthy when connected
			WHEN dbs.synchronization_health_aggr IS NULL THEN CAST (0 AS TINYINT)				-- NOT_HEALTHY
			WHEN ars.configured_database_count > dbs.database_count THEN CAST (0 AS TINYINT)	-- NOT_HEALTHY (one or more DBs not joined)
			ELSE CAST (dbs.synchronization_health_aggr AS TINYINT) END,
		synchronization_health_desc = CASE
			WHEN (war.availability_mode = 4 AND ars.connected_state = 1) THEN CAST ('HEALTHY' AS NVARCHAR(60))
			WHEN dbs.synchronization_health_aggr IS NULL THEN CAST ('NOT_HEALTHY' AS NVARCHAR(60))
			WHEN ars.configured_database_count > dbs.database_count THEN CAST ('NOT_HEALTHY' AS NVARCHAR(60))
			WHEN dbs.synchronization_health_aggr = 2 THEN CAST ('HEALTHY' AS NVARCHAR(60))
			WHEN dbs.synchronization_health_aggr = 1 THEN CAST ('PARTIALLY_HEALTHY' AS NVARCHAR(60))
			ELSE CAST ('NOT_HEALTHY' AS NVARCHAR(60)) END,
		ars.last_connect_error_number,
		ars.last_connect_error_description,
		ars.last_connect_error_timestamp,
		ars.write_lease_remaining_ticks,
		ars.current_configuration_commit_start_time_utc,
		ars.is_internal
	FROM
		sys.dm_hadr_internal_availability_replica_states ars
		LEFT OUTER JOIN
		(
			SELECT
				replica_id,
				database_count = COUNT (*),
				synchronization_health_aggr = MIN (synchronization_health),
				database_state_aggr = MAX (database_state)
			FROM
				sys.dm_hadr_database_replica_states
			where is_local = 1
			GROUP BY replica_id
			UNION
			SELECT
				replica_id,
				database_count = COUNT (*),
				synchronization_health_aggr = MIN (synchronization_health),
				database_state_aggr = NULL
			FROM
				sys.dm_hadr_database_replica_states
			where is_local = 0
			GROUP BY replica_id
		)
		AS dbs
		ON (ars.real_replica_id = dbs.replica_id)
		LEFT OUTER JOIN
		(
			SELECT
				ag_replica_id,
				availability_mode
			FROM
				sys.hadr_availability_replicas$ -- defined in metaview.xml
		)
		AS war
		ON (ars.real_replica_id = war.ag_replica_id)


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_hadr_availability_replica_states AS
	SELECT
		ars.replica_id,
		ars.group_id,
		ars.is_local,
		ars.role,
		ars.role_desc,
		ars.operational_state,
		ars.operational_state_desc,
		ars.connected_state,
		ars.connected_state_desc,
		recovery_health = CASE
			WHEN dbs.database_state_aggr IS NULL THEN NULL
			WHEN ars.configured_database_count > dbs.database_count THEN CAST (0 AS TINYINT)	-- ONLINE_IN_PROGRESS
			WHEN dbs.database_state_aggr = 0 THEN CAST (1 AS TINYINT)							-- ONLINE
			ELSE CAST (0 AS TINYINT) END,														-- ONLINE_IN_PROGRESS
		recovery_health_desc = CASE
			WHEN dbs.database_state_aggr IS NULL THEN CAST (NULL AS NVARCHAR(60))
			WHEN ars.configured_database_count > dbs.database_count THEN CAST ('ONLINE_IN_PROGRESS' AS NVARCHAR(60))
			WHEN dbs.database_state_aggr = 0 THEN CAST ('ONLINE' AS NVARCHAR(60))
			ELSE CAST ('ONLINE_IN_PROGRESS' AS NVARCHAR(60)) END,
		synchronization_health = CASE
			WHEN (war.availability_mode = 4 AND ars.connected_state = 1) THEN CAST (2 AS TINYINT) -- Configuration-only, always healthy when connected
			WHEN dbs.synchronization_health_aggr IS NULL THEN CAST (0 AS TINYINT)				-- NOT_HEALTHY
			WHEN ars.configured_database_count > dbs.database_count THEN CAST (0 AS TINYINT)	-- NOT_HEALTHY (one or more DBs not joined)
			ELSE CAST (dbs.synchronization_health_aggr AS TINYINT) END,
		synchronization_health_desc = CASE
			WHEN (war.availability_mode = 4 AND ars.connected_state = 1) THEN CAST ('HEALTHY' AS NVARCHAR(60))
			WHEN dbs.synchronization_health_aggr IS NULL THEN CAST ('NOT_HEALTHY' AS NVARCHAR(60))
			WHEN ars.configured_database_count > dbs.database_count THEN CAST ('NOT_HEALTHY' AS NVARCHAR(60))
			WHEN dbs.synchronization_health_aggr = 2 THEN CAST ('HEALTHY' AS NVARCHAR(60))
			WHEN dbs.synchronization_health_aggr = 1 THEN CAST ('PARTIALLY_HEALTHY' AS NVARCHAR(60))
			ELSE CAST ('NOT_HEALTHY' AS NVARCHAR(60)) END,
		ars.last_connect_error_number,
		ars.last_connect_error_description,
		ars.last_connect_error_timestamp,
		ars.write_lease_remaining_ticks,
		ars.current_configuration_commit_start_time_utc
	FROM
		sys.dm_hadr_internal_availability_replica_states ars
		LEFT OUTER JOIN
		(
			SELECT
				replica_id,
				database_count = COUNT (*),
				synchronization_health_aggr = MIN (synchronization_health),
				database_state_aggr = MAX (database_state)
			FROM
				sys.dm_hadr_database_replica_states
			GROUP BY replica_id
		)
		AS dbs
		ON (ars.real_replica_id = dbs.replica_id)
		LEFT OUTER JOIN
		(
			SELECT
				ag_replica_id,
				availability_mode
			FROM
				sys.hadr_availability_replicas$ -- defined in metaview.xml
		)
		AS war
		ON (ars.real_replica_id = war.ag_replica_id)

