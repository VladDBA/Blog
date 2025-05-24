SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.availability_replicas AS
	SELECT
		replica_id = CASE WHEN mr.replica_id is null THEN wr.ag_replica_id -- show remote replica
			ELSE mr.replica_id END,
		group_id = CASE WHEN mr.replica_id is null THEN wr.ag_id -- show remote group id
			ELSE mr.group_id END,
		replica_metadata_id = mr.internal_group_id,
		replica_server_name = CASE WHEN mr.replica_id is not null and wr.ag_replica_name is null THEN (select @@servername)
			ELSE wr.ag_replica_name END,
		owner_sid = mr.owner_sid,
		endpoint_url = wr.endpoint_url,
		availability_mode = wr.availability_mode,
		availability_mode_desc = CASE
			WHEN (wr.availability_mode = 1) THEN CAST('SYNCHRONOUS_COMMIT' AS nvarchar(60))
			WHEN (wr.availability_mode = 0) THEN CAST('ASYNCHRONOUS_COMMIT' AS nvarchar(60))
			WHEN (wr.availability_mode = 4) THEN CAST('CONFIGURATION_ONLY' AS nvarchar(60))
			ELSE CAST (NULL as nvarchar(60))
			END,
		failover_mode = wr.failover_mode,
		failover_mode_desc = CASE
			WHEN (wr.failover_mode = 0) THEN CAST('AUTOMATIC' AS nvarchar(60))
			WHEN (wr.failover_mode = 1) THEN CAST('MANUAL' AS nvarchar(60))
			WHEN (wr.failover_mode = 2) THEN CAST('EXTERNAL' AS nvarchar(60))
			ELSE CAST (NULL as nvarchar(60))
			END,
		session_timeout = wr.session_timeout,
		primary_role_allow_connections = wr.primary_role_allow_connections,
		primary_role_allow_connections_desc = wr.primary_role_allow_connections_desc,
		secondary_role_allow_connections = wr.secondary_role_allow_connections,
		secondary_role_allow_connections_desc = wr.secondary_role_allow_connections_desc,
		create_date = mr.create_date,
		modify_date = mr.modify_date,
		backup_priority = wr.backup_priority,
		read_only_routing_url = wr.read_only_routing_url,
		seeding_mode = wr.seeding_mode,
		seeding_mode_desc = CASE
			WHEN (wr.seeding_mode = 1) THEN CAST('MANUAL' AS nvarchar(60))
			WHEN (wr.seeding_mode = 0) THEN CAST('AUTOMATIC' AS nvarchar(60))
			ELSE CAST (NULL as nvarchar(60))
			END,
		read_write_routing_url = wr.read_write_routing_url
	FROM sys.availability_replicas_internal AS mr
	FULL JOIN sys.hadr_availability_replicas$ AS wr
		ON mr.group_id = wr.ag_id and mr.replica_id = wr.ag_replica_id
	WHERE is_internal IS NULL or is_internal = 0


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.availability_replicas AS
	SELECT
		replica_id = CASE WHEN mr.replica_id is null THEN wr.ag_replica_id -- show remote replica
			ELSE mr.replica_id END,
		group_id = CASE WHEN mr.replica_id is null THEN wr.ag_id -- show remote group id
			ELSE mr.group_id END,
		replica_metadata_id = mr.internal_group_id,
		replica_server_name = CASE WHEN mr.replica_id is not null and wr.ag_replica_name is null THEN (select @@servername)
			ELSE wr.ag_replica_name END,
		owner_sid = mr.owner_sid,
		endpoint_url = wr.endpoint_url,
		availability_mode = wr.availability_mode,
		availability_mode_desc = CASE
			WHEN (wr.availability_mode = 1) THEN CAST('SYNCHRONOUS_COMMIT' AS nvarchar(60))
			WHEN (wr.availability_mode = 0) THEN CAST('ASYNCHRONOUS_COMMIT' AS nvarchar(60))
			WHEN (wr.availability_mode = 4) THEN CAST('CONFIGURATION_ONLY' AS nvarchar(60))
			ELSE CAST (NULL as nvarchar(60))
			END,
		failover_mode = wr.failover_mode,
		failover_mode_desc = CASE
			WHEN (wr.failover_mode = 0) THEN CAST('AUTOMATIC' AS nvarchar(60))
			WHEN (wr.failover_mode = 1) THEN CAST('MANUAL' AS nvarchar(60))
			WHEN (wr.failover_mode = 2) THEN CAST('EXTERNAL' AS nvarchar(60))
			ELSE CAST (NULL as nvarchar(60))
			END,
		session_timeout = wr.session_timeout,
		primary_role_allow_connections = wr.primary_role_allow_connections,
		primary_role_allow_connections_desc = wr.primary_role_allow_connections_desc,
		secondary_role_allow_connections = wr.secondary_role_allow_connections,
		secondary_role_allow_connections_desc = wr.secondary_role_allow_connections_desc,
		create_date = mr.create_date,
		modify_date = mr.modify_date,
		backup_priority = wr.backup_priority,
		read_only_routing_url = wr.read_only_routing_url,
		seeding_mode = wr.seeding_mode,
		seeding_mode_desc = CASE
			WHEN (wr.seeding_mode = 1) THEN CAST('MANUAL' AS nvarchar(60))
			WHEN (wr.seeding_mode = 0) THEN CAST('AUTOMATIC' AS nvarchar(60))
			ELSE CAST (NULL as nvarchar(60))
			END,
		read_write_routing_url = wr.read_write_routing_url
	FROM sys.availability_replicas_internal AS mr
	FULL JOIN sys.hadr_availability_replicas$ AS wr
		ON mr.group_id = wr.ag_id and mr.replica_id = wr.ag_replica_id

