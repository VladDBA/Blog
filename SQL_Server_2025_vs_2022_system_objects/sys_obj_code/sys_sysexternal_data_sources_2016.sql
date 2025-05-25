use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.sysexternal_data_sources_2016 AS
	SELECT
		seds.data_source_id AS data_source_id,
		seds.name AS name,
		seds.location AS location,
		seds.type_desc AS type_desc,
		seds.type AS type,
		seds.job_tracker_location AS resource_manager_location,
		seds.credential_id AS credential_id,
		seds.shard_map_manager_db AS database_name,
		seds.shard_map_name AS shard_map_name
	FROM sys.sysextsources seds
	WHERE has_access('ED', DB_ID()) = 1 -- catalog security check

