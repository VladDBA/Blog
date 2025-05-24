SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_hadr_internal_availability_groups AS
	SELECT 
		distributed_availability_group_id = ags.user_availability_group_id,
		group_id = ags.group_id,
		name = CONVERT(sysname, ags.name),
		resource_id = CAST(ags.resource_id AS nvarchar(40)),
		resource_group_id = CAST(ags.resource_group_id AS nvarchar(40)),
		failure_condition_level,
		health_check_timeout,
		automated_backup_preference,
		automated_backup_preference_desc,
		version,
		basic_features,
		dtc_support,
		db_failover,
		is_distributed,
		cluster_type,
		cluster_type_desc,
		required_synchronized_secondaries_to_commit,
		sequence_number,
		is_contained
	FROM sys.availability_groups$ as ags
	WHERE is_internal = 1

