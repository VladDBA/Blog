SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_database_backups AS
	SELECT
		MetadataStore.backup_metadata_uuid AS backup_file_id,
		-- For geo-secondary and point in time restored databases, the "database_guid" in "sys.backup_metadata_store" is not the logical database ID,
		-- so the logical database ID is taken from the "DM_XDB_DATABASE_BACKUP_CONFIG_SETTINGS" TVF
		BackupSettings.logical_database_id,
		MetadataStore.physical_database_name,
		CASE
			WHEN serverproperty('EngineEdition') != 12
			THEN MetadataStore.logical_server_name
			ELSE NULL
		END AS logical_server_name,
		-- For Trident Native DB truncate GUID(added by Renzo layer) at the end of the datbase name.
		CASE
			WHEN serverproperty('EngineEdition') = 12 and MetadataStore.logical_database_name LIKE '%-'+REPLACE('00000000-0000-0000-0000-000000000000', '0', '[0-9a-fA-F]')
			THEN substring(MetadataStore.logical_database_name, 1, PATINDEX('%-'+REPLACE('00000000-0000-0000-0000-000000000000', '0', '[0-9a-fA-F]'), logical_database_name)-1)
			ELSE MetadataStore.logical_database_name
		END AS logical_database_name,
		MetadataStore.backup_start_date,
		MetadataStore.backup_finish_date,
		MetadataStore.backup_type,
		CASE
			WHEN MetadataStore.backup_start_date >= DATEADD(day, -BackupSettings.backup_retention_days, SYSUTCDATETIME()) AND MetadataStore.backup_start_date >= BackupSettings.first_backup_time_for_current_retention
			THEN CAST(1 AS BIT)
			ELSE CAST(0 AS BIT)
		END as in_retention
	FROM sys.backup_metadata_store AS MetadataStore
	CROSS JOIN OpenRowset(TABLE DM_XDB_DATABASE_BACKUP_CONFIG_SETTINGS) AS BackupSettings
	WHERE MetadataStore.backup_start_date >=
	(
		-- If there are backups outside the retention period, then the first subquery will return the date/time for the most recent full backup that falls outside the retention period
		-- If there are no backups outside the retention period, then the first subquery returns no data and the 2nd subquery is executed which returns the oldest backup date/time in the retenion period
		COALESCE
		(
			(	SELECT MAX(backup_start_date)
				FROM sys.backup_metadata_store
				WHERE (backup_type = 'D')
					AND (backup_start_date <= DATEADD(day, -BackupSettings.backup_retention_days, SYSUTCDATETIME()) OR backup_start_date <= BackupSettings.first_backup_time_for_current_retention)
			),
			(SELECT MIN(backup_start_date) FROM sys.backup_metadata_store)
		)
	)
	-- There are scenarios where a database may have backup history data that belongs to another database. For example, a geo-secondary or point in time restore database may have backup history from the primary or source database.
	-- Since this view should only return the backup history data related to this specific logical database, the backup history data has to be filtered based on the logical database ID
	AND LOWER(MetadataStore.backup_path) LIKE '%' + COALESCE(LOWER(BackupSettings.logical_database_id), LOWER('')) + '%' -- If the "logical_database_id" is null, then "LOWER('')" ensures that there is no filter based on logical database ID


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_database_backups AS
	SELECT
		MetadataStore.backup_metadata_uuid AS backup_file_id,
		-- For geo-secondary and point in time restored databases, the "database_guid" in "sys.backup_metadata_store" is not the logical database ID,
		-- so the logical database ID is taken from the "DM_XDB_DATABASE_BACKUP_CONFIG_SETTINGS" TVF
		BackupSettings.logical_database_id AS database_guid,
		MetadataStore.physical_database_name,
		MetadataStore.server_name AS physical_server_name,
		MetadataStore.backup_start_date,
		MetadataStore.backup_finish_date,
		MetadataStore.backup_type,
		CASE
			WHEN MetadataStore.backup_start_date >= DATEADD(day, -BackupSettings.backup_retention_days, SYSUTCDATETIME()) AND MetadataStore.backup_start_date >= BackupSettings.first_backup_time_for_current_retention
			THEN CAST(1 AS BIT)
			ELSE CAST(0 AS BIT)
		END as in_retention
	FROM sys.backup_metadata_store AS MetadataStore
	CROSS JOIN OpenRowset(TABLE DM_XDB_DATABASE_BACKUP_CONFIG_SETTINGS) AS BackupSettings
	WHERE MetadataStore.backup_start_date >=
	(
		-- If there are backups outside the retention period, then the first subquery will return the date/time for the most recent full backup that falls outside the retention period
		-- If there are no backups outside the retention period, then the first subquery returns no data and the 2nd subquery is executed which returns the oldest backup date/time in the retenion period
		COALESCE
		(
			(	SELECT MAX(backup_start_date)
				FROM sys.backup_metadata_store
				WHERE (backup_type = 'D')
					AND (backup_start_date <= DATEADD(day, -BackupSettings.backup_retention_days, SYSUTCDATETIME()) OR backup_start_date <= BackupSettings.first_backup_time_for_current_retention)
			),
			(SELECT MIN(backup_start_date) FROM sys.backup_metadata_store)
		)
	)
	-- There are scenarios where a database may have backup history data that belongs to another database. For example, a geo-secondary or point in time restore database may have backup history from the primary or source database.
	-- Since this view should only return the backup history data related to this specific logical database, the backup history data has to be filtered based on the logical database ID
	AND LOWER(MetadataStore.backup_path) LIKE '%' + COALESCE(LOWER(BackupSettings.logical_database_id), LOWER('')) + '%' -- If the "logical_database_id" is null, then "LOWER('')" ensures that there is no filter based on logical database ID

