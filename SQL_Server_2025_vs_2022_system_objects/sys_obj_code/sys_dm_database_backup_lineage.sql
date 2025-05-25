use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_database_backup_lineage AS
	SELECT
		MetadataStore.backup_metadata_uuid AS backup_file_id,
		-- For geo-secondary and point in time restored databases, the "database_guid" in "sys.backup_metadata_store" is not the logical database ID,
		-- so the logical database ID is taken from the "DM_XDB_DATABASE_BACKUP_CONFIG_SETTINGS" TVF
		BackupSettings.logical_database_id,
		MetadataStore.logical_server_name,
		MetadataStore.logical_database_name,
		MetadataStore.backup_start_date,
		MetadataStore.backup_finish_date,
		MetadataStore.backup_type,
		(MetadataStore.allocated_data_size_bytes / 1024 / 1024) AS database_allocated_storage_mb
	FROM sys.backup_metadata_store AS MetadataStore
	CROSS JOIN OpenRowset(TABLE DM_XDB_DATABASE_BACKUP_CONFIG_SETTINGS) AS BackupSettings
	WHERE MetadataStore.backup_start_date >= DATEADD(day, -35, SYSUTCDATETIME())

