use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_migrate_manageddeltatablecheckpoints
@dw_artifact_id UNIQUEIDENTIFIER
AS
	-- Allow only on Trident frontend instance
	EXEC sys.sp_ensure_trident_frontend

	SET NOCOUNT ON
	
	-- Migrate sys.sys_checkpoint_file_catalog_table -> sys.manageddeltatablecheckpoints if all the v1 tables needed for the migration exist
	IF (OBJECT_ID('sys.sys_checkpoint_file_catalog_table') IS NOT NULL AND OBJECT_ID('sys.sys_manifest_file_catalog_table') IS NOT NULL)
	BEGIN

		INSERT INTO sys.manageddeltatablecheckpoints
		(
			table_id, delta_log_commit_sequence_id, part, file_guid, version, source_database_guid, source_table_guid, 
			xdes_ts, commit_time, delta_log_xdes_ts, delta_log_commit_time
		)
		(
			SELECT
					mdt.table_id,
					deprecated.commit_sequence_id,
					deprecated.part,
					deprecated.file_id,
					deprecated.version,
					iif(deprecated.source_database_guid = '00000000-0000-0000-0000-000000000000', 
						@dw_artifact_id, 
						deprecated.source_database_guid),
					iif(deprecated.source_table_guid = '00000000-0000-0000-0000-000000000000', 
						iif(@dw_artifact_id = '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', mdt.table_guid), 
						deprecated.source_table_guid),
					0,
					'1753-01-01 00:00:00.000',
					deprecated_manifest_catalog.xdes_ts,
					deprecated_manifest_catalog.commit_timestamp
				FROM
					sys.sys_checkpoint_file_catalog_table deprecated
					JOIN sys.manageddeltatables mdt
					ON deprecated.table_id = mdt.sql_object_id AND mdt.drop_commit_time <= '1900-01-01 00:00:00.000'
					JOIN sys.sys_manifest_file_catalog_table deprecated_manifest_catalog
					ON deprecated.table_id = deprecated_manifest_catalog.table_id AND deprecated.commit_sequence_id = deprecated_manifest_catalog.commit_sequence_id
		)
	END

