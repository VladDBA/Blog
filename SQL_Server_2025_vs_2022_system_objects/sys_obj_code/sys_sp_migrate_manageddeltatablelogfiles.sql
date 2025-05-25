SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_migrate_manageddeltatablelogfiles
@dw_artifact_id UNIQUEIDENTIFIER
AS
	-- Allow only on Trident frontend instance
	EXEC sys.sp_ensure_trident_frontend

	SET NOCOUNT ON

	-- Migrate sys.sys_manifest_file_catalog_table -> sys.manageddeltatablelogfiles if all the v1 tables needed for the migration exist
	IF (OBJECT_ID('sys.sys_manifest_file_catalog_table') IS NOT NULL)
	BEGIN
		
		SET IDENTITY_INSERT sys.manageddeltatablelogfiles ON
		INSERT INTO sys.manageddeltatablelogfiles
		(
			table_id, commit_sequence_id, file_guid, xdes_ts, append_only, source_database_guid, source_table_guid,	rows_inserted, commit_time
		)
		(
			SELECT
				mdt.table_id, 
				deprecated.commit_sequence_id, 
				deprecated.file_id, 
				deprecated.xdes_ts, 
				append_only, 
				iif(deprecated.source_database_guid = '00000000-0000-0000-0000-000000000000', 
					@dw_artifact_id, 
					deprecated.source_database_guid),
				iif(deprecated.source_table_guid = '00000000-0000-0000-0000-000000000000', 
					iif(@dw_artifact_id = '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', mdt.table_guid), 
					deprecated.source_table_guid),
				deprecated.rows_inserted, 
				deprecated.commit_timestamp
			FROM
				sys.sys_manifest_file_catalog_table deprecated
				JOIN sys.manageddeltatables mdt
				ON (deprecated.table_id = mdt.sql_object_id AND mdt.drop_commit_time <= '1900-01-01 00:00:00.000')
		)
		SET IDENTITY_INSERT sys.manageddeltatablelogfiles OFF
	END

