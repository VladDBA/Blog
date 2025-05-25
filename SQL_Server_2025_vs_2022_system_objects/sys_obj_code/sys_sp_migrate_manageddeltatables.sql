use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_migrate_manageddeltatables
AS
	-- Allow only on Trident frontend instance
	EXEC sys.sp_ensure_trident_frontend

	SET NOCOUNT ON

	-- Migrate sys.sys_dw_physical_table_values -> sys.manageddeltatables if all the v1 tables needed for the migration exist
	IF (OBJECT_ID('sys.sys_dw_physical_table_values') IS NOT NULL AND OBJECT_ID('sys.sys_manifest_file_catalog_table') IS NOT NULL AND OBJECT_ID('sys.sys_clone_references') IS NOT NULL)
	BEGIN
		INSERT INTO sys.manageddeltatables
		(
			sql_object_id, table_guid, fork_guid, 
			clone_island_guid, clone_parent_guid, 
			delta_log_feature_status, 
			create_commit_time,
			drop_commit_time,
			table_classification
		)
		(
			SELECT deprecated.object_id, deprecated.physical_object_guid, deprecated.fork_guid,
					CASE 
						WHEN cr2.source_object_guid IS NOT NULL THEN cr2.source_object_guid 
						WHEN cr1.source_object_guid IS NOT NULL THEN cr1.source_object_guid
						ELSE deprecated.physical_object_guid
					END AS clone_island_guid,
					iif(cr1.source_object_guid IS NOT NULL, cr1.source_object_guid, '00000000-0000-0000-0000-000000000000'),
					deprecated.manifest_feature_status,
					(SELECT MIN(commit_timestamp) FROM sys.sys_manifest_file_catalog_table WHERE table_id = deprecated.object_id AND source_table_guid = '00000000-0000-0000-0000-000000000000'),
					'1753-01-01 00:00:00.000',
					0 -- 0 is the default, unknown table classification
			FROM 
				sys.sys_dw_physical_table_values deprecated 
				LEFT JOIN sys.sys_clone_references cr1 
					ON deprecated.object_id = cr1.target_object_id AND cr1.reference_rank = 0
				LEFT JOIN sys.sys_clone_references cr2 
					ON deprecated.object_id = cr2.target_object_id AND cr2.reference_rank = 2147483647
		)
	END

