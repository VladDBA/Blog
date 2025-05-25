use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_migrate_manageddeltatableforks
@dw_artifact_id UNIQUEIDENTIFIER
AS
	-- Allow only on Trident frontend instance
	EXEC sys.sp_ensure_trident_frontend

	SET NOCOUNT ON
	
	-- Migrate sys.sys_dw_physical_table_fork_catalog -> sys.manageddeltatableforks if all the v1 tables needed for the migration exist
	IF (OBJECT_ID('sys.sys_dw_physical_table_fork_catalog') IS NOT NULL)
	BEGIN

		SET IDENTITY_INSERT sys.manageddeltatableforks ON
		INSERT INTO sys.manageddeltatableforks
		(
			commit_sequence_id,	table_id, fork_guid, source_database_guid, source_table_guid, xdes_ts, commit_time
		)
		(
			SELECT
				deprecated.commit_sequence_id,
				mdt.table_id,
				deprecated.fork_guid,
				iif(deprecated.source_database_guid = '00000000-0000-0000-0000-000000000000', 
					@dw_artifact_id, 
					deprecated.source_database_guid),
				iif(deprecated.source_table_guid = '00000000-0000-0000-0000-000000000000', 
					iif(@dw_artifact_id = '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', mdt.table_guid), 
					deprecated.source_table_guid),
				deprecated.xdes_ts, 
				deprecated.commit_timestamp
			FROM
				sys.sys_dw_physical_table_fork_catalog deprecated
				JOIN sys.manageddeltatables mdt
				ON deprecated.table_id = mdt.sql_object_id AND mdt.drop_commit_time <= '1900-01-01 00:00:00.000'
		)
		SET IDENTITY_INSERT sys.manageddeltatableforks OFF
	END

