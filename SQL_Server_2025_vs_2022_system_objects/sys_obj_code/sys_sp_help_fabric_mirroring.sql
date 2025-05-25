use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE PROCEDURE sys.sp_help_fabric_mirroring
AS
BEGIN
	DECLARE @retcode int,
			@db_name sysname,
			@metadata_schema_name nvarchar(10),
			@stmt nvarchar(max)

	SET @db_name = db_name()
	SET @retcode = 0

	-- Return if Fabric Link is not enabled.
	IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = @db_name AND is_change_feed_enabled = 1 AND is_data_lake_replication_enabled = 1)
	BEGIN
		RAISERROR(22706, 16, 7, @db_name)
		SET @retcode = 1
	END

	-- Get the metadata table schema name (sys or changefeed) based on the TridentOneLakeLinkUseInternalTablesForMetadata feature switch value.
	EXEC sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT

	DECLARE @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled int
	EXEC @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkUseHelperProcsToExposeReseedId'
	declare @exposed_reseed_id sysname = IIF(sys.fn_trident_link_is_enabled_for_current_db() <> 1 OR @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled <> 1, N'', N', sl_table.reseed_id AS reseed_id')

	IF (OBJECT_ID('' + @metadata_schema_name + '.change_feed_table_groups') IS NOT NULL AND OBJECT_ID('' + @metadata_schema_name + '.change_feed_tables') IS NOT NULL)
	BEGIN
		SET @stmt = 
			N'
			SELECT
				table_group.table_group_id AS table_group_id,
				table_group.table_group_name AS table_group_name,
				table_group.destination_location AS destination_location,
				table_group.destination_credential AS destination_credential,
				table_group.destination_type AS destination_type,
				table_group.workspace_id AS workspace_id,
				table_group.synapse_workgroup_name AS synapse_workgroup_name,
				table_group.enabled AS enabled,
				table_group.max_message_size_bytes AS max_message_size_bytes,
				table_group.partition_scheme AS partition_scheme,
				object_schema_name(sl_table.object_id) AS schema_name,
				object_name(sl_table.object_id) AS table_name,
				sl_table.table_id AS table_id,
				sl_table.object_id AS table_object_id,
				sl_table.state AS state,
				sl_table.version AS version,
				sl_table.enable_lsn AS enable_lsn,
				sl_table.disable_lsn AS disable_lsn,
				sl_table.snapshot_phase AS snapshot_phase,
				sl_table.snapshot_current_phase_time AS snapshot_current_phase_time,
				sl_table.snapshot_retry_count AS snapshot_retry_count,
				sl_table.snapshot_start_time AS snapshot_start_time,
				sl_table.snapshot_end_time AS snapshot_end_time,
				sl_table.snapshot_row_count AS snapshot_row_count' + @exposed_reseed_id + N'
			FROM ' + @metadata_schema_name + '.change_feed_table_groups table_group JOIN ' + @metadata_schema_name + '.change_feed_tables sl_table
			ON table_group.table_group_id = sl_table.table_group_id'
		EXEC sp_executesql @stmt
	END

	RETURN @retcode
END

