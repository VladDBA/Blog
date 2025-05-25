use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE PROCEDURE [sys].[sp_help_fabric_mirroring_table_groups]
AS
BEGIN
	DECLARE @db_name sysname = db_name(),
			@metadata_schema_name nvarchar(10)

	-- Return if Fabric Link is not enabled.
	IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = @db_name AND is_change_feed_enabled = 1 AND is_data_lake_replication_enabled = 1)
	BEGIN
		RAISERROR(22706, 16, 5, @db_name)
		RETURN 1
	END

	-- Get the metadata table schema name (sys or changefeed) based on the TridentOneLakeLinkUseInternalTablesForMetadata feature switch value.
	EXEC sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT
	IF(@metadata_schema_name = 'changefeed')
	BEGIN
		SELECT table_group_id,
			   table_group_name,
			   destination_location,
			   destination_credential,
			   workspace_id,
			   synapse_workgroup_name,
			   enabled,
			   destination_type,
			   max_message_size_bytes,
			   partition_scheme
		FROM changefeed.change_feed_table_groups
	END
	ELSE
	BEGIN
		SELECT table_group_id,
			   table_group_name,
			   destination_location,
			   destination_credential,
			   workspace_id,
			   synapse_workgroup_name,
			   enabled,
			   destination_type,
			   max_message_size_bytes,
			   partition_scheme
		FROM sys.change_feed_table_groups
	END

	RETURN 0
END

