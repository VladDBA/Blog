use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_help_change_feed
as
begin

	declare @retcode int,
			@db_name sysname,
			@metadata_schema_name nvarchar(10),
			@stmt nvarchar(max),
			@is_synapse_link bit,
			@is_trident_link bit,
			@is_change_event_streaming bit

	set @db_name = db_name()
	set @retcode = 0
	set @is_change_event_streaming = sys.fn_ces_is_enabled_for_current_db()
	set @is_trident_link = sys.fn_trident_link_is_enabled_for_current_db()
	set @is_synapse_link = IIF(sys.fn_change_feed_is_enabled_for_current_db() = 1 and @is_trident_link = 0 and @is_change_event_streaming = 0, 1, 0)

	-- Return if Change Feed is not enabled.
	-- This does not need to use the fn_change_feed_is_enabled_for_current_db because this should never be called directly on the sql Azure instance.
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 1, @db_name)
		set @retcode = 1
	end

	-- Get the metadata table schema name based on the feature enabled. Changefeed for Synapse Link and Change Event Streaming, sys for Trident link.
	exec sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT

	declare @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled int
	exec @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkUseHelperProcsToExposeReseedId'
	declare @exposed_workspace_id sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N'table_group.workspace_id as workspace_id,', N'')
	declare @exposed_synapse_workgroup_name sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N'table_group.synapse_workgroup_name as synapse_workgroup_name,', N'')
	declare @exposed_partition_column_name sysname = IIF(@is_change_event_streaming = 1, N'table_group.partition_column_name as partition_column_name,', N'')
	declare @exposed_encoding sysname = IIF(@is_change_event_streaming = 1, N'table_group.encoding as encoding,', N'')
	declare @exposed_streaming_dest_type sysname = IIF(@is_change_event_streaming = 1, N'table_group.streaming_dest_type as streaming_dest_type,', N'')
	declare @exposed_snapshot_phase sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N', sl_table.snapshot_phase as snapshot_phase', N'')
	declare @exposed_snapshot_current_phase_time sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N', sl_table.snapshot_current_phase_time as snapshot_current_phase_time', N'')
	declare @exposed_snapshot_retry_count sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N', sl_table.snapshot_retry_count as snapshot_retry_count', N'')
	declare @exposed_snapshot_start_time sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N', sl_table.snapshot_start_time as snapshot_start_time', N'')
	declare @exposed_snapshot_end_time sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N', sl_table.snapshot_end_time as snapshot_end_time', N'')
	declare @exposed_snapshot_row_count sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N', sl_table.snapshot_row_count as snapshot_row_count', N'')
	declare @exposed_reseed_id sysname = IIF(@is_trident_link = 1 and @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled = 1, N', sl_table.reseed_id as reseed_id', N'')
	declare @exposed_include_old_values sysname = IIF(@is_change_event_streaming = 1, N', sl_table.include_old_values as include_old_values', N'')
	declare @exposed_include_all_columns sysname = IIF(@is_change_event_streaming = 1, N', sl_table.include_all_columns as include_all_columns', N'')
	declare @exposed_include_old_lob_values sysname = IIF(@is_change_event_streaming = 1, N', sl_table.include_old_lob_values as include_old_lob_values', N'')

	if (object_id('' + @metadata_schema_name + '.change_feed_table_groups') is not null and object_id('' + @metadata_schema_name + '.change_feed_tables') is not null)
	begin
		set @stmt = 
			N'
			select
				table_group.table_group_id as table_group_id,
				table_group.table_group_name as table_group_name,
				table_group.destination_location as destination_location,
				table_group.destination_credential as destination_credential,
				table_group.destination_type as destination_type,'
				+ @exposed_workspace_id
				+ @exposed_synapse_workgroup_name
				+ N'table_group.enabled as enabled,
				table_group.max_message_size_bytes as max_message_size_bytes,
				table_group.partition_scheme as partition_scheme,'
				+ @exposed_partition_column_name
				+ @exposed_encoding
				+ @exposed_streaming_dest_type
				+ N'object_schema_name(sl_table.object_id) as schema_name,
				object_name(sl_table.object_id) as table_name,
				sl_table.table_id as table_id,
				sl_table.object_id as table_object_id,
				sl_table.state as state,
				sl_table.version as version,
				sl_table.enable_lsn as enable_lsn,
				sl_table.disable_lsn as disable_lsn'
				+ @exposed_snapshot_phase
				+ @exposed_snapshot_current_phase_time
				+ @exposed_snapshot_retry_count
				+ @exposed_snapshot_start_time
				+ @exposed_snapshot_end_time
				+ @exposed_snapshot_row_count
				+ @exposed_reseed_id
				+ @exposed_include_old_values
				+ @exposed_include_all_columns
				+ @exposed_include_old_lob_values + N'
			from ' + @metadata_schema_name + '.change_feed_table_groups table_group join ' + @metadata_schema_name + '.change_feed_tables sl_table
			on table_group.table_group_id = sl_table.table_group_id'
		exec sp_executesql @stmt
	end

	return @retcode
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_help_change_feed
as
begin

	declare @retcode int
		,@db_name sysname

	set @db_name = db_name()
	set @retcode = 0

	-- This does not need to use the fn_change_feed_is_enabled_for_current_db because this should never be called directly on the sql Azure instance
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 1, @db_name)
		set @retcode = 1
	end

	if (object_id('changefeed.change_feed_table_groups') is not null and object_id('changefeed.change_feed_tables') is not null)
	begin
		select
			table_group.table_group_id as table_group_id,
			table_group.table_group_name as table_group_name,
			table_group.destination_location as destination_location,
			table_group.destination_credential as destination_credential,
			table_group.workspace_id as workspace_id,
			table_group.synapse_workgroup_name as synapse_workgroup_name,
			object_schema_name(sl_table.object_id) as schema_name,
			object_name(sl_table.object_id) as table_name,
			sl_table.table_id as table_id,
			sl_table.object_id as table_object_id,
			sl_table.state as state,
			sl_table.version as version,
			sl_table.snapshot_phase as snapshot_phase,
			sl_table.snapshot_current_phase_time as snapshot_current_phase_time,
			sl_table.snapshot_retry_count as snapshot_retry_count,
			sl_table.snapshot_start_time as snapshot_start_time,
			sl_table.snapshot_end_time as snapshot_end_time,
			sl_table.snapshot_row_count as snapshot_row_count
		from changefeed.change_feed_table_groups table_group join changefeed.change_feed_tables sl_table
		on table_group.table_group_id = sl_table.table_group_id
	end

	return @retcode
end

