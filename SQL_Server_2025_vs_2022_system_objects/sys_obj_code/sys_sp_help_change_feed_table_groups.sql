SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure [sys].[sp_help_change_feed_table_groups]
as
begin
	declare @db_name sysname = db_name(),
		@metadata_schema_name nvarchar(10),
		@is_allow_helper_sprocs_for_SL_enabled int,
		@is_synapse_link bit,
		@is_trident_link bit,
		@is_change_event_streaming bit,
		@check_to_use_internal_tables_for_ces bit

	set @is_change_event_streaming = sys.fn_ces_is_enabled_for_current_db()
	set @is_trident_link = sys.fn_trident_link_is_enabled_for_current_db()
	set @is_synapse_link = IIF(sys.fn_change_feed_is_enabled_for_current_db() = 1 and @is_trident_link = 0 and @is_change_event_streaming = 0, 1, 0)
	exec sys.sp_change_streams_check_to_use_internal_tables @check_to_use_internal_tables_for_ces output

	-- Return if Change Feed is not enabled.
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 2, @db_name)
		return 1
	end

	exec @is_allow_helper_sprocs_for_SL_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkAllowNewHelperSprocsForChangeFeed'
	if (@is_allow_helper_sprocs_for_SL_enabled = 1)
	begin
		-- Get the metadata table schema name based on the feature enabled. Changefeed for Synapse Link and Change Event Streaming, sys for Trident link.
		exec sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT
		if(@metadata_schema_name = 'changefeed')
		begin
			if (@is_change_event_streaming = 1 and @is_trident_link = 1)
			begin
				select * from changefeed.change_feed_table_groups
			end
			else if (@is_synapse_link = 1 or @is_trident_link = 1)
			begin
				select table_group_id,
					   table_group_name,
					   destination_location,
					   destination_credential,
					   workspace_id,
					   synapse_workgroup_name,
					   enabled,
					   destination_type,
					   max_message_size_bytes,
					   partition_scheme
				from changefeed.change_feed_table_groups
			end
			else if (@is_change_event_streaming = 1)
			begin
				select table_group_id,
					   table_group_name,
					   destination_location,
					   destination_credential,
					   enabled,
					   destination_type,
					   max_message_size_bytes,
					   partition_scheme,
					   partition_column_name,
					   encoding,
					   streaming_dest_type
				from changefeed.change_feed_table_groups
			end
			return 0
		end
	end

	if (@is_trident_link = 1 and @check_to_use_internal_tables_for_ces = 1 and @is_change_event_streaming = 1)
	begin
		select * from sys.change_feed_table_groups
	end
	else if (@is_trident_link = 1)
	begin
		select table_group_id,
			   table_group_name,
			   destination_location,
			   destination_credential,
			   workspace_id,
			   synapse_workgroup_name,
			   enabled,
			   destination_type,
			   max_message_size_bytes,
			   partition_scheme
		from sys.change_feed_table_groups
	end
	else if (@check_to_use_internal_tables_for_ces = 1 and @is_change_event_streaming = 1)
	begin
		declare @stmt_to_exec nvarchar(max)
		set @stmt_to_exec = N'
			select table_group_id,
				   table_group_name,
				   destination_location,
				   destination_credential,
				   enabled,
				   destination_type,
				   max_message_size_bytes,
				   partition_scheme,
				   partition_column_name,
				   encoding,
				   streaming_dest_type
			from sys.change_feed_table_groups'
		exec sp_executesql @stmt_to_exec
		
	end
	return 0
end

