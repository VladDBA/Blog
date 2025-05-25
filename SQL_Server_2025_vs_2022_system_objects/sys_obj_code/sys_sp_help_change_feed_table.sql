use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure [sys].[sp_help_change_feed_table]
(
	@table_group_id uniqueidentifier = null,
	@table_id uniqueidentifier = null,
	@source_schema sysname = 'dbo',
	@source_name sysname = null
)
as
begin
	declare @stmt nvarchar(max),
		@id_params bit = 0,
		@source_object_id int,
		@source_table nvarchar(1000),
		@metadata_schema_name nvarchar(10),
		@db_name sysname,
		@is_synapse_link bit,
		@is_trident_link bit,
		@is_change_event_streaming bit

	set @db_name = db_name()
	set @is_change_event_streaming = sys.fn_ces_is_enabled_for_current_db()
	set @is_trident_link = sys.fn_trident_link_is_enabled_for_current_db()
	set @is_synapse_link = IIF(sys.fn_change_feed_is_enabled_for_current_db() = 1 and @is_trident_link = 0 and @is_change_event_streaming = 0, 1, 0)

	-- Return if Change Feed is not enabled.
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 1, @db_name)
		return 1
	end

	-- Get the metadata table schema name based on the feature enabled. Changefeed for Synapse Link and Change Event Streaming, sys for Trident link.
	exec sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT

	if (@table_group_id is not null and @table_id is not null)
	begin
		set @id_params = 1
		declare @result_count int = 0;

		set @stmt = 
			N'
			SELECT @result_count = count(*)
			FROM ' + @metadata_schema_name + '.change_feed_tables INNER JOIN ' + @metadata_schema_name + '.change_feed_table_groups 
			ON ' + @metadata_schema_name + '.change_feed_table_groups.table_group_id = ' + @metadata_schema_name + '.change_feed_tables.table_group_id
			WHERE ' + @metadata_schema_name +  '.change_feed_tables.table_id = ''' + cast(@table_id as nvarchar(max)) + '''
				and ' + @metadata_schema_name + '.change_feed_tables.table_group_id = ''' + cast(@table_group_id as nvarchar(max)) + ''''
			
		exec sp_executesql @stmt, N'@result_count int output', @result_count = @result_count output;

		if @result_count = 0
		begin
			raiserror(22769, 16, -1)
			return 1
		end
	end
	else if (@source_name is not null)
	begin
		select @source_schema = ISNULL(@source_schema, 'dbo')
		select @source_table = quotename(@source_schema) + N'.' + quotename(@source_name)
		select @source_object_id = object_id(@source_table)

		if (@source_object_id is null)
		or not exists (select *
			from sys.tables
			where object_id = @source_object_id and is_ms_shipped = 0)
		begin
			raiserror(22769, 16, -1)
			return 1
		end
	end
	else
	begin
		raiserror(22770, 16, -1)
		return 1
	end

	declare @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled int
	exec @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkUseHelperProcsToExposeReseedId'
	declare @exposed_workspace_id sysname = IIF(@is_synapse_link = 1 or @is_trident_link = 1, N'table_group.workspace_id as workspace_id,', N'')
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

	set @stmt =
		N'
		select
			table_group.table_group_id as table_group_id,
			table_group.table_group_name as table_group_name,
			object_schema_name(sl_table.object_id) as schema_name,
			object_name(sl_table.object_id) as table_name,
			sl_table.table_id as table_id,
			table_group.destination_location as destination_location,'
			+ @exposed_workspace_id
			+ N'sl_table.state as [state],
			sl_table.object_id as table_object_id,
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
		on table_group.table_group_id = sl_table.table_group_id and '

	if (@id_params = 1)
	begin
		set @stmt += 'sl_table.table_id = @t_id and sl_table.table_group_id = @tg_id'
		exec sp_executesql @stmt, N'@t_id uniqueidentifier, @tg_id uniqueidentifier', @t_id = @table_id, @tg_id = @table_group_id;
	end
	else if (@id_params <> 1)
	begin
		set @stmt += 'sl_table.object_id = @object_id'
		exec sp_executesql @stmt, N'@object_id int', @object_id = @source_object_id;
	end

	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure [sys].[sp_help_change_feed_table]
(
	@table_group_id uniqueidentifier = null
	,@table_id uniqueidentifier = null
	,@source_schema sysname = 'dbo'
	,@source_name sysname = null
)
as
begin
	declare @stmt nvarchar(max)
		,@id_params bit
		,@source_object_id int
		,@source_table nvarchar(1000)

	if (object_id('changefeed.change_feed_table_groups') is null and object_id('changefeed.change_feed_tables') is null)
	begin
		raiserror(22710, 16, -1)
        return 1
	end

	if (@table_group_id is not null and @table_id is not null)
	begin
		set @id_params = 1

		if not exists (select 1 from changefeed.change_feed_table_groups where table_group_id = @table_group_id)
			or not exists (select 1 from changefeed.change_feed_tables where table_id = @table_id and table_group_id = @table_group_id)
		begin
			raiserror(22769, 16, -1)
			return 1
		end
	end
	else if (@source_name is not null)
	begin
		select @source_schema = ISNULL(@source_schema, 'dbo')
		select @source_table = quotename(@source_schema) + N'.' + quotename(@source_name)
		select @source_object_id = object_id(@source_table)

		if (@source_object_id is null)
		or not exists (select *
			from sys.tables
			where object_id = @source_object_id and is_ms_shipped = 0)
		begin
			raiserror(22769, 16, -1)
			return 1
		end
	end
	else
	begin
		raiserror(22770, 16, -1)
		return 1
	end

	set @stmt =
		N'
		select
			table_group.table_group_id as table_group_id,
			table_group.table_group_name as table_group_name,
			object_schema_name(sl_table.object_id) as schema_name,
			object_name(sl_table.object_id) as table_name,
			sl_table.table_id as table_id,
			table_group.destination_location as destination_location,
			table_group.workspace_id as workspace_id,
			sl_table.state as [state],
			sl_table.object_id as table_object_id
		from changefeed.change_feed_table_groups table_group join changefeed.change_feed_tables sl_table
		on table_group.table_group_id = sl_table.table_group_id and '

	if (@id_params = 1)
	begin
		set @stmt += 'sl_table.table_id = @t_id and sl_table.table_group_id = @tg_id'
		exec sp_executesql @stmt, N'@t_id uniqueidentifier, @tg_id uniqueidentifier', @t_id = @table_id, @tg_id = @table_group_id;
	end
	else if (@id_params <> 1)
	begin
		set @stmt += 'sl_table.object_id = @object_id'
		exec sp_executesql @stmt, N'@object_id int', @object_id = @source_object_id;
	end

	return 0
end

