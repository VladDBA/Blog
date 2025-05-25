use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure sys.sp_change_feed_enable_table_after_reseed
(
	@table_group_id uniqueidentifier,
	@table_id uniqueidentifier
)
as
	declare @is_trident_publishing_table_reseed_enabled int
	exec @is_trident_publishing_table_reseed_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkPublishingTableReseed'

	if (@is_trident_publishing_table_reseed_enabled = 0)
	begin
		raiserror(22607, 16, 5, N'Table-Reseed')
		return 22607
	end

	if([sys].[fn_trident_link_is_in_backend_connection_mode]() <> 1)
	begin
		raiserror(22607, 16, 6, N'Table-Reseed')
		return 22607
	end

	declare @db_name sysname = db_name()
	if (sys.fn_trident_link_is_enabled_for_current_db() <> 1)
	begin
		RAISERROR(22604, 16, 8, @db_name)
		RETURN (22604)
	end
	
	declare @retcode int
		,@logmessage nvarchar(4000)

	set @logmessage = concat(N'Executing internal proc. Table Group ID: ', @table_group_id, 
		N'. Table ID: ', @table_id)
	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_enable_table_after_reseed', @logmessage

	exec @retcode = sys.sp_change_feed_enable_table_after_reseed_internal @table_group_id = @table_group_id, @table_id = @table_id

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end

	set @logmessage = concat(N'Completed internal proc. Table Group ID: ', @table_group_id, 
		N'. Table ID: ', @table_id,
		N'. Return code: ', @retcode,
		N'. Error code: ', @errcode)
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_enable_table_after_reseed', @logmessage
	
	return @status

