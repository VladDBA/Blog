SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure sys.sp_change_feed_reseed_table
(
	@table_group_id uniqueidentifier
	,@table_id uniqueidentifier
	,@reseed_id nvarchar(36)
)
as
	set nocount on

	declare @is_trident_publishing_table_reseed_enabled int
	exec @is_trident_publishing_table_reseed_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkPublishingTableReseed'

	if (@is_trident_publishing_table_reseed_enabled = 0)
	begin
		raiserror(22607, 16, 3, N'Table-Reseed')
		return 22607
	end

	declare @sal_return INT
	exec @sal_return =  sys.sp_trident_native_sal_raise_error_if_needed "sp_change_feed_reseed_table"
	if @sal_return <> 0
	BEGIN
		RETURN @sal_return
	END

	declare @db_name sysname = db_name()
	if (sys.fn_trident_link_is_enabled_for_current_db() <> 1)
	begin
		RAISERROR(22604, 16, 4, @db_name)
		RETURN (22604)
	end
	
	declare @retcode int
		,@logmessage nvarchar(4000)

	set @logmessage = concat(N'Executing internal proc. Table Group ID: ', @table_group_id, 
		N'. Table ID: ', @table_id)
	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_reseed_table', @logmessage

	exec @retcode = sys.sp_change_feed_reseed_table_internal @table_group_id = @table_group_id, @table_id = @table_id, @reseed_id = @reseed_id

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end

	set @logmessage = concat(N'Completed internal proc. Table Group ID: ', @table_group_id, 
		N'. Table ID: ', @table_id,
		N'. Return code: ', @retcode,
		N'. Error code: ', @errcode)
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_reseed_table', @logmessage
	
	return @status

