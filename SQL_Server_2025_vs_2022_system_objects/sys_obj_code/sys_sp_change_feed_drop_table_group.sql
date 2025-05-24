SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_feed_drop_table_group
(
	@table_group_id uniqueidentifier
)
as
	declare @retcode int
		,@logmessage nvarchar(4000)

	set @logmessage = concat(N'Executing internal proc. Table Group ID: ', @table_group_id)
	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_drop_table_group', @logmessage

	exec @retcode = sys.sp_synapse_link_drop_topic_internal @table_group_id = @table_group_id, @wait = 1

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end
	
	set @logmessage = concat(N'Completed internal proc. Table Group ID: ', @table_group_id,
		N'. Return code: ', @retcode,
		N'. Error code: ', @errcode)
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_drop_table_group', @logmessage
	
	return @status


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_feed_drop_table_group
(
	@table_group_id uniqueidentifier
)
as
	declare @ret int
	exec @ret = sys.sp_synapse_link_drop_topic_internal @table_group_id = @table_group_id, @wait = 1
	return @ret

