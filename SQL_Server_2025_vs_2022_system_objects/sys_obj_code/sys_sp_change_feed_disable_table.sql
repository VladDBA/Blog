use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_feed_disable_table
(
	@table_group_id uniqueidentifier
	,@table_id uniqueidentifier
)
as
	declare @retcode int

	exec @retcode = sys.sp_change_feed_disable_table_internal @table_group_id = @table_group_id, @table_id = @table_id, @wait = 1

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 or @retcode <> 0 then 1 else 0 end

	return @status


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_feed_disable_table
(
	@table_group_id uniqueidentifier
	,@table_id uniqueidentifier
)
as
	declare @ret int
	exec @ret = sys.sp_synapse_link_disable_table_internal @table_group_id = @table_group_id, @table_id = @table_id, @wait = 1
	return @ret

