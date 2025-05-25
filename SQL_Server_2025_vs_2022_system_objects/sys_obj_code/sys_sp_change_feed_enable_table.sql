SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_feed_enable_table
(
	@table_group_id uniqueidentifier,
	@table_id uniqueidentifier,
	@source_schema sysname,
	@source_name sysname
)
as
begin
	set nocount on

	declare	@retcode int
			,@logmessage nvarchar(4000)

	set @logmessage = concat(N'Executing internal proc. Table Group ID: ', @table_group_id, 
		N'. Table ID: ', @table_id)
	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_enable_table', @logmessage

	-- Call internal stored procedure that executes as 'dbo' to do the work.	
	exec @retcode = sys.sp_synapse_link_enable_table_internal 
		@table_group_id,
		@table_id,
		@source_schema,
		@source_name

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end
	
	set @logmessage = concat(N'Completed internal proc. Table Group ID: ', @table_group_id, 
		N'. Table ID: ', @table_id, 
		N'. Return code: ', @retcode, 
		N'. Error code: ', @errcode)
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_enable_table', @logmessage
	
	return @status
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_feed_enable_table
(
	@table_group_id uniqueidentifier,
	@table_id uniqueidentifier,
	@source_schema sysname,
	@source_name sysname
)
as
begin
	set nocount on

	declare	@supported bit
			,@db_name sysname
			,@retcode int
	
	set @db_name = db_name()

	-- Verify Synapse Link is supported for this server
	set @supported = [sys].[fn_synapse_link_is_supported]()
	IF (@@ERROR <> 0 or @supported = 0)
	BEGIN
		RAISERROR(22701, 16, 4)
		RETURN (1)
	END

	IF (sys.fn_has_permission_run_changefeed() = 0)
	BEGIN
		RAISERROR(22702, 16, 1)
		RETURN (1)
	END

	-- This does not need to use the fn_change_feed_is_enabled_for_current_db because this should never be called directly on the sql Azure instance
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 1, @db_name)
		return 1
	end

	declare @vupgraderesult int
	exec @vupgraderesult = [sys].[sp_change_feed_vupgrade]
	if (@vupgraderesult != 0)
	begin
		return 1
	end

	-- Call internal stored procedure that executes as 'dbo' to do the work.	
	exec @retcode = sys.sp_synapse_link_enable_table_internal 
		@table_group_id,
		@table_id,
		@source_schema,
		@source_name

	if (@@error <> 0) or (@retcode <> 0)
	begin
		return 1
	end

	return 0
end

