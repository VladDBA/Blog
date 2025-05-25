use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_feed_disable_db
as
begin
	declare @retcode int
			,@logmessage nvarchar(4000)

	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_disable_db', N'Executing internal proc'

	exec @retcode = sys.sp_synapse_link_disable_db_internal

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end
	
	set @logmessage = concat(N'Completed internal proc. Return code: ', @retcode,
		N'. Error code: ', @errcode)
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_disable_db', @logmessage
	
	return @status
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_feed_disable_db
as
begin
	declare @db_name sysname
			,@retcode int
			,@supported bit

	set @db_name = db_name()
	set @supported = [sys].[fn_synapse_link_is_supported]()

	-- Verify Synapse Link is supported for this server
	-- For Managed Instance we enable this stored procedure for system-level actions in MI Link (Hybrid) scenarios.
	if (@@ERROR <> 0 or (@supported = 0 and serverproperty('EngineEdition') <> 8))
	begin
		raiserror(22701, 16, 4)
		return (1)
	end

	if sys.fn_change_feed_is_enabled_for_current_db () = 0
	begin
		raiserror(22706, 16, 1, @db_name)
		return 1
	end

	IF (sys.fn_has_permission_run_changefeed() = 0)
	BEGIN
		RAISERROR(22702, 16, 1)
		RETURN (1)
	END

	exec @retcode = sys.sp_synapse_link_disable_db_internal

	if (@@error <> 0) or (@retcode <> 0)
    begin
		return 1
	end

	return 0
end

