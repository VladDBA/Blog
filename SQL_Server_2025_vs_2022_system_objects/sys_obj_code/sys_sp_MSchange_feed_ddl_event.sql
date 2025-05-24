SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_MSchange_feed_ddl_event
(
	@EventData xml
)
as
begin
	declare @retcode int

	-- if Synapse Link is not enabled for this db, don't do anything
	-- This does not need to use the fn_change_feed_is_enabled_for_current_db because this should never be called directly on the sql Azure instance
	IF (@@ERROR <> 0
		or not exists (select * from sys.databases where name = db_name() and is_change_feed_enabled = 1))
	BEGIN
		RETURN 0
	END

	exec @retcode = sys.sp_synapse_link_ddl_event_internal @EventData 

	if (@@error <> 0) or (@retcode <> 0)
	begin
		return 1
	end	
	
    return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_MSchange_feed_ddl_event
(
	@EventData xml
)
as
begin
	declare @retcode int
		,@supported bit

	-- if Synapse Link is not enabled for this db, don't do anything
	-- This does not need to use the fn_change_feed_is_enabled_for_current_db because this should never be called directly on the sql Azure instance
	IF (@@ERROR <> 0
		or not exists (select * from sys.databases where name = db_name() and is_change_feed_enabled = 1))
	BEGIN
		RETURN 0
	END

	exec @retcode = sys.sp_synapse_link_ddl_event_internal @EventData 

	if (@@error <> 0) or (@retcode <> 0)
	begin
		return 1
	end	
	
    return 0
end

