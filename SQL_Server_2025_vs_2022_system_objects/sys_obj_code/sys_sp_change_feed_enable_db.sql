SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_feed_enable_db
(
	@maxtrans 	int = NULL,
	@pollinterval int = NULL,
	@destination_type int = 0
)
as
begin
	declare @retcode int
		,@logmessage nvarchar(4000)

	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_enable_db', N'Executing internal proc'

	exec @retcode = sys.sp_synapse_link_enable_db_internal @maxtrans, @pollinterval, @destination_type

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end
	
	set @logmessage = concat(N'Completed internal proc. Return code: ', @retcode, 
		N'. Error code: ', @errcode)
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_enable_db', @logmessage
	
	return @status
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_feed_enable_db
(
	@maxtrans 	int = NULL,
	@pollinterval int = NULL
)
as
begin
	declare @db_name sysname
		,@supported bit
		,@containment tinyint
		,@retcode int
		,@physical_db_name sysname

	set @db_name = db_name()

	-- Verify Synapse Link is supported for this server
	set @supported = [sys].[fn_synapse_link_is_supported]()
	IF (@@ERROR <> 0 or @supported = 0)
	BEGIN
		RAISERROR(22701, 16, 3)
		RETURN (1)
	END

	IF (sys.fn_has_permission_run_changefeed() = 0)
	BEGIN
		RAISERROR(22702, 16, 1)
		RETURN (1)
	END

	/*
    ** Contained Database check (Synapse Link is not yet supported on contained databases)
    ** If the current database is a contained database, then we error out.
    */
    SELECT @containment=containment FROM sys.databases WHERE
        name = @db_name
    if (@containment != 0)
    BEGIN
        set @db_name = db_name()
        RAISERROR(12839, 16, 2, @db_name)
        RETURN(1)
	END

	if (@maxtrans is null)
		set @maxtrans = 500
	else if (@maxtrans <= 0)
	BEGIN
		RAISERROR(22713, 16, -1)
		RETURN(1)
	END

	if (@pollinterval is null)
		set @pollinterval = 5
	else if (@pollinterval < 5)
	BEGIN
		RAISERROR(22767, 16, -1)
		RETURN(1)
	END

	exec @retcode = sys.sp_synapse_link_enable_db_internal @maxtrans, @pollinterval

	if (@@error <> 0) or (@retcode <> 0)
	begin
		return 1
	end

	return 0
end

