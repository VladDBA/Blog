use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure [sys].[sp_cdc_enable_db]
(
	@fCreateCDCUserImplicit bit = NULL
)
as
begin
	declare @retcode int
			,@containment tinyint
			,@db_name sysname
			,@user_privileges_needed nvarchar(1000)

    -- Verify CDC is supported for this SQL Server edition
    IF ([sys].[fn_cdc_is_supported]() = 1)
    BEGIN
		IF (serverproperty('EngineEdition') = 12)
		BEGIN
			RAISERROR(22607, 16, -1, 'Change Data Capture (CDC) is')
		END
		ELSE
		BEGIN
			DECLARE @edition sysname
			SELECT @edition = CONVERT(sysname, SERVERPROPERTY('Edition'))
			RAISERROR(22988, 16, -1, @edition)
		END
        RETURN (1)
    END
 
    -- Verify caller is authorized to enable change data capture for the database 
    if ([sys].[fn_cdc_is_user_authorized]() = 0) 
    begin
		select @user_privileges_needed = [sys].[fn_cdc_get_user_privileges]()
		raiserror(22902, 16, -1, @user_privileges_needed)
		return 1
    end

    /*
    ** Contained Database check (Replication is not yet supported on contained databases)
    ** If the current database is a contained database, then we error out.
    */
    SELECT @containment=containment FROM sys.databases WHERE
        database_id = db_id()
    if (@containment != 0)
    BEGIN
        set @db_name = db_name()
        RAISERROR(12839, 16, -1, @db_name)
        RETURN(1)
    END

	exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_enable_db', N'entering'

	if (@fCreateCDCUserImplicit is null)
	begin
		declare @is_cdc_create_user_implicit_enabled int
		exec @is_cdc_create_user_implicit_enabled = sys.sp_is_featureswitch_enabled N'ReplicationCreateImplicitUserForCDC';
		Select @fCreateCDCUserImplicit = IIF(@is_cdc_create_user_implicit_enabled = 1, 1, 0)
	end

	exec @retcode = sys.sp_cdc_enable_db_internal @fCreateCDCUserImplicit

	declare @status int = @@error
	if @status = 0 set @status = @retcode
	exec sys.sp_cdc_fire_trace_event 0, @status, N'sp_cdc_enable_db', N'complete'

	if (@status <> 0)
	begin
		return 1
	end

	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure [sys].[sp_cdc_enable_db]
(
	@fCreateCDCUserImplicit bit = 0
)
as
begin
	declare @retcode int
			,@containment tinyint
			,@db_name sysname
			,@user_privileges_needed nvarchar(1000)

    -- Verify CDC is supported for this SQL Server edition
    IF ([sys].[fn_cdc_is_supported]() = 1)
    BEGIN
        DECLARE @edition sysname
        SELECT @edition = CONVERT(sysname, SERVERPROPERTY('Edition'))
        RAISERROR(22988, 16, -1, @edition)
        RETURN (1)
    END
 
    -- Verify caller is authorized to enable change data capture for the database 
    if ([sys].[fn_cdc_is_user_authorized]() = 0) 
    begin
		select @user_privileges_needed = [sys].[fn_cdc_get_user_privileges]()
		raiserror(22902, 16, -1, @user_privileges_needed)
		return 1
    end

    /*
    ** Contained Database check (Replication is not yet supported on contained databases)
    ** If the current database is a contained database, then we error out.
    */
    SELECT @containment=containment FROM sys.databases WHERE
        database_id = db_id()
    if (@containment != 0)
    BEGIN
        set @db_name = db_name()
        RAISERROR(12839, 16, -1, @db_name)
        RETURN(1)
    END
    
    exec @retcode = sys.sp_cdc_enable_db_internal @fCreateCDCUserImplicit
    
    if (@@error <> 0) or (@retcode <> 0)
    begin
		return 1
	end
	
	return 0
end

