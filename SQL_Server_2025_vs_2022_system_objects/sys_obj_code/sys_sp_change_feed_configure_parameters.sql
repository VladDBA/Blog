use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_feed_configure_parameters
(
	@maxtrans 	int = NULL,
	@pollinterval int = NULL,
	@autoreseed bit = NULL,
	@autoreseedthreshold int = NULL
)
as
begin
	declare @action nvarchar(1000)
			,@trancount int
			,@raised_error int
			,@raised_state int
			,@raised_message nvarchar(4000)
			,@metadata_schema_name nvarchar(10)
			,@destination_type int = 0
			,@db_name sysname
			,@sal_return int
			,@is_blocking_maxtrans_change_trident_enabled int

	exec @sal_return =  sys.sp_trident_native_sal_raise_error_if_needed "sp_change_feed_configure_parameters"
	if @sal_return <> 0
	BEGIN
		RETURN (1)
	END

	set @db_name = db_name()

	-- Return if synapse link nor trident link is enabled
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 1, @db_name)
		return 1
	end

	if sys.fn_trident_link_is_enabled_for_current_db () = 1
	begin
		set @destination_type = 2
	end

	IF (sys.fn_has_permission_run_changefeed(@destination_type) = 0)
	BEGIN
		IF @destination_type = 2 -- Fabric Link
			BEGIN
				RAISERROR(22702, 16, 1, N'ALTER ANY EXTERNAL MIRROR and SELECT')
				RETURN (1)
			END

		IF @destination_type in (0, 1)  -- Synapse Link
			BEGIN
				RAISERROR(22702, 16, 1, N'CONTROL')
				RETURN (1)
			END
		-- There is no check below for invalid destiation types
		-- but the value is set above.
	END

	exec @is_blocking_maxtrans_change_trident_enabled = sys.sp_is_featureswitch_enabled N'TridentLinkBlockMaxTransChange'

	-- Block the maxtrans change in case trident link is enabled on this db
	if (@is_blocking_maxtrans_change_trident_enabled = 1 and  @destination_type = 2)
	begin
		if (@maxtrans is not null)
		begin
			raiserror(22635, 16, -1)
			return(1)
		end
	end

	if (@maxtrans is not null and  @maxtrans <= 0)
	begin
		raiserror(22713, 16, -1)
		return(1)
	end

	-- Get the metadata table schema name based on the feature enabled. Changefeed for synapse link & sys for Trident link
	exec sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT

	if object_id('' + @metadata_schema_name + '.change_feed_settings') is null
	begin
		raiserror(22768, 16, -1)
		return(1)
	end

	if (@pollinterval is not null and @pollinterval < 5)
	begin
		raiserror(22767, 16, -1)
		return(1)
	end

	declare @use_box_msi int
	exec @use_box_msi = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkUseBoxMSI'
	if @use_box_msi = 1 and object_id('sys.dm_os_host_info') is not null
	begin
		select @use_box_msi = 0 from sys.dm_os_host_info where host_platform = N'Linux' collate SQL_Latin1_General_CP1_CI_AS
	end

	if @autoreseed is not null and @use_box_msi <> 1
	begin
		raiserror(40584, 16, -1, 12, N'change', 22, N'@autoreseed')
		return(1)
	end

	if @autoreseedthreshold is not null
	begin
		if @use_box_msi <> 1
		begin
			raiserror(40584, 16, -1, 12, N'change', 30, N'@autoreseedthreshold')
			return(1)
		end
		else if not @autoreseedthreshold between 1 and 99
		begin
			raiserror(22611, 16, -1, N'@autoreseedthreshold')
			return(1)
		end
	end

	begin try
		begin tran
		
		set @action = N'ConfigureParameters'
		set @trancount = @@trancount
		
		declare @check_to_use_internal_tables bit
		-- Get the bit to determine if we need to use user tables or internal tables
		exec sys.sp_check_to_use_internal_tables @check_to_use_internal_tables OUTPUT
		
		if @check_to_use_internal_tables = 0
		begin
			execute as user = 'changefeed'
			UPDATE [changefeed].[change_feed_settings]
				SET maxtrans = IsNull(@maxtrans, maxtrans), pollinterval = IsNull(@pollinterval, pollinterval)
			revert
		end
		else
		begin
			UPDATE [sys].[change_feed_settings]
				SET maxtrans = IsNull(@maxtrans, maxtrans), pollinterval = IsNull(@pollinterval, pollinterval)
		end

		declare @val nvarchar(11)
		if @autoreseed is not null
		begin
			set @val = convert(nvarchar(11), @autoreseed)
			EXEC sys.xp_instance_regwrite	@rootkey = N'HKEY_LOCAL_MACHINE',
											@key = N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer\Trident',
											@value_name = 'TridentOneLakeLinkPublishingAutoReseed',
											@type = 'REG_SZ',
											@value = @val
		end

		if @autoreseedthreshold is not null
		begin
			set @val = convert(nvarchar(11), @autoreseedthreshold)
			EXEC sys.xp_instance_regwrite	@rootkey = N'HKEY_LOCAL_MACHINE',
											@key = N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer\Trident',
											@value_name = 'TridentLinkAutoReseedLogThresholdPct',
											@type = 'REG_SZ',
											@value = @val
		end

		if @use_box_msi = 1
		begin
			-- only call this proc to refresh configuration
			EXEC sys.sp_is_trident_one_lake_url N''
		end

		commit tran
	end try

	begin catch
		if @@trancount > @trancount
		begin
			-- If Change Feed opened the transaction, rollback the transaction
			if ( @trancount = 0 ) OR ( XACT_STATE() <> 1 )
			begin
				rollback tran
			end
		end

		if CURRENT_USER = 'changefeed'
		begin
			revert
		end

		-- Save the error number and associated message raised in the TRY block
		select @raised_error = ERROR_NUMBER()
		select @raised_state = ERROR_STATE()
		select @raised_message = ERROR_MESSAGE()

		raiserror(22710, 16, -1, @action, @raised_error, @raised_state, @raised_message)
		return 1
	end catch
	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_feed_configure_parameters
(
	@maxtrans 	int = NULL,
	@pollinterval int = NULL
)
as
begin
	declare @action nvarchar(1000)
			,@trancount int
			,@raised_error int
			,@raised_state int
			,@raised_message nvarchar(4000)

	IF (sys.fn_has_permission_run_changefeed() = 0)
	BEGIN
		RAISERROR(22702, 16, 1)
		RETURN (1)
	END

	if (@maxtrans is not null and  @maxtrans <= 0)
	begin
		raiserror(22713, 16, -1)
		return(1)
	end

	if (@pollinterval is not null and @pollinterval < 5)
	begin
		raiserror(22767, 16, -1)
		return(1)
	end

	if object_id(N'changefeed.change_feed_settings') is null
	begin
		raiserror(22768, 16, -1)
		return(1)
	end

	begin try
		begin tran
		execute as user = 'changefeed'
		set @action = N'ConfigureParameters'
		set @trancount = @@trancount

		declare @rows int, @error int
		UPDATE [changefeed].[change_feed_settings]
			SET maxtrans = IsNull(@maxtrans, maxtrans), pollinterval = IsNull(@pollinterval, pollinterval)
		revert
		commit tran
	end try

	begin catch
		if @@trancount > @trancount
		begin
			-- If Change Feed opened the transaction, rollback the transaction
			if ( @trancount = 0 ) OR ( XACT_STATE() <> 1 )
			begin
				rollback tran
			end
		end

		revert

		-- Save the error number and associated message raised in the TRY block
		select @raised_error = ERROR_NUMBER()
		select @raised_state = ERROR_STATE()
		select @raised_message = ERROR_MESSAGE()

		raiserror(22710, 16, -1, @action, @raised_error, @raised_state, @raised_message)
		return 1
	end catch
	return 0
end

