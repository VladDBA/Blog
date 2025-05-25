use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_feed_vupgrade
as
begin
	declare @supported bit
			,@SqlStmt nvarchar(4000)
			,@action nvarchar(1000)
			,@trancount int
			,@raised_error int
			,@raised_state int
			,@raised_message nvarchar(4000)
			,@sal_return INT
			,@is_disable_synapse_link_enabled int
	
	-- Verify if change feed schema upgrade is supported for this server
	set @supported = [sys].[fn_change_feed_schema_upgrade_is_supported]()
	if (@@ERROR <> 0 or @supported = 0)
	begin
		return 0
	end

	-- This is the current schema version tracked by this store procedure
	-- ** THIS SHOULD BE BUMPED ** whenever we add any new upgrade step to this procedure
	declare @current_schema_version int = 15;

	-- This is the schema version that current db is on
	declare @db_schema_version int = 0;
	
	declare @check_to_use_internal_tables bit
	-- Get the bit to determine if we need to use user tables or internal tables
	exec sys.sp_check_to_use_internal_tables @check_to_use_internal_tables OUTPUT

	set nocount on

	exec @sal_return =  sys.sp_trident_native_sal_raise_error_if_needed "sp_change_feed_vupgrade"
	if @sal_return <> 0
	BEGIN
		RETURN @sal_return
	END

	if @check_to_use_internal_tables = 0
	begin
		-- Switch to database user 'changefeed'
		execute as user = 'changefeed'
	end
	
	BEGIN TRY

	set @trancount = @@trancount

	-- Create schema_version column if not exists
	if (@check_to_use_internal_tables = 0)
	begin
		if (not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_settings') and name = N'schema_version'))
		begin
			set @action = N'AddSchemaVersionColumn'
			begin tran
				alter table [changefeed].[change_feed_settings] add schema_version int
			commit tran
		end
		else 
		begin
			set @action = N'FetchCurrentSchemaVersion'
			set @SqlStmt = 'select @db_schema_version = schema_version from [changefeed].[change_feed_settings]'
			exec sp_executesql @SqlStmt, N'@db_schema_version int output', @db_schema_version = @db_schema_version output;
		end
	end
	else
	begin
		set @action = N'FetchCurrentSchemaVersion'
		select @db_schema_version = schema_version from [sys].[change_feed_settings]
	end
	

	-- Run upgrade statements only when db_schema_version is lower than current schema version.
	if (@db_schema_version < @current_schema_version)
	begin
		begin tran
		set @action = N'RunUpgradeStatements'

		-- Upgrade statements go here
		-- Please make sure to follow the following guidelines while adding upgrade steps
		--		Bump up the @current_schema_version
		--		Update the default value for schema_version in change_feed_settings table to the @current_schema_version
		--		Schema changes are backward compatible
		--		Upgrade step is idempotent
		--		Upgrade step is added as an incremental change
		--      If adding a new column as part of the upgrade path, please specify NULL or NOT NULL default constraint

		-- This is an example for adding an upgrade step
		-- 		Assuming @current_schema_version is bumped
		--		
		--		if (@db_schema_version < <replace this place-holder with current_schema_version>)
		-- 		begin
		--			if not exists (select * from sys.columns where object_id = object_id('<table_name>') and name = N'<column_name>')
		-- 				alter table <table_name> add <column_name> <column_type>
		-- 		end
		--Add pollinterval, dbversion is < 3
		if (@db_schema_version < 3)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_settings') and name = N'pollinterval')
				alter table [changefeed].[change_feed_settings] add pollinterval int not null default 5
		end

		if (@db_schema_version < 4)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'destination_type')
			begin
				alter table [changefeed].[change_feed_table_groups] add destination_type tinyint NOT NULL DEFAULT 0
			end
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'max_message_size_bytes')
			begin
				alter table [changefeed].[change_feed_table_groups] add max_message_size_bytes int NULL
			end
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'partition_scheme')
			begin
				alter table [changefeed].[change_feed_table_groups] add partition_scheme tinyint NOT NULL DEFAULT 0
			end
		end

		-- Upgrade step to add snapshot_phase, snapshot_current_phase_time, snapshot_retry_count, snapshot_start_time and snapshot_end_time columns
		-- to the Change_Feed_Tables table.
		if (@db_schema_version < 5)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'snapshot_phase')
			begin
				alter table [changefeed].[change_feed_tables] 
				add snapshot_phase tinyint NULL,
				snapshot_current_phase_time datetime NULL,
				snapshot_retry_count int NULL,
				snapshot_start_time datetime NULL,
				snapshot_end_time datetime NULL
			end
		end
		
		-- Upgrade step to add snapshot_row_count
		-- to the Change_Feed_Tables table. 
		if (@db_schema_version < 6)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'snapshot_row_count')
			begin
				alter table [changefeed].[change_feed_tables] 
				add snapshot_row_count bigint NOT NULL default 0
			end
		end

		if (@db_schema_version < 7)
		begin
			-- Upgrade step to add reseed_state and reseed_id
			-- to the change_feed_settings and change_feed_tables.
			if (sys.fn_trident_link_is_enabled_for_current_db () = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_settings') and name = N'reseed_state')
				begin
					alter table [changefeed].[change_feed_settings] 
					add reseed_state tinyint NOT NULL default 0
				end
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'reseed_id')
				begin
					alter table [changefeed].[change_feed_tables] 
					add reseed_id nvarchar(10) NULL
				end
			end
		end

		if (@db_schema_version < 8)
		begin
			-- Upgrade step to increase the size of reseed_id in change_feed_tables.
			if (sys.fn_trident_link_is_enabled_for_current_db () = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'reseed_id')
				begin
					alter table [changefeed].[change_feed_tables] 
					add reseed_id nvarchar(36) NULL
				end
				else
				begin
					alter table [changefeed].[change_feed_tables] 
					alter column reseed_id nvarchar(36) NULL
				end
			end
		end
		
		if (@db_schema_version < 9)
		begin
			-- Upgrade step to add partition_column_name in change_feed_table_groups table.
			if (sys.fn_change_feed_is_enabled_for_current_db () = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'partition_column_name')
				begin
					alter table [changefeed].[change_feed_table_groups] 
					add partition_column_name sysname NULL
				end
				else
				begin
					alter table [changefeed].[change_feed_table_groups] 
					alter column partition_column_name sysname NULL
				end
			end
		end

		if (@db_schema_version < 10)
		begin
			-- Upgrade step to add include_old_values in change_feed_tables table.
			if (sys.fn_change_feed_is_enabled_for_current_db () = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'include_old_values')
				begin
					alter table [changefeed].[change_feed_tables] 
					add include_old_values bit NOT NULL DEFAULT 0
				end
			end
		end

		if (@db_schema_version < 11)
		begin
			-- Upgrade step to add include_all_columns in change_feed_tables table.
			if (sys.fn_change_feed_is_enabled_for_current_db() = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'include_all_columns')
				begin
					alter table [changefeed].[change_feed_tables] 
					add include_all_columns bit NOT NULL DEFAULT 0
				end
			end
		end

		if (@db_schema_version < 12)
		begin
			-- Upgrade step to add include_old_lob_values in change_feed_tables table.
			if (sys.fn_change_feed_is_enabled_for_current_db() = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'include_old_lob_values')
				begin
					alter table [changefeed].[change_feed_tables] 
					add include_old_lob_values bit NOT NULL DEFAULT 0
				end
			end
		end

		if (@db_schema_version < 13)
		begin
			-- Upgrade step to add encoding in change_feed_table_groups table.
			if (sys.fn_change_feed_is_enabled_for_current_db () = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'encoding')
				begin
					alter table [changefeed].[change_feed_table_groups]
					add encoding tinyint NOT NULL DEFAULT 0
				end
			end
		end

		if (@db_schema_version < 14)
		begin
			-- Upgrade step to add streaming_dest_type in change_feed_table_groups table.
			if (sys.fn_change_feed_is_enabled_for_current_db () = 1 and @check_to_use_internal_tables = 0)
			begin
				if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'streaming_dest_type')
				begin
					alter table [changefeed].[change_feed_table_groups]
					add streaming_dest_type sysname NULL
				end
			end
		end

		exec @is_disable_synapse_link_enabled = sys.sp_is_featureswitch_enabled N'DisableSynapseLinkOnUpgrade'
		if  (@is_disable_synapse_link_enabled = 1 and sys.fn_synapse_link_is_enabled_for_current_db () = 1)
		begin
			-- Upgrade step to disable synapse link if sql server version is 2025 and above
			if (@db_schema_version < 15)
			begin
				raiserror(22798, 10, -1)
				exec @sal_return =  sys.sp_change_feed_disable_db
				commit tran
				revert
				return @sal_return
			end
		end

		if(@check_to_use_internal_tables = 1)
		begin
			-- Update the schema version to engine version
			set @action = N'UpdateSchemaVersion'
			update [sys].[change_feed_settings] set schema_version = @current_schema_version
		end
		else
		begin
			-- Update the schema version to engine version
			set @action = N'UpdateSchemaVersion'
			update [changefeed].[change_feed_settings] set schema_version = @current_schema_version
		end
		
		commit tran
	end

	revert
	END TRY
	BEGIN CATCH
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
	END CATCH
	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_feed_vupgrade
as
begin
	declare @supported bit
			,@SqlStmt nvarchar(4000)
			,@action nvarchar(1000)
			,@trancount int
			,@raised_error int
			,@raised_state int
			,@raised_message nvarchar(4000)
	
	-- Verify if change feed schema upgrade is supported for this server
	set @supported = [sys].[fn_change_feed_schema_upgrade_is_supported]()
	if (@@ERROR <> 0 or @supported = 0)
	begin
		return 0
	end

	-- This is the current schema version tracked by this store procedure
	-- ** THIS SHOULD BE BUMPED ** whenever we add any new upgrade step to this procedure
	declare @current_schema_version int = 6;

	-- This is the schema version that current db is on
	declare @db_schema_version int = 0;

	set nocount on

	-- Switch to database user 'changefeed'
	execute as user = 'changefeed'

	BEGIN TRY

	set @trancount = @@trancount

	-- Create schema_version column if not exists
	if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_settings') and name = N'schema_version')
	begin
		set @action = N'AddSchemaVersionColumn'
		begin tran
			alter table [changefeed].[change_feed_settings] add schema_version int
		commit tran
	end
	else
	begin
		set @action = N'FetchCurrentSchemaVersion'
		set @SqlStmt = 'select @db_schema_version = schema_version from [changefeed].[change_feed_settings]'
		exec sp_executesql @SqlStmt, N'@db_schema_version int output', @db_schema_version = @db_schema_version output;
	end

	-- Run upgrade statements only when db_schema_version is lower than current schema version.
	if (@db_schema_version < @current_schema_version)
	begin
		begin tran
		set @action = N'RunUpgradeStatements'

		-- Upgrade statements go here
		-- Please make sure to follow the following guidelines while adding upgrade steps
		--		Bump up the @current_schema_version
		--		Update the default value for schema_version in change_feed_settings table to the @current_schema_version
		--		Schema changes are backward compatible
		--		Upgrade step is idempotent
		--		Upgrade step is added as an incremental change
		--      If adding a new column as part of the upgrade path, please specify NULL or NOT NULL default constraint

		-- This is an example for adding an upgrade step
		-- 		Assuming @current_schema_version is bumped
		--		
		--		if (@db_schema_version < <replace this place-holder with current_schema_version>)
		-- 		begin
		--			if not exists (select * from sys.columns where object_id = object_id('<table_name>') and name = N'<column_name>')
		-- 				alter table <table_name> add <column_name> <column_type>
		-- 		end
		--Add pollinterval, dbversion is < 3
		if (@db_schema_version < 3)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_settings') and name = N'pollinterval')
				alter table [changefeed].[change_feed_settings] add pollinterval int not null default 5
		end

		if (@db_schema_version < 4)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'destination_type')
			begin
				alter table [changefeed].[change_feed_table_groups] add destination_type tinyint NOT NULL DEFAULT 0
			end
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'max_message_size_bytes')
			begin
				alter table [changefeed].[change_feed_table_groups] add max_message_size_bytes int NULL
			end
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_table_groups') and name = N'partition_scheme')
			begin
				alter table [changefeed].[change_feed_table_groups] add partition_scheme tinyint NOT NULL DEFAULT 0
			end
		end

		-- Upgrade step to add snapshot_phase, snapshot_current_phase_time, snapshot_retry_count, snapshot_start_time and snapshot_end_time columns
		-- to the Change_Feed_Tables table.
		if (@db_schema_version < 5)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'snapshot_phase')
			begin
				alter table [changefeed].[change_feed_tables] 
				add snapshot_phase tinyint NULL,
				snapshot_current_phase_time datetime NULL,
				snapshot_retry_count int NULL,
				snapshot_start_time datetime NULL,
				snapshot_end_time datetime NULL
			end
		end
		
		-- Upgrade step to add snapshot_row_count
		-- to the Change_Feed_Tables table. 
		if (@db_schema_version < 6)
		begin
			if not exists (select * from sys.columns where object_id = object_id('changefeed.change_feed_tables') and name = N'snapshot_row_count')
			begin
				alter table [changefeed].[change_feed_tables] 
				add snapshot_row_count bigint NOT NULL default 0
			end
		end

		-- Update the schema version to engine version
		set @action = N'UpdateSchemaVersion'
		set @SqlStmt = 'update [changefeed].[change_feed_settings] set schema_version = @db_schema_version'
		EXEC sp_executesql @SqlStmt, N'@db_schema_version int', @current_schema_version;
		commit tran
	end

	revert
	END TRY
	BEGIN CATCH
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
	END CATCH
	return 0
end

