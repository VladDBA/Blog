SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure [sys].[sp_cdc_vupgrade](@force_metadata_fixup bit = 0)
as
begin

	declare	@db_name sysname
			,@raised_error int
			,@raised_message nvarchar(4000)
			,@action nvarchar(1000)
			,@trancount int
			,@bypass_proc_gen bit
			,@allow_alter_cdc_objects int
			,@allow_drop_cdc_objects int
			,@allow_fixup_cdc_objects int

	set nocount on

	--
 	-- Authorization check.
	if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
	begin
		raiserror(22904, 16, -1)
		return(1)
	end

	set @raised_error = 0
	set @db_name = db_name()
	set @bypass_proc_gen = 0
	
	--
	-- Determine whether trace flag to bypass proc generation has been set 
	-- (trace flag 8218)
	exec sys.sp_is_trace_flag_to_bypass_proc_gen_set @bypass_proc_gen output
	
	exec @allow_alter_cdc_objects = sys.sp_is_featureswitch_enabled N'AllowAlterCDCMetaObjects'
	exec @allow_drop_cdc_objects = sys.sp_is_featureswitch_enabled N'AllowDropCDCMetaObjects'
	exec @allow_fixup_cdc_objects = sys.sp_is_featureswitch_enabled N'AllowFixupCDCMetaObjects'

	create table #index_column_names(column_name sysname null, index_ordinal int null)

	-- Encapsulate transaction logic in TRY/CATCH. 
	begin try

		-- Open a transaction to begin metadata updates.
		set @trancount = @@trancount
		
		begin tran
		save tran tr_cdc_vupgrade

		exec sp_set_drop_meta_objects_allowed

		-- let's check metadata before lockdown only when both DDL alter/drop disabled and fixup enabled or when customer forcefully wants to be clean it up
		if (isnull(@allow_alter_cdc_objects, -1) = 0 and isnull(@allow_drop_cdc_objects, -1) = 0 and isnull(@allow_fixup_cdc_objects, -1) = 1) or @force_metadata_fixup = 1
		begin
			exec sys.sp_cdc_fire_trace_event 0, -1, sp_cdc_vupgrade, N'entering sp_fixup_cdc_metadata section'

			exec sys.sp_fixup_cdc_metadata N'[cdc].[captured_columns]', N'object_id,column_name,column_id,column_type,column_ordinal,is_computed,masking_function'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[cdc_jobs]', N'job_type,maxtrans,maxscans,continuous,pollinginterval,retention,threshold'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[change_tables]', N'object_id,version,source_object_id,capture_instance,start_lsn,end_lsn,supports_net_changes,has_drop_pending,role_name,index_name,filegroup_name,create_date,partition_switch', N'partition_switch', N'((0))'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[ddl_history]', N'source_object_id,object_id,required_column_update,ddl_command,ddl_lsn,ddl_time'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[index_columns]', N'object_id,column_name,index_ordinal,column_id'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[lsn_time_mapping]', N'start_lsn,tran_begin_time,tran_end_time,tran_id,tran_begin_lsn'
			exec sys.sp_fixup_cdc_metadata N'[dbo].[systranschemas]', N'tabid,startlsn,endlsn,typeid', N'typeid', N'((52))'

			if object_id(N'[cdc].[change_tables]') is not null and object_id(N'[cdc].[captured_columns]') is not null
			begin
				declare #hobjs cursor local fast_forward for select object_id from [cdc].[change_tables]
				open #hobjs
				declare @obj_id int
				declare @sys_cols nvarchar(max) = N'__$start_lsn,__$end_lsn,__$seqval,__$operation,__$update_mask,__$command_id'
				fetch #hobjs into @obj_id
				while (@@fetch_status <> -1)
				begin
					declare @obj_name nvarchar(261) = N'[' + object_schema_name(@obj_id) + N'].[' + object_name(@obj_id) + N']'
					exec sys.sp_fixup_cdc_metadata @obj_name = @obj_name, @sys_column_list = @sys_cols, @ct_obj_id = @obj_id
					fetch #hobjs into @obj_id
				end
				close #hobjs
				deallocate #hobjs
			end

			exec sys.sp_cdc_fire_trace_event 0, 0, sp_cdc_vupgrade, N'complete sp_fixup_cdc_metadata section'
		end

		if object_id(N'[cdc].[change_tables]') is not null
		begin
			if not exists (select * from sys.columns where object_id = object_id(N'[cdc].[change_tables]') and name = N'partition_switch')
			begin

				set @action = N'add column partition_switch'
				alter table [cdc].[change_tables] add partition_switch bit default 0 not null
			end
			
			if not exists (select * from sys.indexes where object_id = object_id(N'[cdc].[change_tables]') and name = N'source_object_id_idx')
			begin
			
				set @action = N'create index source_object_id_idx on cdc.change_tables'
				create index [source_object_id_idx] on [cdc].[change_tables] (  source_object_id ASC )
			end
			
			-- Drop index change_table_nonunique_idx if it exists
			if exists (select * from sys.indexes where object_id = object_id(N'[cdc].[change_tables]') and name = N'change_tables_nonunique_idx')
			begin
			
				set @action = N'drop cdc.change_tables index change_tables_nonunique_idx'
				drop index [change_tables_nonunique_idx] on [cdc].[change_tables]
			end
			
			-- create index change_table_unique_idx if it doesn't exist otherwise create with drop_existing option
			if not exists (select * from sys.indexes where object_id = object_id(N'[cdc].[change_tables]') and name = N'change_tables_unique_idx')
			begin
			
				set @action = N'create cdc.change_tables index change_tables_unique_idx'
				create unique index [change_tables_unique_idx] on [cdc].[change_tables] (  capture_instance ASC )
			end
			else
			begin
			
				set @action = N'alter cdc.change_tables index change_tables_unique_idx with (drop_existing = on)'
				create unique index [change_tables_unique_idx] on [cdc].[change_tables] (  capture_instance ASC )
				with (drop_existing = ON)
			end
			
			-- Insure nullability setting is correct for all columns in the meta data tables
			-- cdc.change_tables
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'version' and is_nullable = 0)
				alter table cdc.change_tables alter column version int null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'source_object_id' and is_nullable = 0)
				alter table cdc.change_tables alter column source_object_id int null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'supports_net_changes' and is_nullable = 0)
				alter table cdc.change_tables alter column supports_net_changes bit null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'has_drop_pending' and is_nullable = 0)
				alter table cdc.change_tables alter column has_drop_pending bit null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'create_date' and is_nullable = 0)
				alter table cdc.change_tables alter column create_date datetime null

			-- cdc.captured_columns
			if exists (select * from sys.columns where object_id = object_id('cdc.captured_columns') and name = N'column_id' and is_nullable = 0)
				alter table cdc.captured_columns alter column column_id int null
			if exists (select * from sys.columns where object_id = object_id('cdc.captured_columns') and name = N'is_computed' and is_nullable = 0)
				alter table cdc.captured_columns alter column is_computed bit null
			if not exists (select * from sys.columns where object_id = object_id('cdc.captured_columns') and name = N'masking_function')
				alter table cdc.captured_columns add masking_function nvarchar(4000) collate Latin1_General_CI_AS_KS_WS null
			declare @dmstmt nvarchar(max) = N'
			-- Populate the new masking function column if needed (e.g., if upgrading from a SQL 2016 CTP installation)
			update cdc.captured_columns
			set masking_function = m.masking_function
			from sys.masked_columns m
			inner join cdc.change_tables ct
			on m.object_id = ct.source_object_id
			where cdc.captured_columns.object_id = ct.object_id
				and cdc.captured_columns.column_id = m.column_id

			-- Update the captured columns of the change tables (_CT table) by applying the masking function of the original table, if needed
			declare
				@alter_stmt			nvarchar(max),
				@change_table_name	sysname,
				@ct_column_name		sysname,
				@masking_function	nvarchar(4000)

			declare #hcolumns cursor local fast_forward for
			select t.name as change_table_name, i.column_name, i.masking_function
			from cdc.captured_columns i
			inner join sys.tables t on t.object_id = i.object_id
			where i.masking_function is not null

			open #hcolumns
			fetch #hcolumns into @change_table_name
				,@ct_column_name
				,@masking_function
			while (@@fetch_status <> -1)
			begin
				if exists (select * from sys.tables where name = @change_table_name)
				begin
					set @alter_stmt =  N''alter table [cdc].'' + quotename(@change_table_name) + N'' alter column '' + quotename(@ct_column_name) + N'' add masked with (function = '' + quotename(@masking_function, '''''''') + N'')''
					exec (@alter_stmt)
				end
				fetch #hcolumns into @change_table_name
					,@ct_column_name
					,@masking_function
			end
			close #hcolumns
			deallocate #hcolumns'
			exec (@dmstmt)

			-- cdc.index_columns (fixed for a case of nonclustered index with included columns)
			if exists (select * from sys.columns where object_id = object_id('cdc.index_columns') and name = N'column_id' and is_nullable = 1)
			begin
				update cdc.index_columns set column_id = 0 where column_id is null
				alter table cdc.index_columns alter column column_id int not null
				if exists (select * from sys.objects where name = 'index_columns_clustered_idx' and type='PK' and parent_object_id = object_id('cdc.index_columns'))
				begin
					alter table cdc.index_columns drop constraint [index_columns_clustered_idx]
					alter table cdc.index_columns add constraint [index_columns_clustered_idx] primary key clustered (object_id ASC, index_ordinal ASC, column_id ASC)
				end
			end

			-- cdc.dll_history
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'source_object_id' and is_nullable = 0)
				alter table cdc.ddl_history alter column source_object_id int null
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'required_column_update' and is_nullable = 0)
				alter table cdc.ddl_history alter column required_column_update bit null
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'ddl_command' and is_nullable = 0)
				alter table cdc.ddl_history alter column ddl_command nvarchar(max) null
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'ddl_time' and is_nullable = 0)
				alter table cdc.ddl_history alter column ddl_time datetime null

			-- cdc.lsn_time_mapping
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_begin_time' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_begin_time datetime null
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_end_time' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_end_time datetime null
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_id' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_id varbinary(10) null
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_begin_lsn' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_begin_lsn binary(10) null
			
			-- Regenerate the enumeration TVFs to use the modified definition of fn_cdc_check_parameters
			declare @source_schema sysname, @source_name sysname, @capture_instance sysname,
				@supports_net_changes bit, @role_name sysname, @index_name nvarchar(1000),
				@fn_all_changes nvarchar(1000), @fn_net_changes nvarchar(1000), @change_table nvarchar(1000),
				@stmt nvarchar(max), @pk_column_list nvarchar(max), @column_name sysname, @ddl_action nvarchar(10)

			declare #hinstance cursor local fast_forward for
				select object_schema_name(source_object_id) as source_schema,
					   object_name(source_object_id) as source_name,
					   capture_instance, supports_net_changes, role_name
				from cdc.change_tables
				where object_schema_name(source_object_id) is not null and
					  object_name(source_object_id) is not null

			open #hinstance
			fetch #hinstance into @source_schema, @source_name, @capture_instance,
				@supports_net_changes, @role_name

			while (@@fetch_status <> -1)
			begin
				-- Drop the enumeration TVFs if they exist
				set @fn_all_changes = N'[cdc].' + quotename( N'fn_cdc_get_all_changes' +  N'_' + @capture_instance)
				set @fn_net_changes = N'[cdc].' + quotename( N'fn_cdc_get_net_changes' +  N'_' + @capture_instance)
				set @index_name = quotename(@capture_instance + N'_CT_idx')
				set @change_table = N'[cdc].' + quotename(@capture_instance + N'_CT')
	
				if not exists (select * from sys.columns where object_id = object_id(@change_table) and name = N'__$command_id')
				begin
					set @action = N'add column __$command_id'
					set @stmt = N'alter table ' + @change_table + N' add __$command_id int null'
					exec (@stmt)
				end

				if object_id (@fn_all_changes, 'IF') is not null
				begin
					set @action = N'drop function fn_cdc_get_all_changes_<capture_instance>' 
					set @stmt = N'drop function ' + @fn_all_changes
					exec (@stmt)
				end
		
				if object_id (@fn_net_changes, 'IF') is not null
				begin
					set @action = N'drop function fn_cdc_get_net_changes_<capture_instance>' 
					set @stmt = N'drop function ' + @fn_net_changes
					exec (@stmt)
				end
		
				-- Generate the enumeration functions.
				set @action = N'sp_cdc_create_change_enumeration_functions' 
				exec [sys].[sp_cdc_create_change_enumeration_functions]
					@source_schema, @source_name, @capture_instance, @supports_net_changes

				-- Grant select permission on the enumeration functions.
				set @action = N'sp_cdc_grant_select_on_change_enumeration_functions' 
				exec [sys].[sp_cdc_grant_select_on_change_enumeration_functions]
					@capture_instance, @supports_net_changes, @role_name
					
				-- Drop the change table non-clustered index if it exists and then recreate it 
				-- This should only take place when @supports_net_changes = 1 and if new index definition differs from existing one
				-- NB: Following query should be in sync with following index definition
				-- Check if ChangeDataCaptureResolveCollationConflict feature switch is enabled
				DECLARE @diff int
				if ([sys].[fn_cdc_resolvecollationconflict_featureswitch_is_enabled]() = 1)
				begin
					EXEC sp_cdc_vupgrade_get_diff @capture_instance, @change_table, @diff output
				end
				else
				begin
					EXEC sp_cdc_vupgrade_get_diff_v2 @capture_instance, @change_table, @diff output
				end
				IF (@supports_net_changes = 1 AND @diff <> 0)
				begin

					if exists (select * from sys.indexes where object_id = object_id(@change_table) and quotename(name) = @index_name)
					begin
						set @action = N'drop non-clustered change table index'
						set @stmt = N'drop index ' + @index_name + N' on ' + @change_table
						exec (@stmt)
					end	
								
					set @pk_column_list = N' '
					
					delete #index_column_names
		
					insert into #index_column_names
					select column_name, index_ordinal
					from [cdc].[index_columns] where object_id = object_id(@change_table)

					declare #hidxcolumns cursor local fast_forward for
						select column_name
						from #index_column_names 
						order by index_ordinal
		
					open #hidxcolumns
					fetch #hidxcolumns into @column_name
	
					while (@@fetch_status <> -1)
					begin
						set @pk_column_list = @pk_column_list + quotename(@column_name) +  N' ASC, ' 

						fetch #hidxcolumns into @column_name
					end
	
					close #hidxcolumns
					deallocate #hidxcolumns
	
					set @action = N'create unique nonclustered index for change table'
					-- NB: Following index script should be in sync with above query identifying necessary changes in the index metadata
					set @stmt =  N'create unique nonclustered index ' + 
						quotename(@capture_instance + N'_CT_idx') +
						N' on ' + @change_table + N'
						( '  + @pk_column_list + N'
							[__$start_lsn] ASC,
							[__$command_id] ASC,
							[__$seqval] ASC,
							[__$operation] ASC
						) INCLUDE ( [__$update_mask] )'	 	
	
					exec (@stmt)
				end

				if (@bypass_proc_gen = 0)
				begin
					-- Alter the stored procedures to populate the change table
					-- and mark them as system procs
					set @action = N'sp_cdc_create_populate_stored_procs'
					exec [sys].[sp_cdc_create_populate_stored_procs]
						@capture_instance, N'alter' 
				end	
				
				-- Mark change table as a system object
				set @action = N'sp_MS_marksystemobject @change_table'
				exec sp_MS_marksystemobject @change_table

				fetch #hinstance into @source_schema, @source_name, @capture_instance,
					@supports_net_changes, @role_name
			end		

			close #hinstance
			deallocate #hinstance
		
		end

		if object_id(N'[cdc].[lsn_time_mapping]') is not null
		begin
			if not exists (select * from sys.columns where object_id = object_id(N'[cdc].[lsn_time_mapping]') and name = N'tran_begin_lsn')
			begin

				set @action = N'add column tran_begin_lsn'
				alter table [cdc].[lsn_time_mapping] add tran_begin_lsn binary(10) null

				set @action = N'upgrade/create procs for lsn_time_mapping'
				exec [sys].[sp_cdc_lsn_time_mapping_procs] @action = N'alter'
				
			end
		end
		
		-- Mark CDC tables as system objects
		set @action = N'sp_MS_marksystemobject cdc.change_tables'
		exec sp_MS_marksystemobject N'cdc.change_tables'

		set @action = N'sp_MS_marksystemobject cdc.ddl_history'
		exec sp_MS_marksystemobject N'cdc.ddl_history'

		set @action = N'sp_MS_marksystemobject cdc.captured_columns'
		exec sp_MS_marksystemobject N'cdc.captured_columns'

		set @action = N'sp_MS_marksystemobject cdc.index_columns'
		exec sp_MS_marksystemobject N'cdc.index_columns'
		
		set @action = N'sp_MS_marksystemobject dbo.systranschemas'
		exec sp_MS_marksystemobject N'dbo.systranschemas'
		
		set @action = N'sp_MS_marksystemobject cdc.lsn_time_mapping'
		exec sp_MS_marksystemobject N'cdc.lsn_time_mapping'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_all_changes_...]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_all_changes_...]'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_net_changes_...]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_net_changes_...]'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_all_changes_ ... ]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_all_changes_ ... ]'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_net_changes_ ... ]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_net_changes_ ... ]'

		declare	@fs_vupgrade_v2_skip_owner_check int
		exec @fs_vupgrade_v2_skip_owner_check = sys.sp_is_featureswitch_enabled N'CDCVUpgradeV2SkipOwnerCheck'
		if @fs_vupgrade_v2_skip_owner_check = 1
		begin
			EXEC sys.sp_cdc_vupgrade_v2 @db_name, 1
		end
		else
		begin
			EXEC master.sys.sp_cdc_vupgrade_v2 @db_name
		end

		commit tran

	end try
	begin catch
    
		if @@trancount > @trancount
		begin
			-- If CDC opened the transaction or it is not possible 
			-- to rollback to the savepoint, rollback the transaction
			if ( @trancount = 0 ) OR ( XACT_STATE() <> 1 )
			begin
				rollback tran 
			end
			-- Otherwise rollback to the savepoint
			else
			begin
				rollback tran tr_cdc_vupgrade
				commit tran
			end
		end
		
		-- Save the error number and associated message raised in the TRY block
		select @raised_error = ERROR_NUMBER()
		select @raised_message = N'line ' + cast(ERROR_LINE() as nvarchar) + N', state ' + cast(ERROR_STATE() as nvarchar) + N', ' + ERROR_MESSAGE() 
		exec sys.sp_cdc_fire_trace_event 0, @raised_error, N'sp_cdc_vupgrade', @raised_message
	end catch
	
	exec sp_unset_drop_meta_objects_allowed

	if @raised_error = 0
		return (0)
		
	raiserror(22841, 16, -1, @db_name, @action, @raised_error, @raised_message)  
	return (1)

end


/*====  SQL Server 2022 version  ====*/
create procedure [sys].[sp_cdc_vupgrade](@force_metadata_fixup bit = 0)
as
begin

	declare	@db_name sysname
			,@raised_error int
			,@raised_message nvarchar(4000)
			,@action nvarchar(1000)
			,@trancount int
			,@bypass_proc_gen bit
			,@allow_alter_cdc_objects int
			,@allow_drop_cdc_objects int
			,@allow_fixup_cdc_objects int

	set nocount on

	--
 	-- Authorization check.
	if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
	begin
		raiserror(22904, 16, -1)
		return(1)
	end

	set @raised_error = 0
	set @db_name = db_name()
	set @bypass_proc_gen = 0
	
	--
	-- Determine whether trace flag to bypass proc generation has been set 
	-- (trace flag 8218)
	exec sys.sp_is_trace_flag_to_bypass_proc_gen_set @bypass_proc_gen output
	
	exec @allow_alter_cdc_objects = sys.sp_is_featureswitch_enabled N'AllowAlterCDCMetaObjects'
	exec @allow_drop_cdc_objects = sys.sp_is_featureswitch_enabled N'AllowDropCDCMetaObjects'
	exec @allow_fixup_cdc_objects = sys.sp_is_featureswitch_enabled N'AllowFixupCDCMetaObjects'

	create table #index_column_names(column_name sysname null, index_ordinal int null)

	-- Encapsulate transaction logic in TRY/CATCH. 
	begin try

		-- Open a transaction to begin metadata updates.
		set @trancount = @@trancount
		
		begin tran
		save tran tr_cdc_vupgrade

		exec sp_set_drop_meta_objects_allowed

		-- let's check metadata before lockdown only when both DDL alter/drop disabled and fixup enabled or when customer forcefully wants to be clean it up
		if (isnull(@allow_alter_cdc_objects, -1) = 0 and isnull(@allow_drop_cdc_objects, -1) = 0 and isnull(@allow_fixup_cdc_objects, -1) = 1) or @force_metadata_fixup = 1
		begin
			exec sys.sp_fixup_cdc_metadata N'[cdc].[captured_columns]', N'object_id,column_name,column_id,column_type,column_ordinal,is_computed,masking_function'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[cdc_jobs]', N'job_type,maxtrans,maxscans,continuous,pollinginterval,retention,threshold'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[change_tables]', N'object_id,version,source_object_id,capture_instance,start_lsn,end_lsn,supports_net_changes,has_drop_pending,role_name,index_name,filegroup_name,create_date,partition_switch', N'partition_switch', N'((0))'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[ddl_history]', N'source_object_id,object_id,required_column_update,ddl_command,ddl_lsn,ddl_time'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[index_columns]', N'object_id,column_name,index_ordinal,column_id'
			exec sys.sp_fixup_cdc_metadata N'[cdc].[lsn_time_mapping]', N'start_lsn,tran_begin_time,tran_end_time,tran_id,tran_begin_lsn'
			exec sys.sp_fixup_cdc_metadata N'[dbo].[systranschemas]', N'tabid,startlsn,endlsn,typeid', N'typeid', N'((52))'

			if object_id(N'[cdc].[change_tables]') is not null and object_id(N'[cdc].[captured_columns]') is not null
			begin
				declare #hobjs cursor local fast_forward for select object_id from [cdc].[change_tables]
				open #hobjs
				declare @obj_id int
				declare @sys_cols nvarchar(max) = N'__$start_lsn,__$end_lsn,__$seqval,__$operation,__$update_mask,__$command_id'
				fetch #hobjs into @obj_id
				while (@@fetch_status <> -1)
				begin
					declare @obj_name nvarchar(261) = N'[' + object_schema_name(@obj_id) + N'].[' + object_name(@obj_id) + N']'
					exec sys.sp_fixup_cdc_metadata @obj_name = @obj_name, @sys_column_list = @sys_cols, @ct_obj_id = @obj_id
					fetch #hobjs into @obj_id
				end
				close #hobjs
				deallocate #hobjs
			end
		end

		if object_id(N'[cdc].[change_tables]') is not null
		begin
			if not exists (select * from sys.columns where object_id = object_id(N'[cdc].[change_tables]') and name = N'partition_switch')
			begin

				set @action = N'add column partition_switch'
				alter table [cdc].[change_tables] add partition_switch bit default 0 not null
			end
			
			if not exists (select * from sys.indexes where object_id = object_id(N'[cdc].[change_tables]') and name = N'source_object_id_idx')
			begin
			
				set @action = N'create index source_object_id_idx on cdc.change_tables'
				create index [source_object_id_idx] on [cdc].[change_tables] (  source_object_id ASC )
			end
			
			-- Drop index change_table_nonunique_idx if it exists
			if exists (select * from sys.indexes where object_id = object_id(N'[cdc].[change_tables]') and name = N'change_tables_nonunique_idx')
			begin
			
				set @action = N'drop cdc.change_tables index change_tables_nonunique_idx'
				drop index [change_tables_nonunique_idx] on [cdc].[change_tables]
			end
			
			-- create index change_table_unique_idx if it doesn't exist otherwise create with drop_existing option
			if not exists (select * from sys.indexes where object_id = object_id(N'[cdc].[change_tables]') and name = N'change_tables_unique_idx')
			begin
			
				set @action = N'create cdc.change_tables index change_tables_unique_idx'
				create unique index [change_tables_unique_idx] on [cdc].[change_tables] (  capture_instance ASC )
			end
			else
			begin
			
				set @action = N'alter cdc.change_tables index change_tables_unique_idx with (drop_existing = on)'
				create unique index [change_tables_unique_idx] on [cdc].[change_tables] (  capture_instance ASC )
				with (drop_existing = ON)
			end
			
			-- Insure nullability setting is correct for all columns in the meta data tables
			-- cdc.change_tables
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'version' and is_nullable = 0)
				alter table cdc.change_tables alter column version int null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'source_object_id' and is_nullable = 0)
				alter table cdc.change_tables alter column source_object_id int null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'supports_net_changes' and is_nullable = 0)
				alter table cdc.change_tables alter column supports_net_changes bit null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'has_drop_pending' and is_nullable = 0)
				alter table cdc.change_tables alter column has_drop_pending bit null
			if exists (select * from sys.columns where object_id = object_id('cdc.change_tables') and name = N'create_date' and is_nullable = 0)
				alter table cdc.change_tables alter column create_date datetime null

			-- cdc.captured_columns
			if exists (select * from sys.columns where object_id = object_id('cdc.captured_columns') and name = N'column_id' and is_nullable = 0)
				alter table cdc.captured_columns alter column column_id int null
			if exists (select * from sys.columns where object_id = object_id('cdc.captured_columns') and name = N'is_computed' and is_nullable = 0)
				alter table cdc.captured_columns alter column is_computed bit null
			if not exists (select * from sys.columns where object_id = object_id('cdc.captured_columns') and name = N'masking_function')
				alter table cdc.captured_columns add masking_function nvarchar(4000) collate Latin1_General_CI_AS_KS_WS null
			declare @dmstmt nvarchar(max) = N'
			-- Populate the new masking function column if needed (e.g., if upgrading from a SQL 2016 CTP installation)
			update cdc.captured_columns
			set masking_function = m.masking_function
			from sys.masked_columns m
			inner join cdc.change_tables ct
			on m.object_id = ct.source_object_id
			where cdc.captured_columns.object_id = ct.object_id
				and cdc.captured_columns.column_id = m.column_id

			-- Update the captured columns of the change tables (_CT table) by applying the masking function of the original table, if needed
			declare
				@alter_stmt			nvarchar(max),
				@change_table_name	sysname,
				@ct_column_name		sysname,
				@masking_function	nvarchar(4000)

			declare #hcolumns cursor local fast_forward for
			select t.name as change_table_name, i.column_name, i.masking_function
			from cdc.captured_columns i
			inner join sys.tables t on t.object_id = i.object_id
			where i.masking_function is not null

			open #hcolumns
			fetch #hcolumns into @change_table_name
				,@ct_column_name
				,@masking_function
			while (@@fetch_status <> -1)
			begin
				if exists (select * from sys.tables where name = @change_table_name)
				begin
					set @alter_stmt =  N''alter table [cdc].'' + quotename(@change_table_name) + N'' alter column '' + quotename(@ct_column_name) + N'' add masked with (function = '' + quotename(@masking_function, '''''''') + N'')''
					exec (@alter_stmt)
				end
				fetch #hcolumns into @change_table_name
					,@ct_column_name
					,@masking_function
			end
			close #hcolumns
			deallocate #hcolumns'
			exec (@dmstmt)

			-- cdc.index_columns (fixed for a case of nonclustered index with included columns)
			if exists (select * from sys.columns where object_id = object_id('cdc.index_columns') and name = N'column_id' and is_nullable = 1)
			begin
				update cdc.index_columns set column_id = 0 where column_id is null
				alter table cdc.index_columns alter column column_id int not null
				if exists (select * from sys.objects where name = 'index_columns_clustered_idx' and type='PK' and parent_object_id = object_id('cdc.index_columns'))
				begin
					alter table cdc.index_columns drop constraint [index_columns_clustered_idx]
					alter table cdc.index_columns add constraint [index_columns_clustered_idx] primary key clustered (object_id ASC, index_ordinal ASC, column_id ASC)
				end
			end

			-- cdc.dll_history
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'source_object_id' and is_nullable = 0)
				alter table cdc.ddl_history alter column source_object_id int null
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'required_column_update' and is_nullable = 0)
				alter table cdc.ddl_history alter column required_column_update bit null
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'ddl_command' and is_nullable = 0)
				alter table cdc.ddl_history alter column ddl_command nvarchar(max) null
			if exists (select * from sys.columns where object_id = object_id('cdc.ddl_history') and name = N'ddl_time' and is_nullable = 0)
				alter table cdc.ddl_history alter column ddl_time datetime null

			-- cdc.lsn_time_mapping
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_begin_time' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_begin_time datetime null
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_end_time' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_end_time datetime null
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_id' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_id varbinary(10) null
			if exists (select * from sys.columns where object_id = object_id('cdc.lsn_time_mapping') and name = N'tran_begin_lsn' and is_nullable = 0)
				alter table cdc.lsn_time_mapping alter column tran_begin_lsn binary(10) null
			
			-- Regenerate the enumeration TVFs to use the modified definition of fn_cdc_check_parameters
			declare @source_schema sysname, @source_name sysname, @capture_instance sysname,
				@supports_net_changes bit, @role_name sysname, @index_name nvarchar(1000),
				@fn_all_changes nvarchar(1000), @fn_net_changes nvarchar(1000), @change_table nvarchar(1000),
				@stmt nvarchar(max), @pk_column_list nvarchar(max), @column_name sysname, @ddl_action nvarchar(10)

			declare #hinstance cursor local fast_forward for
				select object_schema_name(source_object_id) as source_schema,
					   object_name(source_object_id) as source_name,
					   capture_instance, supports_net_changes, role_name
				from cdc.change_tables
				where object_schema_name(source_object_id) is not null and
					  object_name(source_object_id) is not null

			open #hinstance
			fetch #hinstance into @source_schema, @source_name, @capture_instance,
				@supports_net_changes, @role_name

			while (@@fetch_status <> -1)
			begin
				-- Drop the enumeration TVFs if they exist
				set @fn_all_changes = N'[cdc].' + quotename( N'fn_cdc_get_all_changes' +  N'_' + @capture_instance)
				set @fn_net_changes = N'[cdc].' + quotename( N'fn_cdc_get_net_changes' +  N'_' + @capture_instance)
				set @index_name = quotename(@capture_instance + N'_CT_idx')
				set @change_table = N'[cdc].' + quotename(@capture_instance + N'_CT')
	
				if not exists (select * from sys.columns where object_id = object_id(@change_table) and name = N'__$command_id')
				begin
					set @action = N'add column __$command_id'
					set @stmt = N'alter table ' + @change_table + N' add __$command_id int null'
					exec (@stmt)
				end

				if object_id (@fn_all_changes, 'IF') is not null
				begin
					set @action = N'drop function fn_cdc_get_all_changes_<capture_instance>' 
					set @stmt = N'drop function ' + @fn_all_changes
					exec (@stmt)
				end
		
				if object_id (@fn_net_changes, 'IF') is not null
				begin
					set @action = N'drop function fn_cdc_get_net_changes_<capture_instance>' 
					set @stmt = N'drop function ' + @fn_net_changes
					exec (@stmt)
				end
		
				-- Generate the enumeration functions.
				set @action = N'sp_cdc_create_change_enumeration_functions' 
				exec [sys].[sp_cdc_create_change_enumeration_functions]
					@source_schema, @source_name, @capture_instance, @supports_net_changes

				-- Grant select permission on the enumeration functions.
				set @action = N'sp_cdc_grant_select_on_change_enumeration_functions' 
				exec [sys].[sp_cdc_grant_select_on_change_enumeration_functions]
					@capture_instance, @supports_net_changes, @role_name
					
				-- Drop the change table non-clustered index if it exists and then recreate it 
				-- This should only take place when @supports_net_changes = 1 and if new index definition differs from existing one
				-- NB: Following query should be in sync with following index definition
				-- Check if ChangeDataCaptureResolveCollationConflict feature switch is enabled
				DECLARE @diff int
				if ([sys].[fn_cdc_resolvecollationconflict_featureswitch_is_enabled]() = 1)
				begin
					EXEC sp_cdc_vupgrade_get_diff @capture_instance, @change_table, @diff output
				end
				else
				begin
					EXEC sp_cdc_vupgrade_get_diff_v2 @capture_instance, @change_table, @diff output
				end
				IF (@supports_net_changes = 1 AND @diff <> 0)
				begin

					if exists (select * from sys.indexes where object_id = object_id(@change_table) and quotename(name) = @index_name)
					begin
						set @action = N'drop non-clustered change table index'
						set @stmt = N'drop index ' + @index_name + N' on ' + @change_table
						exec (@stmt)
					end	
								
					set @pk_column_list = N' '
					
					delete #index_column_names
		
					insert into #index_column_names
					select column_name, index_ordinal
					from [cdc].[index_columns] where object_id = object_id(@change_table)

					declare #hidxcolumns cursor local fast_forward for
						select column_name
						from #index_column_names 
						order by index_ordinal
		
					open #hidxcolumns
					fetch #hidxcolumns into @column_name
	
					while (@@fetch_status <> -1)
					begin
						set @pk_column_list = @pk_column_list + quotename(@column_name) +  N' ASC, ' 

						fetch #hidxcolumns into @column_name
					end
	
					close #hidxcolumns
					deallocate #hidxcolumns
	
					set @action = N'create unique nonclustered index for change table'
					-- NB: Following index script should be in sync with above query identifying necessary changes in the index metadata
					set @stmt =  N'create unique nonclustered index ' + 
						quotename(@capture_instance + N'_CT_idx') +
						N' on ' + @change_table + N'
						( '  + @pk_column_list + N'
							[__$start_lsn] ASC,
							[__$command_id] ASC,
							[__$seqval] ASC,
							[__$operation] ASC
						) INCLUDE ( [__$update_mask] )'	 	
	
					exec (@stmt)
				end

				if (@bypass_proc_gen = 0)
				begin
					-- Alter the stored procedures to populate the change table
					-- and mark them as system procs
					set @action = N'sp_cdc_create_populate_stored_procs'
					exec [sys].[sp_cdc_create_populate_stored_procs]
						@capture_instance, N'alter' 
				end	
				
				-- Mark change table as a system object
				set @action = N'sp_MS_marksystemobject @change_table'
				exec sp_MS_marksystemobject @change_table

				fetch #hinstance into @source_schema, @source_name, @capture_instance,
					@supports_net_changes, @role_name
			end		

			close #hinstance
			deallocate #hinstance
		
		end

		if object_id(N'[cdc].[lsn_time_mapping]') is not null
		begin
			if not exists (select * from sys.columns where object_id = object_id(N'[cdc].[lsn_time_mapping]') and name = N'tran_begin_lsn')
			begin

				set @action = N'add column tran_begin_lsn'
				alter table [cdc].[lsn_time_mapping] add tran_begin_lsn binary(10) null

				set @action = N'upgrade/create procs for lsn_time_mapping'
				exec [sys].[sp_cdc_lsn_time_mapping_procs] @action = N'alter'
				
			end
		end
		
		-- Mark CDC tables as system objects
		set @action = N'sp_MS_marksystemobject cdc.change_tables'
		exec sp_MS_marksystemobject N'cdc.change_tables'

		set @action = N'sp_MS_marksystemobject cdc.ddl_history'
		exec sp_MS_marksystemobject N'cdc.ddl_history'

		set @action = N'sp_MS_marksystemobject cdc.captured_columns'
		exec sp_MS_marksystemobject N'cdc.captured_columns'

		set @action = N'sp_MS_marksystemobject cdc.index_columns'
		exec sp_MS_marksystemobject N'cdc.index_columns'
		
		set @action = N'sp_MS_marksystemobject dbo.systranschemas'
		exec sp_MS_marksystemobject N'dbo.systranschemas'
		
		set @action = N'sp_MS_marksystemobject cdc.lsn_time_mapping'
		exec sp_MS_marksystemobject N'cdc.lsn_time_mapping'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_all_changes_...]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_all_changes_...]'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_net_changes_...]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_net_changes_...]'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_all_changes_ ... ]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_all_changes_ ... ]'
		
		set @action = N'sp_MS_marksystemobject cdc.[fn_cdc_get_net_changes_ ... ]'
		exec sp_MS_marksystemobject N'cdc.[fn_cdc_get_net_changes_ ... ]'

		EXEC  master.sys.sp_cdc_vupgrade_v2 @db_name
		commit tran

	end try
	begin catch
    
		if @@trancount > @trancount
		begin
			-- If CDC opened the transaction or it is not possible 
			-- to rollback to the savepoint, rollback the transaction
			if ( @trancount = 0 ) OR ( XACT_STATE() <> 1 )
			begin
				rollback tran 
			end
			-- Otherwise rollback to the savepoint
			else
			begin
				rollback tran tr_cdc_vupgrade
				commit tran
			end
		end
		
		-- Save the error number and associated message raised in the TRY block
		select @raised_error = ERROR_NUMBER()
		select @raised_message = N'line ' + cast(ERROR_LINE() as nvarchar) + N', state ' + cast(ERROR_STATE() as nvarchar) + N', ' + ERROR_MESSAGE() 
	end catch
	
	exec sp_unset_drop_meta_objects_allowed

	if @raised_error = 0
		return (0)
		
	raiserror(22841, 16, -1, @db_name, @action, @raised_error, @raised_message)  
	return (1)

end

