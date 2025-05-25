use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_vupgrade_mergetables( @remove_repl bit = 0 )
as
begin

    set nocount on 

    declare @artnick int
    declare @objid  int
    declare @goodlen int
    declare @col_track int
    declare @article sysname
    declare @pubname sysname
    declare @artid uniqueidentifier
    declare @pubid uniqueidentifier
    declare @qualified_name nvarchar(257)
    declare @source_owner   sysname
    declare @source_object  sysname
    declare @table_name     sysname
    declare @cmd            nvarchar(1000)
    declare @default_name   nvarchar(258)
    declare @retcode    integer
    declare @snapshot_ready int
    declare @constraintname nvarchar(258)
    declare @dbname sysname
    
    declare @additive_resolver_clsid    nvarchar(60)
    declare @average_resolver_clsid     nvarchar(60)
    declare @download_resolver_clsid    nvarchar(60)
    declare @max_resolver_clsid         nvarchar(60)
    declare @mergetxt_resolver_clsid    nvarchar(60)
    declare @min_resolver_clsid         nvarchar(60)
    declare @subwins_resolver_clsid     nvarchar(60)
    declare @upload_resolver_clsid      nvarchar(60)
    declare @sp_resolver_clsid          nvarchar(60)

    declare @additive_resolver      nvarchar(80)
    declare @average_resolver       nvarchar(80)
    declare @download_resolver      nvarchar(80)
    declare @max_resolver           nvarchar(80)
    declare @mergetxt_resolver      nvarchar(80)
    declare @min_resolver           nvarchar(80)
    declare @subwins_resolver       nvarchar(80)
    declare @upload_resolver        nvarchar(80)
    declare @sp_resolver            nvarchar(80)
    declare @priority_resolver      nvarchar(80)
    declare @earlierwins_resolver   nvarchar(80)
    declare @laterwins_resolver     nvarchar(80)

    declare @column_name nvarchar(128)
    declare @column_type nvarchar(128)
    declare @alter_cmd nvarchar(max)

    DECLARE @cnt int, @idx  int    /* Loop counter, index */
    DECLARE @columnid smallint     /* Columnid-1 = bit to set */
    DECLARE @columns binary(128)   /* Temporary storage for the converted column */


    select @additive_resolver_clsid = '{0D64B1B7-1E18-48CF-A7E8-7F6D9861DD05}'
    select @average_resolver_clsid  = '{A110D612-7FB7-4471-805D-0C4FD58403D3}'
    select @download_resolver_clsid = '{56B0953F-DDF6-423E-BC15-0CCE657088FA}'
    select @max_resolver_clsid      = '{915051D3-45C3-44A2-9EEC-3BA8FA575B7C}'
    select @mergetxt_resolver_clsid = '{3310B051-64FC-47C6-A7C2-03CB54BB8C54}'
    select @min_resolver_clsid      = '{8D22F39E-EEBF-4A2C-9698-8AA84152A2D2}'
    select @subwins_resolver_clsid  = '{20C8E8F2-1017-49E8-98E5-C143833D5626}'
    select @upload_resolver_clsid   = '{790DD78E-636F-4CA9-A6F9-AAB1EACCA3DB}'
    select @sp_resolver_clsid       = '{709A9DEE-97DA-4486-A479-B94EC8229D21}'

    select @additive_resolver       = 'Microsoft SQL Server Additive Conflict Resolver'
    select @average_resolver        = 'Microsoft SQL Server Averaging Conflict Resolver'
    select @download_resolver       = 'Microsoft SQL Server Download Only Conflict Resolver'
    select @max_resolver            = 'Microsoft SQL Server Maximum Conflict Resolver'
    select @mergetxt_resolver       = 'Microsoft SQL Server Merge Text Columns Conflict Resolver'
    select @min_resolver            = 'Microsoft SQL Server Minimum Conflict Resolver'
    select @subwins_resolver        = 'Microsoft SQL Server Subscriber Always Wins Conflict Resolver'
    select @upload_resolver         = 'Microsoft SQL Server Upload Only Conflict Resolver'
    select @sp_resolver             = 'Microsoft SQLServer Stored Procedure Resolver'
    select @priority_resolver       = 'Microsoft SQL Server Priority Column Resolver'
    select @earlierwins_resolver    = 'Microsoft SQL Server DATETIME (Earlier Wins) Conflict Resolver'
    select @laterwins_resolver      = 'Microsoft SQL Server DATETIME (Later Wins) Conflict Resolver'

    select @dbname = db_name()
    
    exec @retcode = sys.sp_MSreplcheck_publish
    if (@retcode <> 0 or @@error <> 0)
        return 1

    
    if object_id('sysmergearticles') is not NULL
    begin
    -- Update to 110 resolver clsids
        update dbo.sysmergearticles set resolver_clsid =
            case article_resolver
                when @additive_resolver     then @additive_resolver_clsid
                when @average_resolver      then @average_resolver_clsid
                when @download_resolver     then @download_resolver_clsid
                when @max_resolver          then @max_resolver_clsid
                when @mergetxt_resolver     then @mergetxt_resolver_clsid
                when @min_resolver          then @min_resolver_clsid
                when @subwins_resolver      then @subwins_resolver_clsid
                when @upload_resolver       then @upload_resolver_clsid
                when @sp_resolver           then @sp_resolver_clsid
                when @priority_resolver     then @max_resolver_clsid
                when @earlierwins_resolver  then @min_resolver_clsid
                when @laterwins_resolver    then @max_resolver_clsid
                else resolver_clsid
            end
    end

    -- Check if upgrade is needed. If the database is 90 then dispatch to sp_MSmerge_upgrade_from_90rtm
    if object_id('sysmergepublications') is not NULL
    begin
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'use_partition_groups')
        begin
            exec @retcode = sys.sp_MSmerge_upgrade_from_90rtm  
            return @retcode 
        end
    end

    begin tran
    save tran vupgrade_mergetables
    
    /*
     * dbo.sysmergepublications
    */
    if object_id('sysmergepublications') is not NULL
    begin
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'backward_comp_level')
        begin
            alter table dbo.sysmergepublications add backward_comp_level int not NULL default 10 -- defaulted to 70 RTM
            if @@error<>0 goto error
        end
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = N'allow_partition_realignment' and is_nullable = 0)
        	alter table dbo.sysmergepublications alter column allow_partition_realignment bit null
        
    end

    if @remove_repl=0 and object_id('sysmergepublications') is not NULL
    begin
        /*
         * No direct select on sysmergepublications for public
        */
        -- default_access default no longer used 
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and
            name = 'default_access')
        begin
            -- Get the name of the default associated with default_access and drop it
            select @default_name = QUOTENAME(object_name(constid)) 
                from sysconstraints    
                where id = object_id('dbo.sysmergepublications') 
                    and col_name(id, colid) = N'default_access'
            if @default_name is not null
            begin
                -- Drop the default
                exec (N'alter table dbo.sysmergepublications drop constraint ' + @default_name) 
                if @@error<>0 goto error
            end    

            alter table dbo.sysmergepublications drop column default_access
            if @@error<>0 goto error
        end 

        /* 
         * Since the ftp_address is now required to enable a publication for internet, publications 
         * that were enabled for internet can not be upgraded automatically. So the enabled for 
         * internet option is reset in the upgrade process. New FTP columns added later in script. (Shiloh)
        */
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and 
           name = 'enabled_for_internet')
        begin
            if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'ftp_address')
            begin
                exec(N'update dbo.sysmergepublications set enabled_for_internet = 0')
            end
            else
            begin
                exec(N'update dbo.sysmergepublications set enabled_for_internet = 0 where ftp_address is null')
            end
            if @@error<>0 goto error
        end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'snapshot_in_defaultfolder', col_type = 'bit default 0 not null'-- Portable snapshot (Shiloh)
                                                             union all
                                                             select col_name = 'alt_snapshot_folder', col_type = 'nvarchar(255) null'
                                                             union all
                                                             select col_name = 'pre_snapshot_script', col_type = 'nvarchar(255) null'-- Snapshot pre/post scripts (Shiloh)
                                                             union all
                                                             select col_name = 'post_snapshot_script', col_type = 'nvarchar(255) null'
                                                             union all
                                                             select col_name = 'compress_snapshot', col_type = 'bit default 0 not null'-- Snapshot compression (Shiloh)
                                                             union all
                                                             select col_name = 'ftp_address', col_type = 'sysname null'-- Ftp support at publication level (Shiloh)
                                                             union all
                                                             select col_name = 'ftp_port', col_type = 'int not null default 21'
                                                             union all
                                                             select col_name = 'ftp_subdirectory', col_type = 'nvarchar(255) null'
                                                             union all
                                                             select col_name = 'ftp_login', col_type = 'sysname null default N''anonymous'''
                                                             union all
                                                             select col_name = 'ftp_password', col_type = 'nvarchar(524) null'
                                                             union all
                                                             select col_name = 'conflict_retention', col_type = 'int null' -- Conflict retention  (Shiloh)
                                                             union all
                                                             select col_name = 'keep_before_values', col_type = 'int null'-- Keep partition changes (SQL7.0 SP2 )
                                                             union all
                                                             select col_name = 'allow_subscription_copy', col_type = 'bit null default 0'-- Attach & Go (Shiloh)
                                                             union all
                                                             select col_name = 'allow_synctoalternate', col_type = 'bit null default 0'-- Sync to any hub (Shiloh)
                                                             union all
                                                             select col_name = 'web_synchronization_url', col_type = 'nvarchar(500) null'-- WebSync URL (Yukon)
                                                             union all
                                                             select col_name = 'retention_period_unit', col_type = 'tinyint default 0 not null'-- 0=day, 1=week, 2=month, 3=year, 4=hour, 5=minute
                                                             union all
                                                             select col_name = 'validate_subscriber_info', col_type = 'nvarchar(500) NULL'-- Dynamic partition rvalue validation (Shiloh)
                                                             union all
                                                             select col_name = 'ad_guidname', col_type = 'sysname NULL'-- Active directory registration for publications (Shiloh)
                                                             union all
                                                             select col_name = 'max_concurrent_merge', col_type = 'int not NULL default 0'-- max_concurrent_merge control the max # of concurrent merge process at publisher side (Shiloh)
                                                             union all
                                                             select col_name = 'max_concurrent_dynamic_snapshots', col_type = 'int not NULL default 0'-- Maximum number of current dynamic snapshot sessions
                                                             union all
                                                             select col_name = 'use_partition_groups', col_type = 'smallint NULL'
                                                             union all
                                                             select col_name = 'dynamic_filters_function_list', col_type = 'nvarchar(500) NULL'-- Semi-colon delimited list of functions used in all dynamic filters used in this publication
                                                             union all
                                                             select col_name = 'replicate_ddl', col_type = 'int not NULL default 0'-- Bitmask on how this publication accepts new objects
                                                             union all
                                                             select col_name = 'partition_id_eval_proc', col_type = 'sysname NULL'-- Partition id evaluation proc for this publication
                                                             union all
                                                             select col_name = 'publication_number', col_type = 'smallint identity NOT NULL' -- publication_number for this publication (just a mapped value to be used locally instead of the 16-byte guid)
                                                             union all
                                                             select col_name = 'allow_subscriber_initiated_snapshot', col_type = 'bit not NULL default 0'-- allow_subscriber_initiated_snapshot column
                                                             union all
                                                             select col_name = 'allow_partition_realignment', col_type = 'bit not NULL default 1' -- allow_partition_realignment column
                                                             union all
                                                             select col_name = 'generation_leveling_threshold', col_type = 'int null default 1000' -- generation leveling threshold
                                                             union all
                                                             select col_name = 'automatic_reinitialization_policy', col_type = 'bit not null default 0'-- whether or not to upload first on reinits that are triggered by certain publication/article property changes
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergepublications', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
            select @alter_cmd = 'alter table dbo.sysmergepublications add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs

        -- allow web sync (Yukon)
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'allow_web_synchronization')
        begin
            alter table dbo.sysmergepublications add allow_web_synchronization bit null default 0
            if @@error<>0 goto error

            -- we want to set allow_web_synchronization for ssce subscribers. So if we find a 
            -- character mode snapshot type we will set this property.
            exec('update dbo.sysmergepublications set allow_web_synchronization = 1 where sync_mode = 1')
            if @@error<>0 goto error
        end
        
        --insure that index nc2sysmergepublications exists on status
        if exists( select * from sys.indexes where name = 'nc2sysmergepublications' AND 
          			object_id = object_id('dbo.sysmergepublications') )
        begin
          	drop index nc2sysmergepublications on dbo.sysmergepublications
        end
     	 if exists(select * from syscolumns where id = object_id('sysmergepublications') and name = 'status')
	 begin
	 	create index nc2sysmergepublications on dbo.sysmergepublications(status)
	 end
 
				   
	 --insure that default on generation_leveling_threshold is 1000 instead of 0 (or anything else)
	declare @defaultname sysname
	select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc 
              	on sysdc.parent_object_id = sysc.object_id
                        and sysdc.parent_column_id = sysc.column_id
      		where sysdc.parent_object_id = object_id('sysmergepublications')
		       and sysc.name = 'generation_leveling_threshold' 
	 if @defaultname is not null
	 begin
		select @alter_cmd = 'alter table dbo.sysmergepublications drop constraint ' + QUOTENAME(@defaultname)
		exec (@alter_cmd) -- drop old default		
	 end
	 if exists(select * from syscolumns where id = object_id('sysmergepublications') and name = 'generation_leveling_threshold')
	 begin
	 	select @alter_cmd = 'alter table dbo.sysmergepublications add default 1000 for generation_leveling_threshold'
		 exec (@alter_cmd) -- add new default
	 end
	
        -- conflict logging on both publisher and subscriber
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'decentralized_conflicts')
        begin
            alter table dbo.sysmergepublications add decentralized_conflicts int null
            if @@error<>0 goto error

            -- before upgrade, centralized_conflicts==1 means centralized logging,
            -- centralized_conflicts==0 means decentralized logging.
            -- We now map this to the two explicit columns.
            exec ('update dbo.sysmergepublications set decentralized_conflicts=1 where centralized_conflicts=0')
            if @@error<>0 goto error
            exec ('update dbo.sysmergepublications set decentralized_conflicts=0 where centralized_conflicts=1')
            if @@error<>0 goto error
        end
        
        /*
         * sysmergepublications ftp_password
         * no need to upgrade passwords since this column is new in 8.0.
        */
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'ftp_password' and max_length = '1048')
        begin
            declare @cmptlevel tinyint
            select @cmptlevel = cmptlevel from master.dbo.sysdatabases where name = @dbname collate database_default
            if @cmptlevel < 70
            begin
                raiserror (15048, -1, -1, 70, 70, 70, 80)
            end
            else
            begin
                exec( 'alter table dbo.sysmergepublications alter column ftp_password nvarchar(524)' )
                if @@error <> 0
                    goto error
            end
        end

        -- in Yukon snapshot_jobid column has been moved from MSmerge_replinfo to sysmergepublications
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'snapshot_jobid')
        begin
            -- we cannot do alter table and update in the same batch so make this a dynamic sql
            alter table dbo.sysmergepublications add snapshot_jobid  binary(16) NULL
            if @@error <> 0 goto error
            -- get the values of snapshot_jobid from MSmerge_replinfo
            if exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'snapshot_jobid')
            begin
                -- need to exec update in diff process space to avoid syntax error on deferred name resolution at time of proc exec
                exec ('update dbo.sysmergepublications
                set snapshot_jobid = r.snapshot_jobid 
                from dbo.sysmergepublications p, dbo.MSmerge_replinfo r
                where r.repid = p.pubid')
                if @@error <> 0
                    goto error
            end
        end
        
        -- in Yukon distributor column has been moved from sysmergesubscriptions to sysmergepublications
        -- the following is only useful in shiloh to yukon upgrade. In 70 the column distributor did not even exist
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'distributor')
        begin
            -- we will move the value from sysmergesubscriptions to sysmergepublications
            alter table dbo.sysmergepublications add distributor sysname NULL
            if @@error<>0 goto error
            -- get the values of distributor from MSmerge_replinfo
            if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'distributor')
             and exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'publication')
            begin
                -- need to exec update in diff process space to avoid syntax error on deferred name resolution at time of proc exec
                -- add the colums for republisher's publications into sysmergepublications
                exec ('insert into dbo.sysmergepublications (pubid, name, publisher_db, publisher, distributor)
                select distinct s.pubid, s.publication, s.db_name, s.subscriber_server, s.distributor
                    from dbo.sysmergesubscriptions s, dbo.sysmergepublications p
                    where s.subid = s.pubid and s.pubid not in (select pubid from dbo.sysmergepublications)')
                 if @@error <> 0
                    goto error
                exec ('update dbo.sysmergepublications 
                        set distributor = s.distributor
                        from dbo.sysmergesubscriptions s, dbo.sysmergepublications p
                        where s.subid = s.pubid and p.pubid = s.pubid')
                if @@error <> 0
                    goto error
            end
            else
            begin
                -- this is probably a 70 upgrade
                exec ('update dbo.sysmergepublications 
                        set distributor = publisher')
                if @@error <> 0
                    goto error
            end
        end

        -- dynamic_snapshot_queue_timeout column this was in Yukon beta2 but has been removed since
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_queue_timeout')
        begin
            -- now drop the default constraint
            select @constraintname = quotename(name) from sys.default_constraints where parent_object_id = object_id('dbo.sysmergepublications') and parent_column_id =
            	(select column_id from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_queue_timeout')

            exec ('alter table dbo.sysmergepublications drop constraint ' +  @constraintname)   
            if @@error<>0 goto error
            
            alter table dbo.sysmergepublications drop column dynamic_snapshot_queue_timeout    
            if @@error<>0 goto error
        end

        -- dynamic_snapshot_ready_timeout column this was in Yukon beta2 but has been removed since
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_ready_timeout')
        begin
            -- now drop the default constraint
            select @constraintname = quotename(name) from sys.default_constraints where parent_object_id = object_id('dbo.sysmergepublications') and parent_column_id =
            	(select column_id from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_ready_timeout')

            exec ('alter table dbo.sysmergepublications drop constraint ' +  @constraintname)   
            if @@error<>0 goto error

            alter table dbo.sysmergepublications drop column dynamic_snapshot_ready_timeout    
            if @@error<>0 goto error
        end
    end -- end sysmergepublications modifications

    -- dbo.MSmerge_contents
    if object_id('dbo.MSmerge_contents') is not null and @remove_repl=0
    begin
    	-- insure that index nc4Msmerge_contents exists on rowguid
	if exists( select * from sys.indexes where name = 'nc4MSmerge_contents' and 
		object_id = object_id('MSmerge_contents') )
	begin
		drop index nc4MSmerge_contents on dbo.MSmerge_contents
	end
	if exists(select * from syscolumns where id = object_id('MSmerge_contents') and name = 'rowguid')
	begin
		create index nc4MSmerge_contents on dbo.MSmerge_contents(rowguid)
	end
    end

    -- dbo.MSmerge_genhistory
    if object_id('dbo.MSmerge_genhistory') is not null and @remove_repl=0
    begin
	-- insure that index nc2Msmerge_genhistory exists on rowguid
	if exists( select * from sys.indexes where name = 'nc2MSmerge_genhistory' and 
		object_id = object_id('MSmerge_genhistory') )
	begin
		drop index nc2MSmerge_genhistory on dbo.MSmerge_genhistory
	end
	if exists(select * from syscolumns where id = object_id('MSmerge_genhistory') and name = 'genstatus')
		and exists(select * from syscolumns where id = object_id('MSmerge_genhistory') and name = 'art_nick')
		and exists(select * from syscolumns where id = object_id('MSmerge_genhistory') and name = 'changecount')
	begin
		create  index nc2MSmerge_genhistory on MSmerge_genhistory(genstatus, art_nick,changecount) 
	end
    end

    /* 
     * MSmerge_history
     * Add new unique idx for correctness iff there are no uniqueness violations. Drop old
     * index in favor of new column order in this index. Add new non-clustered index as needed.
    */
    if exists( select * from sys.indexes where name = 'nc1MSmerge_history' AND 
                object_id = OBJECT_ID('MSmerge_history'))
    begin
    	drop index dbo.MSmerge_history.nc1MSmerge_history
    end
    if exists(select * from sys.columns where object_id = object_id('MSmerge_history') and name = 'session_id')
    	and exists (select * from sys.columns where object_id = object_id('MSmerge_history') and name = 'timestamp')
    begin
    	create nonclustered index nc1MSmerge_history on MSmerge_history(session_id, timestamp)
    end

    -- dbo.MSmerge_replinfo
    if @remove_repl=0 and (object_id('MSmerge_replinfo') is not NULL and
    exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'replnickname'))
    begin
        -- recgen is int in Shiloh and before, bigint in Yukon and after
        if 56 = (select system_type_id from sys.columns where 
                    object_id = object_id('MSmerge_replinfo') and name = 'recgen')
        begin
            alter table dbo.MSmerge_replinfo alter column recgen bigint null
            if @@error<>0 goto error
        end

        -- sentgen is int in Shiloh and before, bigint in Yukon and after
        if 56 = (select system_type_id from sys.columns where 
                    object_id = object_id('MSmerge_replinfo') and name = 'sentgen')
        begin
            alter table dbo.MSmerge_replinfo alter column sentgen bigint null
            if @@error<>0 goto error
        end

        -- replnickname is int in Shiloh and before, binary(6) in Yukon and after
        if 56 = (select system_type_id from sys.columns where 
                    object_id = object_id('MSmerge_replinfo') and name = 'replnickname')
        begin
            begin tran
            save tran tran_replinfonick80to90
                alter table dbo.MSmerge_replinfo alter column replnickname binary(6) not null
                if @@error<>0 goto err_replinfonick80to90
                exec ('update dbo.MSmerge_replinfo set replnickname= substring(replnickname, 6, 1) + substring(replnickname, 5, 1) + substring(replnickname, 4, 1) + substring(replnickname, 3, 1) + substring(replnickname, 2, 1) + substring(replnickname, 1, 1)')
                if @@error<>0 goto err_replinfonick80to90
            commit tran
            goto after_replinfonick80to90

            err_replinfonick80to90:
            rollback tran tran_replinfonick80to90
            commit tran
            goto error
        end

        after_replinfonick80to90:

        -- this column and its values been added to sysmergepublications in the sysmergepublications if block
        if exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'snapshot_jobid')
        begin
            alter table dbo.MSmerge_replinfo drop column snapshot_jobid
            if @@error <> 0 goto error
        end
        
        -- hostname column added for Yukon
        if not exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'hostname')
        begin
            alter table dbo.MSmerge_replinfo add hostname sysname NULL
            if @@error <> 0 goto error
        end
    end -- dbo.MSmerge_replinfo
    
    /*
    * dbo.sysmergesubscriptions
    */
    -- the following modifications to sysmergesubscriptions have to be done even if replication is being removed
    if (object_id('sysmergesubscriptions') is not NULL)
    begin
        -- subscriber_server (Shiloh) 
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'subscriber_server')
        begin
            alter table dbo.sysmergesubscriptions add subscriber_server sysname null
            if @@error <> 0 goto error
            
            -- need to exec update in diff process space to avoid syntax error on deferred name resolution at time of proc exec
            exec( N'update dbo.sysmergesubscriptions set subscriber_server = 
                        (select srvname from master.dbo.sysservers where srvid = dbo.sysmergesubscriptions.srvid)' )
            if @@error <> 0 goto error
        end

        -- last_makegeneration_datetime
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'last_makegeneration_datetime')
        begin
            alter table dbo.sysmergesubscriptions add last_makegeneration_datetime datetime null
            if @@error <> 0 goto error            
        end



        if exists (select * from sysconstraints where id = object_id('dbo.sysmergesubscriptions') and 
                            object_name(constid) = 'unique_pubsrvdb')
        begin
            alter table dbo.sysmergesubscriptions drop constraint unique_pubsrvdb
            if @@error <> 0 goto error
        end
        
        IF EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc2sysmergesubscriptions' AND
                        id = object_id('dbo.sysmergesubscriptions') )
        begin
            drop index nc2sysmergesubscriptions on dbo.sysmergesubscriptions 
            if @@error <> 0 goto error
        end
        
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'srvid')
        begin
            
            alter table dbo.sysmergesubscriptions drop column srvid
            if @@error <> 0 goto error
        end

        if not exists (select * from sysconstraints where id = object_id('dbo.sysmergesubscriptions') and 
                            object_name(constid) = 'unique_pubsrvdb')
        begin
            exec(N'alter table dbo.sysmergesubscriptions 
                    add constraint unique_pubsrvdb 
                    unique nonclustered (pubid, subscriber_server, db_name)')
            if @@error <> 0 goto error
        end
    end
    
    if @remove_repl=0 and (object_id('sysmergesubscriptions') is not NULL)
    begin
        -- rename partnerid to replicastate... since we will be copying the table
        -- we will do this first so that the remaining changes will not need to be copied
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'partnerid')
        begin
            -- because sp_rename does not allow the renaming of tables marked as system objects
            -- we have to take this round about way of doing things. This table should always be
            -- relatively small so perf here is not a really big concern and should not be affected
            --  exec sys.sp_rename @objname = 'sysmergesubscriptions.partnerid', @newname = 'replicastate', @objtype = 'COLUMN'
            --  if @@error <> 0 goto error
            if object_id(N'sysmergesubscriptions_tmp_name') is not null
            begin
                drop table sysmergesubscriptions_tmp_name
                if @@error <> 0 goto error
            end

            select * 
                into sysmergesubscriptions_tmp_name 
                from dbo.sysmergesubscriptions
            if @@error <> 0 goto error

            exec sys.sp_rename @objname = 'sysmergesubscriptions_tmp_name.partnerid', @newname = 'replicastate', @objtype = 'COLUMN'
            if @@error <> 0
            begin
                drop table sysmergesubscriptions_tmp_name
                goto error
            end

            drop table sysmergesubscriptions
            if @@error <> 0 goto error

            exec sys.sp_rename @objname = 'sysmergesubscriptions_tmp_name', @newname = 'sysmergesubscriptions'
            if @@error <> 0 
            begin
                drop table sysmergesubscriptions_tmp_name
                goto error
            end
  		

            -- recreate indexes
            create unique clustered index uc1sysmergesubscriptions on dbo.sysmergesubscriptions (subid) 
            if @@error <> 0 goto error

            create index nc2sysmergesubscriptions on dbo.sysmergesubscriptions (subscriber_server, db_name)
            if @@error <> 0 goto error

            -- mark as system object
            exec sp_MS_marksystemobject 'sysmergesubscriptions'
            if @@error <> 0 goto error
        end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'use_interactive_resolver', col_type = 'bit NOT NULL default 0'-- Interactive resolver support (Shiloh)
                                                             union all
                                                             select col_name = 'validation_level', col_type = 'int NOT NULL default 0'-- merge validation level (Shiloh)
                                                             union all
                                                             select col_name = 'resync_gen', col_type = 'bigint not NULL default -1'
                                                             union all
                                                             select col_name = 'attempted_validate', col_type = 'datetime NULL' -- date of the last attempted validate (Shiloh)
                                                             union all
                                                             select col_name = 'last_sync_status', col_type = 'int NULL'-- status of the last sync (Shiloh)
                                                             union all
                                                             select col_name = 'last_sync_date', col_type = 'datetime NULL'-- date of the last sync (Shiloh)
                                                             union all
                                                             select col_name = 'last_sync_summary', col_type = 'sysname NULL'-- summary message of the last sync (Shiloh)
                                                             union all
                                                             select col_name = 'metadatacleanuptime', col_type = 'datetime not NULL default getdate()'-- metadata cleanup time
                                                             union all
                                                             select col_name = 'cleanedup_unsent_changes', col_type = 'bit NOT NULL default 0'-- cleanedup_unsent_changes(Yukon)
                                                             union all
                                                             select col_name = 'replica_version', col_type = 'int NOT NULL default 60'-- replica_version (Yukon)
                                                             union all
                                                             select col_name = 'supportability_mode', col_type = 'int NOT NULL default 0'-- supportability_mode (Yukon)
                                                             union all
                                                             select col_name = 'application_name', col_type = 'sysname NULL'-- application_name and subscriber_number added in yukon
                                                             union all 
                                                             select col_name = 'subscriber_number', col_type = 'int identity not NULL'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergesubscriptions', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin        
            select @alter_cmd = 'alter table dbo.sysmergesubscriptions add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 
            begin
            		goto error
            end
            
        end
        close colcurs
        deallocate colcurs

		-- drop old views 
		declare @old_view_name sysname
		declare drop_old_views_cursor cursor LOCAL FAST_FORWARD FOR
		select name from sys.objects where (name like 'ctsv_%' or name like 'tsvw_%') AND type ='V' and  ObjectProperty(object_id, 'IsMSShipped')=1
		for read only
        open drop_old_views_cursor
        fetch drop_old_views_cursor into @old_view_name
		while(@@fetch_status <> -1)
		begin
			declare @drop_view_cmd nvarchar(max)
			select @drop_view_cmd = N'drop view ' + QUOTENAME(@old_view_name) 
			exec(@drop_view_cmd)
			if @@error <> 0 goto error
			fetch drop_old_views_cursor into @old_view_name
		end	
		close drop_old_views_cursor
        deallocate drop_old_views_cursor


        IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc2sysmergesubscriptions' AND
            id = object_id('dbo.sysmergesubscriptions') )
        begin
            create index nc2sysmergesubscriptions on dbo.sysmergesubscriptions (subscriber_server, db_name)
            if @@error <> 0 goto error
        end

        -- Remove alternate_pubid column from sysmergesubscriptions (Shiloh)
        -- This column is dropped in 8.0 Beta 2
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'alternate_pubid')
        begin
            alter table dbo.sysmergesubscriptions drop column alternate_pubid
            if @@error <> 0 goto error
        end

        -- this column and its values been added to sysmergepublications in the sysmergepublications if block (yukon)
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'distributor')
        begin
            alter table sysmergesubscriptions drop column distributor
            if @@error <> 0 goto error
        end

        -- this column and its values been added to sysmergepublications in the sysmergepublications if block (yukon)
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'publication')
        begin
            alter table sysmergesubscriptions drop column publication
            if @@error <> 0 goto error
        end
    end 



    -- move of columns from sysmergesubscriptions to MSmerge_replinfo and vice versa for the sake of 
    -- better consistency and subscriber tracking.
    if @remove_repl=0 and object_id('sysmergesubscriptions') is not NULL and
         (object_id('MSmerge_replinfo') is not NULL and
         exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'replnickname'))
    begin
        select * 
            into #sysmergesubscriptions
            from sysmergesubscriptions
        if @@error <> 0 goto error
        
        select * 
            into #MSmerge_replinfo
            from MSmerge_replinfo
        if @@error <> 0 goto error
        begin tran
        save tran tran_upgrademergesubtables
            drop table dbo.sysmergesubscriptions
            if @@error<>0
                goto err_upgrademergesubtables

            drop table dbo.MSmerge_replinfo
            if @@error<>0
                goto err_upgrademergesubtables

            -- this creates the sysmergesubscription and MSmerge_replinfo tables with new schema
            exec @retcode = sys.sp_MSmerge_create_sub_table
            if @retcode<>0 or @@error<>0
                goto err_upgrademergesubtables
            -- insert the values for the new set of columns
            exec('insert into dbo.sysmergesubscriptions
                (subscriber_server, db_name, pubid, datasource_type, subid, replnickname,
                 replicastate, status, subscriber_type, subscription_type, sync_type,
                 description, priority, recgen, recguid, sentgen, sentguid,
                 schemaversion, schemaguid, last_validated, attempted_validate,
                 last_sync_date, last_sync_status, last_sync_summary, 
                 metadatacleanuptime, cleanedup_unsent_changes)
            select sub.subscriber_server, sub.db_name, sub.pubid, sub.datasource_type, sub.subid, rep.replnickname,
                   sub.replicastate, sub.status, sub.subscriber_type, sub.subscription_type, sub.sync_type,
                   sub.description, sub.priority, rep.recgen, rep.recguid, rep.sentgen, rep.sentguid,
                   rep.schemaversion, rep.schemaguid, sub.last_validated, sub.attempted_validate,
                   sub.last_sync_date, sub.last_sync_status, sub.last_sync_summary,
                   sub.metadatacleanuptime, sub.cleanedup_unsent_changes
            from #sysmergesubscriptions sub, #MSmerge_replinfo rep
            where sub.subid = rep.repid')
            if @retcode<>0 or @@error<>0
                goto err_upgrademergesubtables
            exec('insert into dbo.MSmerge_replinfo
                (repid, use_interactive_resolver, validation_level, resync_gen, 
                  login_name, merge_jobid)
            select rep.repid, sub.use_interactive_resolver, sub.validation_level, sub.resync_gen, 
                  sub.login_name, rep.merge_jobid
            from #sysmergesubscriptions sub, #MSmerge_replinfo rep
            where sub.subid = rep.repid')
            if @retcode<>0 or @@error<>0
                goto err_upgrademergesubtables		

        commit tran
        goto after_upgrademergesubtables

        err_upgrademergesubtables:
        rollback tran tran_upgrademergesubtables
        commit tran
        goto error
        
        after_upgrademergesubtables: 
        -- if we got here everything was successful.
        drop table #sysmergesubscriptions
        drop table #MSmerge_replinfo
    end

    if @remove_repl=0 and object_id('dbo.sysmergesubscriptions') is not NULL
    begin
	 --Dropping columns use_interactive_resolver, validation_level, resync_gen
	 if exists (select * from sys.columns where name = N'use_interactive_resolver' and object_id = object_id('dbo.sysmergesubscriptions'))
	 begin 
	 	-- drop constraint on this if it exists
	 	select @defaultname = null --temp stores constraint name to be removed
		select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
				(sysdc.parent_object_id = sysc.object_id and 
					sysdc.parent_column_id = sysc.column_id)		
			where sysc.object_id = object_id('dbo.sysmergesubscriptions') and
				sysc.name = 'use_interactive_resolver'
	 	if @defaultname is not null
	 	begin
	 		select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop constraint ' + QUOTENAME(@defaultname)
	 		exec (@alter_cmd)
	 	end
	 	-- drop column for Yukon
	 	select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop column use_interactive_resolver'
	 	exec (@alter_cmd)
	 end
	 if exists (select * from sys.columns where name = N'validation_level' and object_id = object_id('dbo.sysmergesubscriptions'))	
	 begin
	 	-- drop constraint on this if it exists
	 	select @defaultname = null -- temp stores constraint name to be removed
		select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
				(sysdc.parent_object_id = sysc.object_id and 
					sysdc.parent_column_id = sysc.column_id)		
			where sysc.object_id = object_id('dbo.sysmergesubscriptions') and
					sysc.name = 'validation_level'
	 	if @defaultname is not null
	 	begin
	 		select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop constraint ' + QUOTENAME(@defaultname)
	 		exec (@alter_cmd)
	 	end 
	 	-- drop column for Yukon
	 	select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop column validation_level'
	 	exec (@alter_cmd)
	 end
	 if exists (select * from sys.columns where name = N'resync_gen' and object_id = object_id('dbo.sysmergesubscriptions'))
	 begin	
	 	-- constraint (default) must be deleted before we can delete resync_gen
		select @defaultname = null --temp stores constraint name to be removed
	 	select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
				(sysdc.parent_object_id = sysc.object_id and 
					sysdc.parent_column_id = sysc.column_id)		
			where sysc.object_id = object_id('dbo.sysmergesubscriptions') and
					sysc.name = 'resync_gen'
	 	if @defaultname is not null
	 	begin
	 		select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop constraint ' + QUOTENAME(@defaultname)
	 		exec (@alter_cmd)
	 	end
	 	-- drop column for Yukon
	 	select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop column resync_gen'
	 	exec (@alter_cmd)
	 end
	 	
	 --insure index nc3sysmergesubscriptions exists on replnickname
	 if exists( select * from sys.indexes where name = 'nc3sysmergesubscriptions' and object_id = object_id('dbo.sysmergesubscriptions') )
	 begin
	 	drop index dbo.sysmergesubscriptions.nc3sysmergesubscriptions
	 end
	 if exists(select * from syscolumns where id = object_id('dbo.sysmergesubscriptions') and name = 'replnickname')
	 begin  -- if column replnickname exists create the index on it
	 	create index nc3sysmergesubscriptions on dbo.sysmergesubscriptions(replnickname)
	 end
    end


    /*
     * dbo.sysmergearticles
    */
    -- the following have to be done for sp_MSremovedbreplication to work correctly
    -- even we are going to remove replication
    if object_id('sysmergearticles') is not NULL
    begin
        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'lightweight', col_type = 'bit not null default 0'
                                                             union all
                                                             select col_name = 'before_upd_view_objid', col_type = 'int NULL'
                                                             union all                                                             
                                                             select col_name = 'metadata_select_proc', col_type = 'sysname NULL'
                                                             union all                                                             
                                                             select col_name = 'delete_proc', col_type = 'sysname NULL'
                                                             union all                                                             
                                                             select col_name = 'before_image_objid', col_type = 'int NULL'-- Keep partition changes (SQL7.0 SP2)
                                                             union all                                                             
                                                             select col_name = 'before_view_objid', col_type = 'int NULL'
                                                             union all                                                             
                                                             select col_name = 'preserve_rowguidcol', col_type = 'bit not null default 1'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergearticles', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
            select @alter_cmd = 'alter table dbo.sysmergearticles add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = N'delete_tracking' and is_nullable = 0)
        	alter table dbo.sysmergearticles alter column delete_tracking bit null
    end
    
    if @remove_repl=0 and object_id('sysmergearticles') is not NULL
    begin
        -- Set all invalid dbo.sysmergearticles.sync_objid to the corresponding 
        -- objid, this will allow regeneration of article procs to succeed
        update dbo.sysmergearticles 
           set sync_objid = objid 
         where object_name(sync_objid) is null
        if @@error <> 0 goto error

        -- Make sure that article type in dbo.sysmergearticles is not null 
        -- for upgraded republisher
        update dbo.sysmergearticles
        set type = 0x0a
        where type is null
        if @@error <> 0 goto error

        -- Turn on the trigger schema option, FK schema option, check 
        -- constraint schema option, and default schema option    by default 
        -- so merge articles will retain the old scripting behaviour (Shiloh)
        -- Also reset the 0x8000 option (PKUK as constraints) as it is 
        -- deprecated starting from yukon. 
        update dbo.sysmergearticles 
            set schema_option = (convert(bigint, schema_option) | convert(bigint, 0x00000F00)) & ~convert(bigint, 0x00008000)
            from sys.objects so
            inner join dbo.sysmergearticles sa
            on so.object_id = sa.objid
            where so.type = 'U' -- table articles only    
        if @@error <> 0 goto error

        -- Add new non-clustered idx on nickname.
        if not exists (select * from sys.indexes where name = 'nc1sysmergearticles')
        begin
            create nonclustered index nc1sysmergearticles on dbo.sysmergearticles(nickname) 
            if @@error <> 0 goto error
        end

        -- Resolver info column datatype change 
        if exists( select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = 'resolver_info' )
        begin
            alter table dbo.sysmergearticles alter column resolver_info nvarchar(517) NULL
            if @@error <> 0 goto error
        end

        exec @retcode = sys.sp_MSUpgradeConflictTable
        if @@ERROR<>0 or @retcode<>0
            goto error

        if object_id('MSmerge_delete_conflicts') is not NULL
        begin
            drop table dbo.MSmerge_delete_conflicts
            if @@error <> 0 goto error
        end

        /* Update the columns column sysmergearticles by counting the number of columns int the
           table. 70 did not have vertical partitioning so the columns column is NULL */
        declare articlescurs cursor LOCAL FAST_FORWARD 
        for (select sma.name, sma.objid, sma.pubid from dbo.sysmergearticles sma 
                    where sma.columns is NULL and sys.fn_MSmerge_islocalpubid(pubid) = 1)
        
        for read only
        open articlescurs
        fetch articlescurs into @article, @objid, @pubid
        while(@@fetch_status <> -1)
        begin


            SELECT @cnt = max(column_id), @idx = 1 FROM sys.columns WHERE object_id = @objid 
            SELECT @columns = NULL
            WHILE @idx <= @cnt
            BEGIN
                /* to make sure column holes will not be included */
                if exists (select * from sys.columns where column_id=@idx and object_id=@objid and 
                    (is_computed<>1 and system_type_id <> type_id('timestamp')))
                begin
                    exec sys.sp_MSsetbit @bm=@columns OUTPUT, @coltoadd=@idx, @toset = 1
                    if @@ERROR<>0 or @retcode<>0
                    begin
                        close articlescurs
                        deallocate articlescurs
                        goto error
                    end

                end
                SELECT @idx = @idx + 1
            END
            UPDATE dbo.sysmergearticles SET columns = @columns WHERE name = @article AND pubid = @pubid

            fetch articlescurs into @article, @objid, @pubid
        end

        close articlescurs
        deallocate articlescurs

        if @@error <> 0 goto error

        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and
                            name = 'excluded_col_count')
        begin
            -- make sure 7.5's column bit map does not get messed up.
            if exists (select * from dbo.sysmergearticles)
            begin
            -- enough to hold 246 columns in one table - after upgrade all columns are in partition 
            -- as in sphinx there is no way to kick any of them out.
            -- in case a publication exists in downlevel subscriber without having ran snapshot.
            -- missing_col_count would be NULL, which can cause problems when being upgraded to
            -- latest version. 
                update dbo.sysmergearticles set missing_col_count=0,missing_cols=0x00 where missing_col_count is NULL
                if @@error <> 0 goto error
            end

            alter table dbo.sysmergearticles add excluded_col_count int NULL
            if @@error <> 0 goto error
        end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'gen_cur', col_type = 'bigint NULL'-- Current generation for new generation assignment added in SQL7.0 SP1
                                                             union all
                                                             select col_name = 'excluded_cols', col_type = 'varbinary(128) NULL'-- Vertical Partitioning (Shiloh)
                                                             union all
                                                             select col_name = 'vertical_partition', col_type = 'int NULL'
                                                             union all
                                                             select col_name = 'identity_support', col_type = 'int default 0 NOT NULL '-- Auto identity management (Shiloh)
                                                             union all
                                                             select col_name = 'destination_owner', col_type = 'sysname default ''dbo'' not NULL'-- Destination owner support (Shiloh)
                                                             union all
                                                             select col_name = 'verify_resolver_signature', col_type = 'int NULL default 1'-- Resolver certificate support (Shiloh)
                                                             union all
                                                             select col_name = 'allow_interactive_resolver', col_type = 'bit NOT NULL default 0'-- Interactive resolver (Shiloh)
                                                             union all
                                                             select col_name = 'published_in_tran_pub', col_type = 'bit NOT NULL default 0'
                                                             union all
                                                             select col_name = 'fast_multicol_updateproc', col_type = 'bit NOT NULL default 0'-- Whether update proc should do one update per column or multiple columns in one update (Shiloh)
                                                             union all
                                                             select col_name = 'check_permissions', col_type = 'int NOT NULL default 0'
                                                             union all
                                                             select col_name = 'processing_order', col_type = 'int NOT NULL default 0'
                                                             union all
                                                             select col_name = 'maxversion_at_cleanup', col_type = 'int not null default 1'
                                                             union all
                                                             select col_name = 'upload_options', col_type = 'tinyint not null default 0'
                                                             union all
                                                             select col_name = 'procname_postfix', col_type = 'nchar(32) null'
                                                             union all
                                                             select col_name = 'well_partitioned_lightweight', col_type = 'bit null'
                                                             union all
                                                             select col_name = 'delete_tracking', col_type = 'bit not null default 1'
                                                             union all
                                                             select col_name = 'compensate_for_errors', col_type = 'bit not null default 0'
                                                             union all
                                                             select col_name = 'pub_range', col_type = 'bigint null'
                                                             union all
                                                             select col_name = 'range', col_type = 'bigint NULL'
                                                             union all
                                                             select col_name = 'threshold', col_type = 'int NULL'
                                                             union all
                                                             select col_name = 'stream_blob_columns', col_type = 'bit not NULL default 0'
                                                             union all
                                                             select col_name = 'deleted_cols', col_type = 'varbinary(128) NULL default 0x0'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergearticles', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
			select @alter_cmd = 'alter table dbo.sysmergearticles add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
		    exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs

		-- add default constraint on verify_resolver_signature column
		if not exists(
		select * 
        from sysconstraints as con join sys.columns as col 
            on con.colid = col.column_id
                and con.id = col.object_id
                and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
                and col.object_id = object_id('dbo.sysmergearticles')
                and col.name = 'verify_resolver_signature')and exists 
				(select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = 'verify_resolver_signature')
		begin
			exec('alter table dbo.sysmergearticles add default 1 for verify_resolver_signature')  
			if @@error <> 0 goto error	
		end

		
		-- change default constraint on compensate_for_errors column to be 0 and also update all values for this column to be 0
		-- we think that compensate_for_errors=1 (sql2k default)is not very usefull and can cause more harm then good.
		if exists(select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = 'compensate_for_errors')
		begin
			declare @default_compensate_for_errors_constraint_name nvarchar(258)
			select @default_compensate_for_errors_constraint_name = obj.name 
			from sysconstraints as con join sys.columns as col 
				on con.colid = col.column_id 
					and con.id = col.object_id
					and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
					and col.object_id = object_id('dbo.sysmergearticles')
					and col.name = 'compensate_for_errors'
					join sys.objects as obj
					on obj.object_id=con.constid

			if(@default_compensate_for_errors_constraint_name is not  null)
			begin
				select @default_compensate_for_errors_constraint_name = quotename(@default_compensate_for_errors_constraint_name)
				exec ('alter table dbo.sysmergearticles  drop constraint ' + @default_compensate_for_errors_constraint_name)
				if @@error <> 0 goto error
			end			
			exec('alter table dbo.sysmergearticles add default 0 for compensate_for_errors')  
			if @@error <> 0 goto error
			exec('update dbo.sysmergearticles set compensate_for_errors = 0 ')			
			if @@error <> 0 goto error
		end

        -- gen_cur is int in SQL8 and earlier, bigint in SQL9
        if 56 = (select system_type_id from sys.columns where 
                        object_id = object_id('dbo.sysmergearticles') and name = 'gen_cur')
        begin
            alter table dbo.sysmergearticles alter column gen_cur bigint null
            if @@error <> 0 goto error
        end
        
        -- Set default value of column destination_owner if NULL - could happen if first upgraded
        -- from 7.0 to Beta 2, which does not have the default value and then to 80 RTM.

        -- Destination owner support (Shiloh)
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and
                        name = 'destination_owner')
        begin
            exec ('update dbo.sysmergearticles set destination_owner=''dbo'' where destination_owner is NULL')
            if @@error <> 0 goto error
        end
        
        if not exists (select * from sys.indexes where name = 'nc2sysmergearticles')
        begin
            create nonclustered index nc2sysmergearticles on sysmergearticles(processing_order) 
            if @@error <> 0 goto error
        end
        
        if not exists (select * from sys.indexes where name = 'nc3sysmergearticles')
        begin
            create unique nonclustered index nc3sysmergearticles on dbo.sysmergearticles(objid, pubid) 
            if @@ERROR <> 0    goto error
        end
        --
        -- add default for compensate_for_errors
        --
        if not exists (select dc.name 
                        from sys.default_constraints as dc 
                            join sys.columns as c
                                on dc.parent_object_id = c.object_id
                                    and dc.parent_column_id = c.column_id
                        where c.object_id = object_id(N'dbo.sysmergearticles')
                            and c.name = N'compensate_for_errors')
        begin
            alter table dbo.sysmergearticles add default 0 for compensate_for_errors
        end
        --
        -- add default for excluded_col_count
        --
        if not exists (select dc.name 
                        from sys.default_constraints as dc 
                            join sys.columns as c
                                on dc.parent_object_id = c.object_id
                                    and dc.parent_column_id = c.column_id
                        where c.object_id = object_id(N'dbo.sysmergearticles')
                            and c.name = N'excluded_col_count')
        begin
            alter table dbo.sysmergearticles add default 0 for excluded_col_count
        end
        --
        -- add default for vertical_partition
        --
        if not exists (select dc.name 
                        from sys.default_constraints as dc 
                            join sys.columns as c
                                on dc.parent_object_id = c.object_id
                                    and dc.parent_column_id = c.column_id
                        where c.object_id = object_id(N'dbo.sysmergearticles')
                            and c.name = N'vertical_partition')
        begin
            alter table dbo.sysmergearticles add default 0 for vertical_partition
        end
        --
        -- remove the default for column destination_owner
        --
        select @column_name = NULL
        select @column_name = dc.name 
        from sys.default_constraints as dc 
            join sys.columns as c
                on dc.parent_object_id = c.object_id
                    and dc.parent_column_id = c.column_id
        where c.object_id = object_id('dbo.sysmergearticles')
            and c.name = N'destination_owner'
        if (@column_name is not null)
        begin
            select @alter_cmd = N'alter table dbo.sysmergearticles drop constraint ' + quotename(@column_name)
            exec(@alter_cmd)
            if @@error <> 0 
                return 1
        end

        -- this table exists at publisher and subscriber dbs
        if object_id('MSmerge_identity_range') is NULL
        begin
            create table dbo.MSmerge_identity_range (
                subid               uniqueidentifier not NULL,
                artid               uniqueidentifier not NULL,
                range_begin         numeric(38,0) NULL,
                range_end           numeric(38,0) NULL,
                next_range_begin    numeric(38,0) NULL,
                next_range_end      numeric(38,0) NULL,
                is_pub_range        bit not NULL,
                max_used            numeric(38,0) NULL
            )
            if @@error <> 0 goto error
            
            exec dbo.sp_MS_marksystemobject MSmerge_identity_range
            if @@error <> 0 goto error
            
            create unique clustered index uclidrange on MSmerge_identity_range(subid, artid, is_pub_range)
            if @@error <> 0 goto error
        end

        if object_id('MSmerge_settingshistory') is NULL
        begin
            --raiserror('Creating table MSmerge_settingshistory',0,1)

            --This table records the history of when merge related settings
            --were changed. It can also bo used to record important events
            --that affect behavior of merge replication.

            --eventtype can have one of the following values
            --  1   Initial publication level property setting.
            --  2   Change in publication property.
            --  101 Initial article level property setting.
            --  102 Change in article property.
            --  In future add publication related event below 100 and
            --  article related events about 100 to make searching easier

            create table dbo.MSmerge_settingshistory
            (
                eventtime        datetime           null default getdate(),
                pubid            uniqueidentifier    NOT NULL,
                artid           uniqueidentifier    NULL,
                eventtype         tinyint                NOT NULL,
                propertyname    sysname             NULL,
                   previousvalue   sysname             NULL,
                newvalue        sysname             NULL,
                eventtext        nvarchar(2000)         NULL    
            )
                    
            if @@error <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_settingshistory
            if @@error <> 0 goto error

            create clustered index c1MSmerge_settingshistory on MSmerge_settingshistory(pubid,eventtype) 
            if @@error <> 0 goto error

        end
        else
        begin
        if exists (select * from sys.columns where object_id = object_id('dbo.MSmerge_settingshistory') and name = N'eventtime' and is_nullable = 0)
        	alter table dbo.MSmerge_settingshistory alter column eventtime datetime           null 
        end

        if object_id('sysmergepartitioninfo') is NULL
        begin
            create table dbo.sysmergepartitioninfo 
            (
                artid                           uniqueidentifier     NOT NULL,
                pubid                           uniqueidentifier     NOT NULL,
                partition_view_id               int                  NULL,
                repl_view_id                    int                  NULL,
                partition_deleted_view_rule     nvarchar(max)        NULL,
                partition_inserted_view_rule    nvarchar(max)        NULL,
                membership_eval_proc_name       sysname              NULL,
                column_list                     nvarchar(max)        NULL,
                column_list_blob                nvarchar(max)        NULL,
                expand_proc                     sysname              NULL,
                logical_record_parent_nickname  int                  NULL,
                logical_record_view             int                  NULL,
                logical_record_deleted_view_rule nvarchar(max)       NULL,
                logical_record_level_conflict_detection bit       null   default 0,
                logical_record_level_conflict_resolution bit      null   default 0,
                partition_options               tinyint        null      default 0
            )
            if @@error <> 0 goto error
            
            create unique clustered index uc1sysmergepartitioninfo
                on dbo.sysmergepartitioninfo(artid, pubid) 
            if @@error <> 0 goto error
            
            exec dbo.sp_MS_marksystemobject sysmergepartitioninfo
            if @@error <> 0 goto error

            -- we need to insert a row for every article in sysmergearticles into sysmergepartitioninfo
            insert dbo.sysmergepartitioninfo (artid, pubid)
                select artid, pubid from dbo.sysmergearticles
            if @@error <> 0 goto error
            
        end
        else
        begin
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepartitioninfo') and name = N'logical_record_level_conflict_detection' and is_nullable = 0)
        	alter table dbo.sysmergepartitioninfo alter column logical_record_level_conflict_detection bit null
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepartitioninfo') and name = N'logical_record_level_conflict_resolution' and is_nullable = 0)
        	alter table dbo.sysmergepartitioninfo alter column logical_record_level_conflict_resolution bit null
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepartitioninfo') and name = N'partition_options' and is_nullable = 0)
        	alter table dbo.sysmergepartitioninfo alter column partition_options tinyint null
        end

        if object_id('sysmergepartitioninfoview') is not NULL
        begin
            drop view dbo.sysmergepartitioninfoview
        end
        
        exec ('create view dbo.sysmergepartitioninfoview as
            select sma.*, smaw.partition_view_id, 
                smaw.repl_view_id,
                smaw.partition_deleted_view_rule,
                smaw.partition_inserted_view_rule,
                smaw.membership_eval_proc_name,
                smaw.column_list,
                smaw.column_list_blob,
                smaw.expand_proc,
                smaw.logical_record_parent_nickname,
                smaw.logical_record_view,
                smaw.logical_record_deleted_view_rule,
                smaw.logical_record_level_conflict_detection,
                smaw.logical_record_level_conflict_resolution,
                smaw.partition_options
           from dbo.sysmergearticles sma, dbo.sysmergepartitioninfo smaw
           where sma.artid = smaw.artid and sma.pubid = smaw.pubid')            
        if @@error <> 0 goto error

        exec dbo.sp_MS_marksystemobject sysmergepartitioninfoview
        if @@error <> 0 goto error

        -- Schema only articles (Shiloh)
        if object_id('dbo.sysmergeschemaarticles', 'U') is null
        begin
            exec @retcode= sys.sp_MScreate_sysmergeschemaarticles_table
            if @@error <> 0 or @retcode <> 0 goto error
        end
        else
        begin
            if not exists (select * from sys.columns where object_id = object_id('sysmergeschemaarticles') and
                        name = 'processing_order')
            begin
                alter table sysmergeschemaarticles add processing_order int NOT NULL default 0
                if @@error <> 0 goto error
            end
        end

        -- create view now that sysmergearticles is altered and sysmergeextendedarticles is created
        if object_id('sysmergeextendedarticlesview') is not NULL
        begin
            drop view dbo.sysmergeextendedarticlesview
        end    

        -- cannot create view directly in proc
        exec @retcode= sys.sp_MScreate_sysmergeextendedarticlesview
        if @@error<>0 or @retcode<>0 goto error

        exec dbo.sp_MS_marksystemobject sysmergeextendedarticlesview

        -- vertical partitioning requires a view based sync obj; SQL7.0 used zero as sync_objid
        -- when a non-partitioned article was created; later versions must use the explicit base table
        -- as sync_objid; fixup article sync_objid's prior to remaking the article procs (Shiloh)
        update dbo.sysmergearticles set sync_objid = objid where sync_objid = 0

        -- Do not regenerate views, procs if this is called from sp_restoredbreplication. Restore only
        -- needs to update schema, then it can call existing system procs to remove db replication cleanly
        if @remove_repl = 0
        begin
            -- when upgrading to yukon we have a huge amount of metadata upgrade to do. We do not want to 
            -- do this as part of the upgrade or restore process. We want the subsequent merge or snapshot to
            -- take care of it. Hence here we will drop all triggers on publisher tables and create triggers
            -- which do not allow the DML to happen. When the snapshot or merge has been run valid triggers will
            -- be created and change tracking with work fine after the metadata has been upgraded.
            select @artnick = min(nickname) from dbo.sysmergearticles
            while @artnick is not null
            begin
                select @objid = NULL
                select @source_object = NULL
                select top 1 @objid = objid, @artid = artid from dbo.sysmergearticles where nickname = @artnick
                select @source_owner = schema_name(schema_id), @source_object = name from sys.objects where object_id = @objid
                if @objid is NULL or @source_object is NULL
                    goto error
                    
                exec @retcode = sys.sp_MSdroparticletriggers @source_object, @source_owner
                if @retcode<>0 or @@error<>0
                    goto error
            
                -- generate the disable dml trigger
                exec sys.sp_MScreatedisabledmltrigger @source_object, @source_owner
                if @retcode<>0 or @@error<>0
                    goto error

                -- set the article status to inactive so that the subsequent snapshot prepares the article
                update dbo.sysmergearticles set status = 1 where artid = @artid and status = 2
                update dbo.sysmergearticles set status = 5 where artid = @artid and status = 6

                -- find next article
                select @artnick = min(nickname) from dbo.sysmergearticles where nickname > @artnick
            end -- end article while
        end -- end @remove_repl

        -- MSmerge_errorlineage (Shiloh)
        if object_id('MSmerge_errorlineage') is NULL
        begin
            create table dbo.MSmerge_errorlineage (
            tablenick          int NOT NULL,
            rowguid            uniqueidentifier NOT NULL,
            lineage            varbinary(311) null
            )
            exec dbo.sp_MS_marksystemobject MSmerge_errorlineage
            if @@ERROR <> 0
                goto error
                
            create unique clustered index uc1errorlineage on MSmerge_errorlineage(tablenick, rowguid)
            if @@ERROR <> 0
                goto error
        end
        else
        begin
        if exists (select * from sys.columns where object_id = object_id('dbo.MSmerge_errorlineage') and name = N'lineage' and is_nullable = 0)
        	alter table dbo.MSmerge_errorlineage alter column lineage varbinary(311) null
        end

        -- lineage is varbinary(255) in Shiloh and before, varbinary(311) in Yukon and after
        -- in addition, the format has changed from 4 to 6 byte nicknames, and there are new mergenicks
        if 311 > col_length('MSmerge_errorlineage', 'lineage')
        begin
            begin tran
            save tran tran_errlin80to90
                alter table dbo.MSmerge_errorlineage alter column lineage varbinary(311) not null
                if @@error<>0 goto err_errlin80to90
                update dbo.MSmerge_errorlineage set lineage= {fn LINEAGE_80_TO_90(lineage)}
                if @@error<>0 goto err_errlin80to90
            commit tran
            goto after_errlin80to90

            err_errlin80to90:
            rollback tran tran_errlin80to90
            commit tran
            goto error
        end

        after_errlin80to90:
            
        -- MSmerge_altsyncpartners (Shiloh)
        if object_id('MSmerge_altsyncpartners') is NULL
        begin
            create table dbo.MSmerge_altsyncpartners (
                subid           uniqueidentifier    not null,
                alternate_subid uniqueidentifier    not null,
                description     nvarchar(255)       NULL
            )
            if @@ERROR <> 0
                goto error

            exec dbo.sp_MS_marksystemobject MSmerge_altsyncpartners
            if @@ERROR <> 0
                goto error

            create unique clustered index uciMSmerge_altsyncpartners on 
                dbo.MSmerge_altsyncpartners(subid, alternate_subid)            
            if @@ERROR <> 0
                goto error
        end

        -- new tables added for the first time after SQL2000.
        if object_id('MSmerge_partition_groups') is NULL
        begin
            create table dbo.MSmerge_partition_groups (partition_id int identity not null primary key clustered, 
            					publication_number smallint not null, maxgen_whenadded bigint null, 
            					using_partition_groups bit null default 0, is_partition_active bit default 1 not null)
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_partition_groups
            if @@ERROR <> 0 goto error

            create nonclustered index nc1MSmerge_partition_groups on dbo.MSmerge_partition_groups (publication_number)
            if @@ERROR <> 0 goto error

            grant select on dbo.MSmerge_partition_groups to public    
        end
        else
        begin
            -- Column is_partition_active was added after IDW15 for SQL 2005
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_partition_groups') and
                        name = 'is_partition_active')
            begin
                alter table MSmerge_partition_groups add is_partition_active bit default 1 not null
                if @@error <> 0 goto error
            end            
            
	        if exists (select * from sys.columns where object_id = object_id('dbo.MSmerge_partition_groups') and name = N'using_partition_groups' and is_nullable = 0)
	        	alter table dbo.MSmerge_partition_groups alter column using_partition_groups bit null
        end

        if object_id('MSmerge_generation_partition_mappings') is NULL
        begin
            create table dbo.MSmerge_generation_partition_mappings 
                (
                publication_number smallint not null, 
                generation bigint not null, 
                partition_id int not null,
                changecount int NOT NULL default 0
                )
            if @@ERROR <> 0 goto error
            
            exec dbo.sp_MS_marksystemobject MSmerge_generation_partition_mappings
            if @@ERROR <> 0 goto error

            create clustered index cMSmerge_generation_partition_mappings on dbo.MSmerge_generation_partition_mappings (partition_id, publication_number)
            if @@ERROR <> 0 goto error

            create nonclustered index nc1MSmerge_generation_partition_mappings on dbo.MSmerge_generation_partition_mappings (generation) include (changecount)
            if @@ERROR <> 0 goto error
        end

        if object_id('MSmerge_current_partition_mappings') is NULL
        begin
            create table dbo.MSmerge_current_partition_mappings (publication_number smallint not null, tablenick int not null, rowguid uniqueidentifier not null, partition_id int not null)
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_current_partition_mappings
            if @@ERROR <> 0 goto error

            create clustered index cMSmerge_current_partition_mappings on dbo.MSmerge_current_partition_mappings (tablenick, rowguid)
            if @@ERROR <> 0
                goto error

            create nonclustered index ncMSmerge_current_partition_mappings on dbo.MSmerge_current_partition_mappings (publication_number, partition_id)
            if @@ERROR <> 0
                goto error
            
        end

        if object_id('MSmerge_past_partition_mappings') is NULL
        begin
            create table dbo.MSmerge_past_partition_mappings (publication_number smallint not null, tablenick int not null, rowguid uniqueidentifier not null, partition_id int not null, generation bigint null, reason tinyint not null default(0))
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_past_partition_mappings
            if @@ERROR <> 0 goto error

            create clustered index cMSmerge_past_partition_mappings on dbo.MSmerge_past_partition_mappings (tablenick, rowguid)
            if @@ERROR <> 0
                goto error

            create nonclustered index nc1MSmerge_past_partition_mappings on dbo.MSmerge_past_partition_mappings (publication_number, partition_id)
            if @@ERROR <> 0
                goto error
                
            create nonclustered index nc2MSmerge_past_partition_mappings on dbo.MSmerge_past_partition_mappings (generation)
            if @@ERROR <> 0
                goto error
        end
        
        if object_id('MSmerge_dynamic_snapshots') is NULL
        begin
            create table dbo.MSmerge_dynamic_snapshots (
                    partition_id int not null primary key clustered foreign key references dbo.MSmerge_partition_groups(partition_id) on delete cascade, 
                    dynamic_snapshot_location nvarchar(255) null, 
                    last_updated datetime null,
                    last_started datetime null)
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_dynamic_snapshots            
            if @@ERROR <> 0 goto error
        end
        else
        begin
			if not exists (select * from sys.columns where object_id = object_id('MSmerge_dynamic_snapshots') and
                        name = 'last_started')
            begin
                alter table MSmerge_dynamic_snapshots add last_started datetime NULL
                if @@error <> 0 goto error
            end
        end

        -- Added in Yukon
        if object_id('MSmerge_supportability_settings') is NULL
        begin
            create table dbo.MSmerge_supportability_settings (
                    pubid                uniqueidentifier    NULL,
                    subid                uniqueidentifier    NULL,
                    web_server           sysname             NULL,                
                    constraint           unique_supportpubsrvdb     unique nonclustered (pubid, subid, web_server),
                    support_options      int NOT NULL default(0),    -- check the SUPPORT_OPTIONS enum in agent code.
                    log_severity         int NOT NULL default(2),
                    log_modules          int NOT NULL default(0),
                    log_file_path        nvarchar(255) NULL,
                    log_file_name        sysname NULL,
                    log_file_size        int NOT NULL default(10000000),
                    no_of_log_files      int NOT NULL default(5),
                    upload_interval      int NOT NULL default(0),
                    delete_after_upload  int NOT NULL default(0),                    
                    custom_script        nvarchar(2048) NULL,
                    message_pattern      nvarchar(2000) NULL,
                    last_log_upload_time datetime            NULL,
                    agent_xe               varbinary(max) NULL,
                    agent_xe_ring_buffer  varbinary(max) NULL,
                    sql_xe                   varbinary(max) NULL
                    )
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_supportability_settings            
        end
        else
        begin
            -- Column agent_xe  was added for SQL 11
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_supportability_settings') and
                        name = 'agent_xe')
            begin
                alter table MSmerge_supportability_settings add agent_xe varbinary(max)
                if @@ERROR <> 0 goto error
            end            
            
            -- Column agent_xe_ring_buffer  was added for SQL 11
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_supportability_settings') and
                        name = 'agent_xe_ring_buffer')
            begin
                alter table MSmerge_supportability_settings add agent_xe_ring_buffer varbinary(max)
                if @@ERROR <> 0 goto error
            end           

            -- Column sql_xe  was added for SQL 11
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_supportability_settings') and
                        name = 'sql_xe')
            begin
                alter table MSmerge_supportability_settings add sql_xe varbinary(max)
                if @@ERROR <> 0 goto error
            end            

        end
        
        -- Added in Yukon
        if object_id('MSmerge_log_files') is NULL
        begin
            create table dbo.MSmerge_log_files (
                    id                   int identity(1,1),
                    pubid                uniqueidentifier    NULL,
                    subid                uniqueidentifier    NULL,
                    web_server           sysname             NULL,
                    file_name            nvarchar(2000)      NOT NULL,
                    upload_time          datetime              NOT NULL default getdate(),
                    log_file_type        int                 NOT NULL, -- Check UPLOAD_LOG_FILE_TYPE enum in agent code.
                    log_file             varbinary(max)      NULL
                    )
            if @@ERROR <> 0 goto error

            create clustered index ucMSmerge_log_files on MSmerge_log_files(pubid, subid, id) 
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_log_files            
        end

        -- Added in Yukon
        if object_id('dbo.MSmerge_metadataaction_request', 'U') is null
        begin
            create table dbo.MSmerge_metadataaction_request
            (
                tablenick int not null,
                rowguid uniqueidentifier not null,
                action tinyint not null,
                generation bigint null, -- for hws cleanup
                changed int null -- for lws cleanup
            )
            if @@ERROR <> 0 goto error

            create clustered index ucMSmerge_metadataaction_request on MSmerge_metadataaction_request(tablenick, rowguid) 
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_metadataaction_request
            if @@ERROR <> 0 goto error
        end

        -- Added in Yukon
        if object_id('dbo.MSmerge_agent_parameters', 'U') is null
        begin
            --raiserror('Creating table MSmerge_agent_parameters',0,1)
            
            create table dbo.MSmerge_agent_parameters
            (
            profile_name         sysname        NOT NULL,
            parameter_name       sysname        NOT NULL,
            value                nvarchar(255)  NOT NULL
            )
        
            if @@ERROR <> 0
                goto error

            exec dbo.sp_MS_marksystemobject MSmerge_agent_parameters
            if @@ERROR <> 0
                goto error        
        end

        -- we will now set the snapshot_ready status of all local publications. We invalidate the snapshot
        -- so that the metadata upgrade can be run at snapshot time.
        update dbo.sysmergepublications set snapshot_ready=2 
            where UPPER(publisher) collate database_default = UPPER(publishingservername()) collate database_default and publisher_db = db_name()
        
        -- revoke select access to public on table which were previously granted to public
        if object_id('dbo.sysmergepublications') is not NULL
            revoke select on dbo.sysmergepublications from public
            
        if object_id('dbo.MSmerge_errorlineage') is not NULL
            revoke select on dbo.MSmerge_errorlineage from public
            
        if object_id('dbo.sysmergearticles') is not NULL
        begin
            revoke select on dbo.sysmergearticles from public
            grant select(nickname, maxversion_at_cleanup, objid) on dbo.sysmergearticles to public
        end
        
        if object_id('dbo.sysmergesubscriptions') is not NULL
            revoke select on dbo.sysmergesubscriptions from public
            
        if object_id('dbo.MSmerge_replinfo') is not NULL
            revoke select on dbo.MSmerge_replinfo from public

        if object_id('dbo.MSmerge_tombstone') is not NULL
            revoke select on dbo.MSmerge_tombstone from public
            
        if object_id('dbo.MSmerge_contents') is not NULL
            revoke select on dbo.MSmerge_contents from public
        
        if object_id('dbo.MSmerge_genhistory') is not NULL
            revoke select on dbo.MSmerge_genhistory from public
        
        if object_id('dbo.sysmergeschemachange') is not NULL
            revoke select on dbo.sysmergeschemachange from public
        
        if object_id('dbo.sysmergesubsetfilters') is not NULL
            revoke select on dbo.sysmergesubsetfilters from public

    end -- end dbo.sysmergearticles modifications

    if object_id('sysmergearticles') is not NULL
    begin
        -- always drop down level triggers since in yukon the triggers are named differently
        if exists (select * from dbo.sysmergearticles)
        begin
            declare @artidstr sysname
            declare @instrigger nvarchar(517)
            declare @updtrigger nvarchar(517)
            declare @deltrigger nvarchar(517)
            
            select @artnick = min(nickname) from dbo.sysmergearticles
            while @artnick is not null
            begin
                select @objid = NULL
                select @source_object = NULL
                select top 1 @objid = objid, @artid = artid from dbo.sysmergearticles where nickname = @artnick
                select @source_owner = schema_name(schema_id), @source_object = name from sys.objects where object_id = @objid
                if @objid is NULL or @source_object is NULL
                    goto error

                exec @retcode=sys.sp_MSguidtostr @artid, @artidstr out
                if @retcode<>0 or @@ERROR<>0 
                    goto error
                    
                -- the following are downlevel trigger names
                select @instrigger = QUOTENAME(@source_owner) + '.ins_' + @artidstr
                select @updtrigger = QUOTENAME(@source_owner) + '.upd_' + @artidstr
                select @deltrigger = QUOTENAME(@source_owner) + '.del_' + @artidstr
                if object_id(@instrigger) is not NULL
                begin
                    exec ('drop trigger ' + @instrigger)
                    if @@ERROR<>0 return (1)
                end
                if object_id(@updtrigger) is not NULL
                begin
                    exec ('drop trigger ' + @updtrigger)
                    if @@ERROR<>0 return (1)
                end
                if object_id(@deltrigger) IS NOT NULL
                begin
                    exec ('drop trigger ' + @deltrigger)
                    if @@ERROR<>0 return (1)
                end
                
                -- find next article
                select @artnick = min(nickname) from dbo.sysmergearticles where nickname > @artnick
            end -- end article while
        end
    end
    
    

    
    /* Merge dynamic snapshot */

    /* Make sure that the database is enabled for merge replication before MSdynamicsnapshotviews is created */
    if @remove_repl = 0 and object_id('sysmergepublications') is not NULL
    begin

        /*
        ** MSdynamicsnapshotviews -- Created from Shiloh Beta2 onwards
        */

        if object_id('MSdynamicsnapshotviews') is NULL
        begin
            create table dbo.MSdynamicsnapshotviews (
                dynamic_snapshot_view_name sysname primary key,
            )
            if @@ERROR <> 0 goto error
        end

        exec dbo.sp_MS_marksystemobject MSdynamicsnapshotviews

        /* 
        ** MSdynamicsnapshotjobs -- Created from Shiloh Beta2 onwards
        */
        if object_id('MSdynamicsnapshotjobs') is NULL
        begin
            create table dbo.MSdynamicsnapshotjobs (
                id int identity,
                name sysname not null unique,
                pubid uniqueidentifier not null,
                job_id uniqueidentifier not null,
                agent_id int not null default 0,
                dynamic_filter_login sysname null,
                dynamic_filter_hostname sysname null,
                dynamic_snapshot_location nvarchar(255) not null,
                partition_id int not NULL default -1,
                computed_dynsnap_location bit not NULL default 0
            )
            if @@ERROR <> 0 goto error
        end 

        -- Update MSdynamicsnapshotjobs so that it has:
        --		agent_id default 1
        --    	partition_id default -1
        -- We are know herer that dbo.MSdynamicsnapshotjobs is not null so no need to check.
        
        -- Adding DEFAULT on column agent_id
	 if exists(select * from sys.columns where object_id = object_id('dbo.MSdynamicsnapshotjobs') 
	 	and name = 'agent_id')
	 begin
	        select @defaultname = null
	        select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
						(sysdc.parent_object_id = sysc.object_id and 
							sysdc.parent_column_id = sysc.column_id)		
					where sysc.object_id = object_id('dbo.MSdynamicsnapshotjobs') and
						sysc.name = 'agent_id'
		 if @defaultname is not null -- check if default exists
		 begin
		 	-- delete default if one exists
		 	select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs drop constraint ' + QUOTENAME(@defaultname)
		 	exec (@alter_cmd)
		 end
		 -- add the default we want
		 select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs add default 0 for agent_id'
		 exec (@alter_cmd)
	 end
	 if exists(select * from sys.columns where object_id = object_id('dbo.MSdynamicsnapshotjobs') 
	 	and name = 'partition_id')
	 begin
		 --Adding DEFAULT on column partition_id
		 select @defaultname = null
		 select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
						(sysdc.parent_object_id = sysc.object_id and 
							sysdc.parent_column_id = sysc.column_id)		
					where sysc.object_id = object_id('dbo.MSdynamicsnapshotjobs') and
						sysc.name = 'partition_id'
		 if @defaultname is not null -- check if default exists
		 begin
		 	-- delete default if one exists
		 	select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs drop constraint ' + QUOTENAME(@defaultname)
		 	exec (@alter_cmd)
		 end
		 -- add the default we want
		 select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs add default -1 for partition_id'
		 exec (@alter_cmd)
	 end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'partition_id', col_type = 'int not NULL default -1'
                                                             union all
                                                             select col_name = 'agent_id', col_type = 'int not NULL default 0'
                                                             union all
                                                             select col_name = 'computed_dynsnap_location', col_type = 'bit not NULL default 0'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.MSdynamicsnapshotjobs', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
            select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs

        if not exists (select * 
                         from sys.indexes 
                        where object_id = object_id('MSdynamicsnapshotjobs') 
                          and name = ('uciMSdynamicsnapshotjobs'))
        begin
            create unique clustered index uciMSdynamicsnapshotjobs on 
                dbo.MSdynamicsnapshotjobs(job_id, pubid)
            if @@ERROR <> 0 goto error
        end
        
        if not exists (select * 
                         from sys.indexes 
                        where object_id = object_id('MSdynamicsnapshotjobs') 
                          and name = ('nciMSdynamicsnapshotjobs'))
        begin
            create nonclustered index nciMSdynamicsnapshotjobs on 
                dbo.MSdynamicsnapshotjobs(partition_id)
            if @@ERROR <> 0 goto error
        end

        exec dbo.sp_MS_marksystemobject MSdynamicsnapshotjobs
        if @@ERROR <> 0 goto error
    end
	
    -- Index updates (SQL7.0 SP1)
    if @remove_repl = 0
    begin
        SELECT @table_name = N'sysmergepublications'
        IF object_id('sysmergepublications') is not NULL
        BEGIN 
            IF EXISTS ( SELECT pubid
                FROM dbo.sysmergepublications
                GROUP BY pubid
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 4, @table_name)
                goto error
            end
            ELSE
                IF NOT EXISTS ( SELECT * FROM sys.indexes WHERE name = 'nc1sysmergepublications' AND
                    object_id = object_id('dbo.sysmergepublications') )
                    CREATE UNIQUE NONCLUSTERED INDEX nc1sysmergepublications
                        ON dbo.sysmergepublications(pubid)
                 if @@ERROR <> 0 goto error

				 IF NOT EXISTS ( SELECT * FROM sys.indexes WHERE name = 'nc2sysmergepublications' AND
                    object_id = object_id('dbo.sysmergepublications') )
					CREATE NONCLUSTERED INDEX nc2sysmergepublications 
					ON sysmergepublications(status)
                 if @@ERROR <> 0 goto error


				 -- add default constraint on allow_anonymous column 
				if not exists(
				select * 
				from sysconstraints as con join sys.columns as col 
					on con.colid = col.column_id
						and con.id = col.object_id
						and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
						and col.object_id = object_id('dbo.sysmergepublications')
						and col.name = 'allow_anonymous')and exists 
				(select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'allow_anonymous')
				begin
					exec(N'alter table dbo.sysmergepublications add default 1 for allow_anonymous')
				end


				-- Changing default constraint on publisher from @@servername to publishingservername() 
				if exists(select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'publisher')
				begin
					declare @default_publisher_c_name nvarchar(258)
					select @default_publisher_c_name = obj.name 
					from sysconstraints as con join sys.columns as col 
						on con.colid = col.column_id 
							and con.id = col.object_id
							and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
							and col.object_id = object_id('dbo.sysmergepublications')
							and col.name = 'publisher'
							join sys.objects as obj
							on obj.object_id=con.constid

					if(@default_publisher_c_name is not null)
					begin
						select @default_publisher_c_name = quotename(@default_publisher_c_name)
						exec (N'alter table dbo.sysmergepublications  drop constraint ' + @default_publisher_c_name)
						exec(N'alter table dbo.sysmergepublications add default publishingservername()  for publisher') 					
					end
				end

        END 


        --  MSmerge_conflicts_info
        SELECT @table_name = N'MSmerge_conflicts_info'
        IF object_id('MSmerge_conflicts_info') is not NULL
        BEGIN
            IF EXISTS ( SELECT tablenick, rowguid, origin_datasource, conflict_type
                FROM MSmerge_conflicts_info
                GROUP BY tablenick, rowguid, origin_datasource, conflict_type
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 6, @table_name)
                goto error
            end
            ELSE
                IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc1MSmerge_conflicts_info' AND
                    id = OBJECT_ID('MSmerge_conflicts_info') )
                    CREATE UNIQUE NONCLUSTERED INDEX nc1MSmerge_conflicts_info 
                        ON MSmerge_conflicts_info(tablenick, rowguid, origin_datasource, conflict_type)
            if @@ERROR <> 0 goto error
        END

        --  sysmergeschemachange
        SELECT @table_name = N'sysmergeschemachange'
        IF object_id('sysmergeschemachange') is not NULL
        BEGIN
            IF EXISTS ( SELECT schemaversion, pubid
                FROM dbo.sysmergeschemachange
                GROUP BY schemaversion, pubid
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 7, @table_name)
                goto error
            end
            ELSE
            BEGIN
                IF EXISTS ( SELECT * FROM sys.indexes WHERE name = 'schemachangeversion' AND
                    object_id = OBJECT_ID('sysmergeschemachange') )            
                    DROP INDEX sysmergeschemachange.schemachangeversion
                if @@ERROR <> 0 goto error
            
                -- Recreate this index as unique clustered with one more field in index key.
                CREATE UNIQUE CLUSTERED INDEX schemachangeversion ON sysmergeschemachange(schemaversion, pubid) 
                if @@ERROR <> 0 goto error
            END

            -- In Yukon we no longer have a schema version SCHEMA_TYPE_SYSTABLE which
            -- indicates a system table schema script file. This type was deprecated because
            -- it was used only by JET consumers. In Yukon Jet subscribers are not supported
            -- Hence deleting any entries which have schema type SCHEMA_TYPE_SYSTABLE (20)
            delete from dbo.sysmergeschemachange where schematype=20        
            
            -- Adding schemastatus column. 
            if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergeschemachange') and
                            name = 'schemastatus')
            begin
                alter table dbo.sysmergeschemachange add schemastatus        tinyint        NOT NULL default(1)
                if @@error <> 0 goto error
            end

            if not exists (select * from sys.columns where object_id = object_id('sysmergeschemachange') and
                        name = 'schemasubtype')
            begin
                alter table sysmergeschemachange add schemasubtype int NOT NULL default 0
                if @@error <> 0 goto error
            end

            -- Modifying the type of the schematext from nvarchar(2000) to nvarchar(max)
            if exists (select * from sys.columns where object_id = object_id('dbo.sysmergeschemachange') and
                            name = 'schematext')
            begin
                exec (' alter table dbo.sysmergeschemachange alter column schematext nvarchar(max) NOT NULL ')
                if @@error <> 0 goto error
            end
            
         END


        --  sysmergesubsetfilters    
        SELECT @table_name = N'sysmergesubsetfilters'
        IF object_id('sysmergesubsetfilters') is not NULL
        BEGIN
            IF EXISTS ( SELECT join_filterid
                FROM dbo.sysmergesubsetfilters
                GROUP BY join_filterid
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 8, @table_name)
                goto error
            end
            ELSE
                IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc1sysmergesubsetfilters' AND
                    id = OBJECT_ID('sysmergesubsetfilters') )
                begin
                    CREATE UNIQUE NONCLUSTERED INDEX nc1sysmergesubsetfilters ON dbo.sysmergesubsetfilters(join_filterid, pubid)
                    if @@ERROR <> 0 goto error
                end
                
            IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'uc2sysmergesubsetfilters' AND
                id = OBJECT_ID('sysmergesubsetfilters') )
            begin
                CREATE UNIQUE CLUSTERED INDEX uc2sysmergesubsetfilters ON dbo.sysmergesubsetfilters(pubid, filtername)
                if @@ERROR <> 0 goto error
            end
                
            if not exists (select * from sys.columns where object_id = object_id('sysmergesubsetfilters') and
                            name = 'filter_type')
            begin
                alter table dbo.sysmergesubsetfilters add filter_type tinyint NOT NULL default 1
                if @@ERROR <> 0 goto error
            end
        END -- end index updates from SQL7.0 SP1


        declare @binames table (biname sysname)
        insert into @binames select name from sys.objects where type='U' and is_ms_shipped=1 and name like 'MS_bi%'
        declare @biname sysname
        set @biname= (select top 1 biname from @binames)
        while @biname is not null
        begin
            set @cmd= 'drop index ' + quotename(@biname) + '.' + quotename(@biname + '_gen')
            exec sys.sp_executesql @cmd
            if @@ERROR <> 0 goto error
            set @cmd= 'create clustered index ' + quotename(@biname + '_gen') + ' on ' + quotename(@biname) + '(generation)'
            exec sys.sp_executesql @cmd
            if @@ERROR <> 0 goto error
            delete from @binames where biname=@biname
            set @biname= (select top 1 biname from @binames)
        end


        if object_id('sysmergearticles') is not NULL
        begin
            exec sys.sp_MScreate_common_dist_tables @subside=1
            if @@error <> 0
               goto error
        end
        
        -- in Yukon we will create the merge ddl triggers, only for databases that have merge replication enabled.
        if object_id('sysmergepublications', 'U') is NOT NULL
        begin
            if exists (select * from sys.triggers where name = 'MSmerge_tr_altertable' and type = 'TR')
            begin
                execute @retcode= sys.sp_MSrepl_ddl_triggers @type='merge', @mode='drop'
                if @@ERROR <> 0 or @retcode <> 0 
                    goto error
            end
            if not exists (select * from sys.triggers where name = 'MSmerge_tr_altertable' and type = 'TR')
            begin
                execute @retcode= sys.sp_MSrepl_ddl_triggers @type='merge', @mode='add'
                if @@ERROR <> 0 or @retcode <> 0 
                    goto error
            end
        end
        -- we will also add a system table MSmerge_upgrade_in_progress which would indicate that
        -- the metadata upgrade has not been completed yet.
        if @remove_repl=0 and object_id('sysmergearticles') is not NULL
        begin
            if object_id('MSmerge_upgrade_in_progress', 'U') is NULL
            begin
                create table dbo.MSmerge_upgrade_in_progress
                (
                    status tinyint not NULL
                )
                if @@ERROR <> 0 goto error
            end
        end
    end
    
    -- Call the v2 proc when we change the implementation of that proc.  Right now it is empty and hence the following code is commented out.
    --Declare @dbname sysname = DB_NAME()
    --EXEC @retcode = master.sys.sp_vupgrade_mergetables_v2 @remove_repl = @remove_repl, @dbname = @dbname
    --if @@ERROR <> 0 or @retcode <> 0
    --    goto error

    
    commit tran
    return 0
    
error:
    rollback tran vupgrade_mergetables
    commit tran
    return 1
    
end



/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_vupgrade_mergetables( @remove_repl bit = 0 )
as
begin

    set nocount on 

    declare @artnick int
    declare @objid  int
    declare @goodlen int
    declare @col_track int
    declare @article sysname
    declare @pubname sysname
    declare @artid uniqueidentifier
    declare @pubid uniqueidentifier
    declare @qualified_name nvarchar(257)
    declare @source_owner   sysname
    declare @source_object  sysname
    declare @table_name     sysname
    declare @cmd            nvarchar(1000)
    declare @default_name   nvarchar(258)
    declare @retcode    integer
    declare @snapshot_ready int
    declare @constraintname nvarchar(258)
    declare @dbname sysname
    
    declare @additive_resolver_clsid    nvarchar(60)
    declare @average_resolver_clsid     nvarchar(60)
    declare @download_resolver_clsid    nvarchar(60)
    declare @max_resolver_clsid         nvarchar(60)
    declare @mergetxt_resolver_clsid    nvarchar(60)
    declare @min_resolver_clsid         nvarchar(60)
    declare @subwins_resolver_clsid     nvarchar(60)
    declare @upload_resolver_clsid      nvarchar(60)
    declare @sp_resolver_clsid          nvarchar(60)

    declare @additive_resolver      nvarchar(80)
    declare @average_resolver       nvarchar(80)
    declare @download_resolver      nvarchar(80)
    declare @max_resolver           nvarchar(80)
    declare @mergetxt_resolver      nvarchar(80)
    declare @min_resolver           nvarchar(80)
    declare @subwins_resolver       nvarchar(80)
    declare @upload_resolver        nvarchar(80)
    declare @sp_resolver            nvarchar(80)
    declare @priority_resolver      nvarchar(80)
    declare @earlierwins_resolver   nvarchar(80)
    declare @laterwins_resolver     nvarchar(80)

    declare @column_name nvarchar(128)
    declare @column_type nvarchar(128)
    declare @alter_cmd nvarchar(max)

    DECLARE @cnt int, @idx  int    /* Loop counter, index */
    DECLARE @columnid smallint     /* Columnid-1 = bit to set */
    DECLARE @columns binary(128)   /* Temporary storage for the converted column */


    select @additive_resolver_clsid = '{6ACA9C22-3CC2-4947-9D5F-525A1F9E8B45}'
    select @average_resolver_clsid  = '{F09F3613-7C9A-481F-952D-84B5E1060AC6}'
    select @download_resolver_clsid = '{B4CA61C8-7495-4F5A-9EE1-AEAF685693C8}'
    select @max_resolver_clsid      = '{608EB36F-373A-4485-A1AA-A21DE806FDF1}'
    select @mergetxt_resolver_clsid = '{C6CE3676-53F4-47B5-B3AF-1BB44996192E}'
    select @min_resolver_clsid      = '{CCC9BC97-EC98-4210-9BA0-2FE28C6DE077}'
    select @subwins_resolver_clsid  = '{BB6E90FC-FFE5-4256-9906-56BDFD7F1CAB}'
    select @upload_resolver_clsid   = '{482BCCD2-3FEB-4CA3-84C6-A380E3AB10E8}'
    select @sp_resolver_clsid       = '{D2701A2D-9D79-41BC-B7C4-1F2B5CF891B7}'

    select @additive_resolver       = 'Microsoft SQL Server Additive Conflict Resolver'
    select @average_resolver        = 'Microsoft SQL Server Averaging Conflict Resolver'
    select @download_resolver       = 'Microsoft SQL Server Download Only Conflict Resolver'
    select @max_resolver            = 'Microsoft SQL Server Maximum Conflict Resolver'
    select @mergetxt_resolver       = 'Microsoft SQL Server Merge Text Columns Conflict Resolver'
    select @min_resolver            = 'Microsoft SQL Server Minimum Conflict Resolver'
    select @subwins_resolver        = 'Microsoft SQL Server Subscriber Always Wins Conflict Resolver'
    select @upload_resolver         = 'Microsoft SQL Server Upload Only Conflict Resolver'
    select @sp_resolver             = 'Microsoft SQLServer Stored Procedure Resolver'
    select @priority_resolver       = 'Microsoft SQL Server Priority Column Resolver'
    select @earlierwins_resolver    = 'Microsoft SQL Server DATETIME (Earlier Wins) Conflict Resolver'
    select @laterwins_resolver      = 'Microsoft SQL Server DATETIME (Later Wins) Conflict Resolver'

    select @dbname = db_name()
    
    exec @retcode = sys.sp_MSreplcheck_publish
    if (@retcode <> 0 or @@error <> 0)
        return 1

    
    if object_id('sysmergearticles') is not NULL
    begin
    -- Update to 110 resolver clsids
        update dbo.sysmergearticles set resolver_clsid =
            case article_resolver
                when @additive_resolver     then @additive_resolver_clsid
                when @average_resolver      then @average_resolver_clsid
                when @download_resolver     then @download_resolver_clsid
                when @max_resolver          then @max_resolver_clsid
                when @mergetxt_resolver     then @mergetxt_resolver_clsid
                when @min_resolver          then @min_resolver_clsid
                when @subwins_resolver      then @subwins_resolver_clsid
                when @upload_resolver       then @upload_resolver_clsid
                when @sp_resolver           then @sp_resolver_clsid
                when @priority_resolver     then @max_resolver_clsid
                when @earlierwins_resolver  then @min_resolver_clsid
                when @laterwins_resolver    then @max_resolver_clsid
                else resolver_clsid
            end
    end

    -- Check if upgrade is needed. If the database is 90 then dispatch to sp_MSmerge_upgrade_from_90rtm
    if object_id('sysmergepublications') is not NULL
    begin
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'use_partition_groups')
        begin
            exec @retcode = sys.sp_MSmerge_upgrade_from_90rtm  
            return @retcode 
        end
    end

    begin tran
    save tran vupgrade_mergetables
    
    /*
     * dbo.sysmergepublications
    */
    if object_id('sysmergepublications') is not NULL
    begin
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'backward_comp_level')
        begin
            alter table dbo.sysmergepublications add backward_comp_level int not NULL default 10 -- defaulted to 70 RTM
            if @@error<>0 goto error
        end
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = N'allow_partition_realignment' and is_nullable = 0)
        	alter table dbo.sysmergepublications alter column allow_partition_realignment bit null
        
    end

    if @remove_repl=0 and object_id('sysmergepublications') is not NULL
    begin
        /*
         * No direct select on sysmergepublications for public
        */
        -- default_access default no longer used 
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and
            name = 'default_access')
        begin
            -- Get the name of the default associated with default_access and drop it
            select @default_name = QUOTENAME(object_name(constid)) 
                from sysconstraints    
                where id = object_id('dbo.sysmergepublications') 
                    and col_name(id, colid) = N'default_access'
            if @default_name is not null
            begin
                -- Drop the default
                exec (N'alter table dbo.sysmergepublications drop constraint ' + @default_name) 
                if @@error<>0 goto error
            end    

            alter table dbo.sysmergepublications drop column default_access
            if @@error<>0 goto error
        end 

        /* 
         * Since the ftp_address is now required to enable a publication for internet, publications 
         * that were enabled for internet can not be upgraded automatically. So the enabled for 
         * internet option is reset in the upgrade process. New FTP columns added later in script. (Shiloh)
        */
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and 
           name = 'enabled_for_internet')
        begin
            if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'ftp_address')
            begin
                exec(N'update dbo.sysmergepublications set enabled_for_internet = 0')
            end
            else
            begin
                exec(N'update dbo.sysmergepublications set enabled_for_internet = 0 where ftp_address is null')
            end
            if @@error<>0 goto error
        end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'snapshot_in_defaultfolder', col_type = 'bit default 0 not null'-- Portable snapshot (Shiloh)
                                                             union all
                                                             select col_name = 'alt_snapshot_folder', col_type = 'nvarchar(255) null'
                                                             union all
                                                             select col_name = 'pre_snapshot_script', col_type = 'nvarchar(255) null'-- Snapshot pre/post scripts (Shiloh)
                                                             union all
                                                             select col_name = 'post_snapshot_script', col_type = 'nvarchar(255) null'
                                                             union all
                                                             select col_name = 'compress_snapshot', col_type = 'bit default 0 not null'-- Snapshot compression (Shiloh)
                                                             union all
                                                             select col_name = 'ftp_address', col_type = 'sysname null'-- Ftp support at publication level (Shiloh)
                                                             union all
                                                             select col_name = 'ftp_port', col_type = 'int not null default 21'
                                                             union all
                                                             select col_name = 'ftp_subdirectory', col_type = 'nvarchar(255) null'
                                                             union all
                                                             select col_name = 'ftp_login', col_type = 'sysname null default N''anonymous'''
                                                             union all
                                                             select col_name = 'ftp_password', col_type = 'nvarchar(524) null'
                                                             union all
                                                             select col_name = 'conflict_retention', col_type = 'int null' -- Conflict retention  (Shiloh)
                                                             union all
                                                             select col_name = 'keep_before_values', col_type = 'int null'-- Keep partition changes (SQL7.0 SP2 )
                                                             union all
                                                             select col_name = 'allow_subscription_copy', col_type = 'bit null default 0'-- Attach & Go (Shiloh)
                                                             union all
                                                             select col_name = 'allow_synctoalternate', col_type = 'bit null default 0'-- Sync to any hub (Shiloh)
                                                             union all
                                                             select col_name = 'web_synchronization_url', col_type = 'nvarchar(500) null'-- WebSync URL (Yukon)
                                                             union all
                                                             select col_name = 'retention_period_unit', col_type = 'tinyint default 0 not null'-- 0=day, 1=week, 2=month, 3=year, 4=hour, 5=minute
                                                             union all
                                                             select col_name = 'validate_subscriber_info', col_type = 'nvarchar(500) NULL'-- Dynamic partition rvalue validation (Shiloh)
                                                             union all
                                                             select col_name = 'ad_guidname', col_type = 'sysname NULL'-- Active directory registration for publications (Shiloh)
                                                             union all
                                                             select col_name = 'max_concurrent_merge', col_type = 'int not NULL default 0'-- max_concurrent_merge control the max # of concurrent merge process at publisher side (Shiloh)
                                                             union all
                                                             select col_name = 'max_concurrent_dynamic_snapshots', col_type = 'int not NULL default 0'-- Maximum number of current dynamic snapshot sessions
                                                             union all
                                                             select col_name = 'use_partition_groups', col_type = 'smallint NULL'
                                                             union all
                                                             select col_name = 'dynamic_filters_function_list', col_type = 'nvarchar(500) NULL'-- Semi-colon delimited list of functions used in all dynamic filters used in this publication
                                                             union all
                                                             select col_name = 'replicate_ddl', col_type = 'int not NULL default 0'-- Bitmask on how this publication accepts new objects
                                                             union all
                                                             select col_name = 'partition_id_eval_proc', col_type = 'sysname NULL'-- Partition id evaluation proc for this publication
                                                             union all
                                                             select col_name = 'publication_number', col_type = 'smallint identity NOT NULL' -- publication_number for this publication (just a mapped value to be used locally instead of the 16-byte guid)
                                                             union all
                                                             select col_name = 'allow_subscriber_initiated_snapshot', col_type = 'bit not NULL default 0'-- allow_subscriber_initiated_snapshot column
                                                             union all
                                                             select col_name = 'allow_partition_realignment', col_type = 'bit not NULL default 1' -- allow_partition_realignment column
                                                             union all
                                                             select col_name = 'generation_leveling_threshold', col_type = 'int null default 1000' -- generation leveling threshold
                                                             union all
                                                             select col_name = 'automatic_reinitialization_policy', col_type = 'bit not null default 0'-- whether or not to upload first on reinits that are triggered by certain publication/article property changes
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergepublications', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
            select @alter_cmd = 'alter table dbo.sysmergepublications add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs

        -- allow web sync (Yukon)
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'allow_web_synchronization')
        begin
            alter table dbo.sysmergepublications add allow_web_synchronization bit null default 0
            if @@error<>0 goto error

            -- we want to set allow_web_synchronization for ssce subscribers. So if we find a 
            -- character mode snapshot type we will set this property.
            exec('update dbo.sysmergepublications set allow_web_synchronization = 1 where sync_mode = 1')
            if @@error<>0 goto error
        end
        
        --insure that index nc2sysmergepublications exists on status
        if exists( select * from sys.indexes where name = 'nc2sysmergepublications' AND 
          			object_id = object_id('dbo.sysmergepublications') )
        begin
          	drop index nc2sysmergepublications on dbo.sysmergepublications
        end
     	 if exists(select * from syscolumns where id = object_id('sysmergepublications') and name = 'status')
	 begin
	 	create index nc2sysmergepublications on dbo.sysmergepublications(status)
	 end
 
				   
	 --insure that default on generation_leveling_threshold is 1000 instead of 0 (or anything else)
	declare @defaultname sysname
	select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc 
              	on sysdc.parent_object_id = sysc.object_id
                        and sysdc.parent_column_id = sysc.column_id
      		where sysdc.parent_object_id = object_id('sysmergepublications')
		       and sysc.name = 'generation_leveling_threshold' 
	 if @defaultname is not null
	 begin
		select @alter_cmd = 'alter table dbo.sysmergepublications drop constraint ' + QUOTENAME(@defaultname)
		exec (@alter_cmd) -- drop old default		
	 end
	 if exists(select * from syscolumns where id = object_id('sysmergepublications') and name = 'generation_leveling_threshold')
	 begin
	 	select @alter_cmd = 'alter table dbo.sysmergepublications add default 1000 for generation_leveling_threshold'
		 exec (@alter_cmd) -- add new default
	 end
	
        -- conflict logging on both publisher and subscriber
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'decentralized_conflicts')
        begin
            alter table dbo.sysmergepublications add decentralized_conflicts int null
            if @@error<>0 goto error

            -- before upgrade, centralized_conflicts==1 means centralized logging,
            -- centralized_conflicts==0 means decentralized logging.
            -- We now map this to the two explicit columns.
            exec ('update dbo.sysmergepublications set decentralized_conflicts=1 where centralized_conflicts=0')
            if @@error<>0 goto error
            exec ('update dbo.sysmergepublications set decentralized_conflicts=0 where centralized_conflicts=1')
            if @@error<>0 goto error
        end
        
        /*
         * sysmergepublications ftp_password
         * no need to upgrade passwords since this column is new in 8.0.
        */
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'ftp_password' and max_length = '1048')
        begin
            declare @cmptlevel tinyint
            select @cmptlevel = cmptlevel from master.dbo.sysdatabases where name = @dbname collate database_default
            if @cmptlevel < 70
            begin
                raiserror (15048, -1, -1, 70, 70, 70, 80)
            end
            else
            begin
                exec( 'alter table dbo.sysmergepublications alter column ftp_password nvarchar(524)' )
                if @@error <> 0
                    goto error
            end
        end

        -- in Yukon snapshot_jobid column has been moved from MSmerge_replinfo to sysmergepublications
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'snapshot_jobid')
        begin
            -- we cannot do alter table and update in the same batch so make this a dynamic sql
            alter table dbo.sysmergepublications add snapshot_jobid  binary(16) NULL
            if @@error <> 0 goto error
            -- get the values of snapshot_jobid from MSmerge_replinfo
            if exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'snapshot_jobid')
            begin
                -- need to exec update in diff process space to avoid syntax error on deferred name resolution at time of proc exec
                exec ('update dbo.sysmergepublications
                set snapshot_jobid = r.snapshot_jobid 
                from dbo.sysmergepublications p, dbo.MSmerge_replinfo r
                where r.repid = p.pubid')
                if @@error <> 0
                    goto error
            end
        end
        
        -- in Yukon distributor column has been moved from sysmergesubscriptions to sysmergepublications
        -- the following is only useful in shiloh to yukon upgrade. In 70 the column distributor did not even exist
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'distributor')
        begin
            -- we will move the value from sysmergesubscriptions to sysmergepublications
            alter table dbo.sysmergepublications add distributor sysname NULL
            if @@error<>0 goto error
            -- get the values of distributor from MSmerge_replinfo
            if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'distributor')
             and exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'publication')
            begin
                -- need to exec update in diff process space to avoid syntax error on deferred name resolution at time of proc exec
                -- add the colums for republisher's publications into sysmergepublications
                exec ('insert into dbo.sysmergepublications (pubid, name, publisher_db, publisher, distributor)
                select distinct s.pubid, s.publication, s.db_name, s.subscriber_server, s.distributor
                    from dbo.sysmergesubscriptions s, dbo.sysmergepublications p
                    where s.subid = s.pubid and s.pubid not in (select pubid from dbo.sysmergepublications)')
                 if @@error <> 0
                    goto error
                exec ('update dbo.sysmergepublications 
                        set distributor = s.distributor
                        from dbo.sysmergesubscriptions s, dbo.sysmergepublications p
                        where s.subid = s.pubid and p.pubid = s.pubid')
                if @@error <> 0
                    goto error
            end
            else
            begin
                -- this is probably a 70 upgrade
                exec ('update dbo.sysmergepublications 
                        set distributor = publisher')
                if @@error <> 0
                    goto error
            end
        end

        -- dynamic_snapshot_queue_timeout column this was in Yukon beta2 but has been removed since
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_queue_timeout')
        begin
            -- now drop the default constraint
            select @constraintname = quotename(name) from sys.default_constraints where parent_object_id = object_id('dbo.sysmergepublications') and parent_column_id =
            	(select column_id from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_queue_timeout')

            exec ('alter table dbo.sysmergepublications drop constraint ' +  @constraintname)   
            if @@error<>0 goto error
            
            alter table dbo.sysmergepublications drop column dynamic_snapshot_queue_timeout    
            if @@error<>0 goto error
        end

        -- dynamic_snapshot_ready_timeout column this was in Yukon beta2 but has been removed since
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_ready_timeout')
        begin
            -- now drop the default constraint
            select @constraintname = quotename(name) from sys.default_constraints where parent_object_id = object_id('dbo.sysmergepublications') and parent_column_id =
            	(select column_id from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'dynamic_snapshot_ready_timeout')

            exec ('alter table dbo.sysmergepublications drop constraint ' +  @constraintname)   
            if @@error<>0 goto error

            alter table dbo.sysmergepublications drop column dynamic_snapshot_ready_timeout    
            if @@error<>0 goto error
        end
    end -- end sysmergepublications modifications

    -- dbo.MSmerge_contents
    if object_id('dbo.MSmerge_contents') is not null and @remove_repl=0
    begin
    	-- insure that index nc4Msmerge_contents exists on rowguid
	if exists( select * from sys.indexes where name = 'nc4MSmerge_contents' and 
		object_id = object_id('MSmerge_contents') )
	begin
		drop index nc4MSmerge_contents on dbo.MSmerge_contents
	end
	if exists(select * from syscolumns where id = object_id('MSmerge_contents') and name = 'rowguid')
	begin
		create index nc4MSmerge_contents on dbo.MSmerge_contents(rowguid)
	end
    end

    -- dbo.MSmerge_genhistory
    if object_id('dbo.MSmerge_genhistory') is not null and @remove_repl=0
    begin
	-- insure that index nc2Msmerge_genhistory exists on rowguid
	if exists( select * from sys.indexes where name = 'nc2MSmerge_genhistory' and 
		object_id = object_id('MSmerge_genhistory') )
	begin
		drop index nc2MSmerge_genhistory on dbo.MSmerge_genhistory
	end
	if exists(select * from syscolumns where id = object_id('MSmerge_genhistory') and name = 'genstatus')
		and exists(select * from syscolumns where id = object_id('MSmerge_genhistory') and name = 'art_nick')
		and exists(select * from syscolumns where id = object_id('MSmerge_genhistory') and name = 'changecount')
	begin
		create  index nc2MSmerge_genhistory on MSmerge_genhistory(genstatus, art_nick,changecount) 
	end
    end

    /* 
     * MSmerge_history
     * Add new unique idx for correctness iff there are no uniqueness violations. Drop old
     * index in favor of new column order in this index. Add new non-clustered index as needed.
    */
    if exists( select * from sys.indexes where name = 'nc1MSmerge_history' AND 
                object_id = OBJECT_ID('MSmerge_history'))
    begin
    	drop index dbo.MSmerge_history.nc1MSmerge_history
    end
    if exists(select * from sys.columns where object_id = object_id('MSmerge_history') and name = 'session_id')
    	and exists (select * from sys.columns where object_id = object_id('MSmerge_history') and name = 'timestamp')
    begin
    	create nonclustered index nc1MSmerge_history on MSmerge_history(session_id, timestamp)
    end

    -- dbo.MSmerge_replinfo
    if @remove_repl=0 and (object_id('MSmerge_replinfo') is not NULL and
    exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'replnickname'))
    begin
        -- recgen is int in Shiloh and before, bigint in Yukon and after
        if 56 = (select system_type_id from sys.columns where 
                    object_id = object_id('MSmerge_replinfo') and name = 'recgen')
        begin
            alter table dbo.MSmerge_replinfo alter column recgen bigint null
            if @@error<>0 goto error
        end

        -- sentgen is int in Shiloh and before, bigint in Yukon and after
        if 56 = (select system_type_id from sys.columns where 
                    object_id = object_id('MSmerge_replinfo') and name = 'sentgen')
        begin
            alter table dbo.MSmerge_replinfo alter column sentgen bigint null
            if @@error<>0 goto error
        end

        -- replnickname is int in Shiloh and before, binary(6) in Yukon and after
        if 56 = (select system_type_id from sys.columns where 
                    object_id = object_id('MSmerge_replinfo') and name = 'replnickname')
        begin
            begin tran
            save tran tran_replinfonick80to90
                alter table dbo.MSmerge_replinfo alter column replnickname binary(6) not null
                if @@error<>0 goto err_replinfonick80to90
                exec ('update dbo.MSmerge_replinfo set replnickname= substring(replnickname, 6, 1) + substring(replnickname, 5, 1) + substring(replnickname, 4, 1) + substring(replnickname, 3, 1) + substring(replnickname, 2, 1) + substring(replnickname, 1, 1)')
                if @@error<>0 goto err_replinfonick80to90
            commit tran
            goto after_replinfonick80to90

            err_replinfonick80to90:
            rollback tran tran_replinfonick80to90
            commit tran
            goto error
        end

        after_replinfonick80to90:

        -- this column and its values been added to sysmergepublications in the sysmergepublications if block
        if exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'snapshot_jobid')
        begin
            alter table dbo.MSmerge_replinfo drop column snapshot_jobid
            if @@error <> 0 goto error
        end
        
        -- hostname column added for Yukon
        if not exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'hostname')
        begin
            alter table dbo.MSmerge_replinfo add hostname sysname NULL
            if @@error <> 0 goto error
        end
    end -- dbo.MSmerge_replinfo
    
    /*
    * dbo.sysmergesubscriptions
    */
    -- the following modifications to sysmergesubscriptions have to be done even if replication is being removed
    if (object_id('sysmergesubscriptions') is not NULL)
    begin
        -- subscriber_server (Shiloh) 
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'subscriber_server')
        begin
            alter table dbo.sysmergesubscriptions add subscriber_server sysname null
            if @@error <> 0 goto error
            
            -- need to exec update in diff process space to avoid syntax error on deferred name resolution at time of proc exec
            exec( N'update dbo.sysmergesubscriptions set subscriber_server = 
                        (select srvname from master.dbo.sysservers where srvid = dbo.sysmergesubscriptions.srvid)' )
            if @@error <> 0 goto error
        end

        -- last_makegeneration_datetime
        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'last_makegeneration_datetime')
        begin
            alter table dbo.sysmergesubscriptions add last_makegeneration_datetime datetime null
            if @@error <> 0 goto error            
        end



        if exists (select * from sysconstraints where id = object_id('dbo.sysmergesubscriptions') and 
                            object_name(constid) = 'unique_pubsrvdb')
        begin
            alter table dbo.sysmergesubscriptions drop constraint unique_pubsrvdb
            if @@error <> 0 goto error
        end
        
        IF EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc2sysmergesubscriptions' AND
                        id = object_id('dbo.sysmergesubscriptions') )
        begin
            drop index nc2sysmergesubscriptions on dbo.sysmergesubscriptions 
            if @@error <> 0 goto error
        end
        
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'srvid')
        begin
            
            alter table dbo.sysmergesubscriptions drop column srvid
            if @@error <> 0 goto error
        end

        if not exists (select * from sysconstraints where id = object_id('dbo.sysmergesubscriptions') and 
                            object_name(constid) = 'unique_pubsrvdb')
        begin
            exec(N'alter table dbo.sysmergesubscriptions 
                    add constraint unique_pubsrvdb 
                    unique nonclustered (pubid, subscriber_server, db_name)')
            if @@error <> 0 goto error
        end
    end
    
    if @remove_repl=0 and (object_id('sysmergesubscriptions') is not NULL)
    begin
        -- rename partnerid to replicastate... since we will be copying the table
        -- we will do this first so that the remaining changes will not need to be copied
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'partnerid')
        begin
            -- because sp_rename does not allow the renaming of tables marked as system objects
            -- we have to take this round about way of doing things. This table should always be
            -- relatively small so perf here is not a really big concern and should not be affected
            --  exec sys.sp_rename @objname = 'sysmergesubscriptions.partnerid', @newname = 'replicastate', @objtype = 'COLUMN'
            --  if @@error <> 0 goto error
            if object_id(N'sysmergesubscriptions_tmp_name') is not null
            begin
                drop table sysmergesubscriptions_tmp_name
                if @@error <> 0 goto error
            end

            select * 
                into sysmergesubscriptions_tmp_name 
                from dbo.sysmergesubscriptions
            if @@error <> 0 goto error

            exec sys.sp_rename @objname = 'sysmergesubscriptions_tmp_name.partnerid', @newname = 'replicastate', @objtype = 'COLUMN'
            if @@error <> 0
            begin
                drop table sysmergesubscriptions_tmp_name
                goto error
            end

            drop table sysmergesubscriptions
            if @@error <> 0 goto error

            exec sys.sp_rename @objname = 'sysmergesubscriptions_tmp_name', @newname = 'sysmergesubscriptions'
            if @@error <> 0 
            begin
                drop table sysmergesubscriptions_tmp_name
                goto error
            end
  		

            -- recreate indexes
            create unique clustered index uc1sysmergesubscriptions on dbo.sysmergesubscriptions (subid) 
            if @@error <> 0 goto error

            create index nc2sysmergesubscriptions on dbo.sysmergesubscriptions (subscriber_server, db_name)
            if @@error <> 0 goto error

            -- mark as system object
            exec sp_MS_marksystemobject 'sysmergesubscriptions'
            if @@error <> 0 goto error
        end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'use_interactive_resolver', col_type = 'bit NOT NULL default 0'-- Interactive resolver support (Shiloh)
                                                             union all
                                                             select col_name = 'validation_level', col_type = 'int NOT NULL default 0'-- merge validation level (Shiloh)
                                                             union all
                                                             select col_name = 'resync_gen', col_type = 'bigint not NULL default -1'
                                                             union all
                                                             select col_name = 'attempted_validate', col_type = 'datetime NULL' -- date of the last attempted validate (Shiloh)
                                                             union all
                                                             select col_name = 'last_sync_status', col_type = 'int NULL'-- status of the last sync (Shiloh)
                                                             union all
                                                             select col_name = 'last_sync_date', col_type = 'datetime NULL'-- date of the last sync (Shiloh)
                                                             union all
                                                             select col_name = 'last_sync_summary', col_type = 'sysname NULL'-- summary message of the last sync (Shiloh)
                                                             union all
                                                             select col_name = 'metadatacleanuptime', col_type = 'datetime not NULL default getdate()'-- metadata cleanup time
                                                             union all
                                                             select col_name = 'cleanedup_unsent_changes', col_type = 'bit NOT NULL default 0'-- cleanedup_unsent_changes(Yukon)
                                                             union all
                                                             select col_name = 'replica_version', col_type = 'int NOT NULL default 60'-- replica_version (Yukon)
                                                             union all
                                                             select col_name = 'supportability_mode', col_type = 'int NOT NULL default 0'-- supportability_mode (Yukon)
                                                             union all
                                                             select col_name = 'application_name', col_type = 'sysname NULL'-- application_name and subscriber_number added in yukon
                                                             union all 
                                                             select col_name = 'subscriber_number', col_type = 'int identity not NULL'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergesubscriptions', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin        
            select @alter_cmd = 'alter table dbo.sysmergesubscriptions add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 
            begin
            		goto error
            end
            
        end
        close colcurs
        deallocate colcurs

		-- drop old views 
		declare @old_view_name sysname
		declare drop_old_views_cursor cursor LOCAL FAST_FORWARD FOR
		select name from sys.objects where (name like 'ctsv_%' or name like 'tsvw_%') AND type ='V' and  ObjectProperty(object_id, 'IsMSShipped')=1
		for read only
        open drop_old_views_cursor
        fetch drop_old_views_cursor into @old_view_name
		while(@@fetch_status <> -1)
		begin
			declare @drop_view_cmd nvarchar(max)
			select @drop_view_cmd = N'drop view ' + QUOTENAME(@old_view_name) 
			exec(@drop_view_cmd)
			if @@error <> 0 goto error
			fetch drop_old_views_cursor into @old_view_name
		end	
		close drop_old_views_cursor
        deallocate drop_old_views_cursor


        IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc2sysmergesubscriptions' AND
            id = object_id('dbo.sysmergesubscriptions') )
        begin
            create index nc2sysmergesubscriptions on dbo.sysmergesubscriptions (subscriber_server, db_name)
            if @@error <> 0 goto error
        end

        -- Remove alternate_pubid column from sysmergesubscriptions (Shiloh)
        -- This column is dropped in 8.0 Beta 2
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and
                            name = 'alternate_pubid')
        begin
            alter table dbo.sysmergesubscriptions drop column alternate_pubid
            if @@error <> 0 goto error
        end

        -- this column and its values been added to sysmergepublications in the sysmergepublications if block (yukon)
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'distributor')
        begin
            alter table sysmergesubscriptions drop column distributor
            if @@error <> 0 goto error
        end

        -- this column and its values been added to sysmergepublications in the sysmergepublications if block (yukon)
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergesubscriptions') and name = 'publication')
        begin
            alter table sysmergesubscriptions drop column publication
            if @@error <> 0 goto error
        end
    end 



    -- move of columns from sysmergesubscriptions to MSmerge_replinfo and vice versa for the sake of 
    -- better consistency and subscriber tracking.
    if @remove_repl=0 and object_id('sysmergesubscriptions') is not NULL and
         (object_id('MSmerge_replinfo') is not NULL and
         exists (select * from sys.columns where object_id = object_id('MSmerge_replinfo') and name = 'replnickname'))
    begin
        select * 
            into #sysmergesubscriptions
            from sysmergesubscriptions
        if @@error <> 0 goto error
        
        select * 
            into #MSmerge_replinfo
            from MSmerge_replinfo
        if @@error <> 0 goto error
        begin tran
        save tran tran_upgrademergesubtables
            drop table dbo.sysmergesubscriptions
            if @@error<>0
                goto err_upgrademergesubtables

            drop table dbo.MSmerge_replinfo
            if @@error<>0
                goto err_upgrademergesubtables

            -- this creates the sysmergesubscription and MSmerge_replinfo tables with new schema
            exec @retcode = sys.sp_MSmerge_create_sub_table
            if @retcode<>0 or @@error<>0
                goto err_upgrademergesubtables
            -- insert the values for the new set of columns
            exec('insert into dbo.sysmergesubscriptions
                (subscriber_server, db_name, pubid, datasource_type, subid, replnickname,
                 replicastate, status, subscriber_type, subscription_type, sync_type,
                 description, priority, recgen, recguid, sentgen, sentguid,
                 schemaversion, schemaguid, last_validated, attempted_validate,
                 last_sync_date, last_sync_status, last_sync_summary, 
                 metadatacleanuptime, cleanedup_unsent_changes)
            select sub.subscriber_server, sub.db_name, sub.pubid, sub.datasource_type, sub.subid, rep.replnickname,
                   sub.replicastate, sub.status, sub.subscriber_type, sub.subscription_type, sub.sync_type,
                   sub.description, sub.priority, rep.recgen, rep.recguid, rep.sentgen, rep.sentguid,
                   rep.schemaversion, rep.schemaguid, sub.last_validated, sub.attempted_validate,
                   sub.last_sync_date, sub.last_sync_status, sub.last_sync_summary,
                   sub.metadatacleanuptime, sub.cleanedup_unsent_changes
            from #sysmergesubscriptions sub, #MSmerge_replinfo rep
            where sub.subid = rep.repid')
            if @retcode<>0 or @@error<>0
                goto err_upgrademergesubtables
            exec('insert into dbo.MSmerge_replinfo
                (repid, use_interactive_resolver, validation_level, resync_gen, 
                  login_name, merge_jobid)
            select rep.repid, sub.use_interactive_resolver, sub.validation_level, sub.resync_gen, 
                  sub.login_name, rep.merge_jobid
            from #sysmergesubscriptions sub, #MSmerge_replinfo rep
            where sub.subid = rep.repid')
            if @retcode<>0 or @@error<>0
                goto err_upgrademergesubtables		

        commit tran
        goto after_upgrademergesubtables

        err_upgrademergesubtables:
        rollback tran tran_upgrademergesubtables
        commit tran
        goto error
        
        after_upgrademergesubtables: 
        -- if we got here everything was successful.
        drop table #sysmergesubscriptions
        drop table #MSmerge_replinfo
    end

    if @remove_repl=0 and object_id('dbo.sysmergesubscriptions') is not NULL
    begin
	 --Dropping columns use_interactive_resolver, validation_level, resync_gen
	 if exists (select * from sys.columns where name = N'use_interactive_resolver' and object_id = object_id('dbo.sysmergesubscriptions'))
	 begin 
	 	-- drop constraint on this if it exists
	 	select @defaultname = null --temp stores constraint name to be removed
		select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
				(sysdc.parent_object_id = sysc.object_id and 
					sysdc.parent_column_id = sysc.column_id)		
			where sysc.object_id = object_id('dbo.sysmergesubscriptions') and
				sysc.name = 'use_interactive_resolver'
	 	if @defaultname is not null
	 	begin
	 		select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop constraint ' + QUOTENAME(@defaultname)
	 		exec (@alter_cmd)
	 	end
	 	-- drop column for Yukon
	 	select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop column use_interactive_resolver'
	 	exec (@alter_cmd)
	 end
	 if exists (select * from sys.columns where name = N'validation_level' and object_id = object_id('dbo.sysmergesubscriptions'))	
	 begin
	 	-- drop constraint on this if it exists
	 	select @defaultname = null -- temp stores constraint name to be removed
		select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
				(sysdc.parent_object_id = sysc.object_id and 
					sysdc.parent_column_id = sysc.column_id)		
			where sysc.object_id = object_id('dbo.sysmergesubscriptions') and
					sysc.name = 'validation_level'
	 	if @defaultname is not null
	 	begin
	 		select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop constraint ' + QUOTENAME(@defaultname)
	 		exec (@alter_cmd)
	 	end 
	 	-- drop column for Yukon
	 	select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop column validation_level'
	 	exec (@alter_cmd)
	 end
	 if exists (select * from sys.columns where name = N'resync_gen' and object_id = object_id('dbo.sysmergesubscriptions'))
	 begin	
	 	-- constraint (default) must be deleted before we can delete resync_gen
		select @defaultname = null --temp stores constraint name to be removed
	 	select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
				(sysdc.parent_object_id = sysc.object_id and 
					sysdc.parent_column_id = sysc.column_id)		
			where sysc.object_id = object_id('dbo.sysmergesubscriptions') and
					sysc.name = 'resync_gen'
	 	if @defaultname is not null
	 	begin
	 		select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop constraint ' + QUOTENAME(@defaultname)
	 		exec (@alter_cmd)
	 	end
	 	-- drop column for Yukon
	 	select @alter_cmd = 'alter table dbo.sysmergesubscriptions drop column resync_gen'
	 	exec (@alter_cmd)
	 end
	 	
	 --insure index nc3sysmergesubscriptions exists on replnickname
	 if exists( select * from sys.indexes where name = 'nc3sysmergesubscriptions' and object_id = object_id('dbo.sysmergesubscriptions') )
	 begin
	 	drop index dbo.sysmergesubscriptions.nc3sysmergesubscriptions
	 end
	 if exists(select * from syscolumns where id = object_id('dbo.sysmergesubscriptions') and name = 'replnickname')
	 begin  -- if column replnickname exists create the index on it
	 	create index nc3sysmergesubscriptions on dbo.sysmergesubscriptions(replnickname)
	 end
    end


    /*
     * dbo.sysmergearticles
    */
    -- the following have to be done for sp_MSremovedbreplication to work correctly
    -- even we are going to remove replication
    if object_id('sysmergearticles') is not NULL
    begin
        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'lightweight', col_type = 'bit not null default 0'
                                                             union all
                                                             select col_name = 'before_upd_view_objid', col_type = 'int NULL'
                                                             union all                                                             
                                                             select col_name = 'metadata_select_proc', col_type = 'sysname NULL'
                                                             union all                                                             
                                                             select col_name = 'delete_proc', col_type = 'sysname NULL'
                                                             union all                                                             
                                                             select col_name = 'before_image_objid', col_type = 'int NULL'-- Keep partition changes (SQL7.0 SP2)
                                                             union all                                                             
                                                             select col_name = 'before_view_objid', col_type = 'int NULL'
                                                             union all                                                             
                                                             select col_name = 'preserve_rowguidcol', col_type = 'bit not null default 1'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergearticles', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
            select @alter_cmd = 'alter table dbo.sysmergearticles add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = N'delete_tracking' and is_nullable = 0)
        	alter table dbo.sysmergearticles alter column delete_tracking bit null
    end
    
    if @remove_repl=0 and object_id('sysmergearticles') is not NULL
    begin
        -- Set all invalid dbo.sysmergearticles.sync_objid to the corresponding 
        -- objid, this will allow regeneration of article procs to succeed
        update dbo.sysmergearticles 
           set sync_objid = objid 
         where object_name(sync_objid) is null
        if @@error <> 0 goto error

        -- Make sure that article type in dbo.sysmergearticles is not null 
        -- for upgraded republisher
        update dbo.sysmergearticles
        set type = 0x0a
        where type is null
        if @@error <> 0 goto error

        -- Turn on the trigger schema option, FK schema option, check 
        -- constraint schema option, and default schema option    by default 
        -- so merge articles will retain the old scripting behaviour (Shiloh)
        -- Also reset the 0x8000 option (PKUK as constraints) as it is 
        -- deprecated starting from yukon. 
        update dbo.sysmergearticles 
            set schema_option = (convert(bigint, schema_option) | convert(bigint, 0x00000F00)) & ~convert(bigint, 0x00008000)
            from sys.objects so
            inner join dbo.sysmergearticles sa
            on so.object_id = sa.objid
            where so.type = 'U' -- table articles only    
        if @@error <> 0 goto error

        -- Add new non-clustered idx on nickname.
        if not exists (select * from sys.indexes where name = 'nc1sysmergearticles')
        begin
            create nonclustered index nc1sysmergearticles on dbo.sysmergearticles(nickname) 
            if @@error <> 0 goto error
        end

        -- Resolver info column datatype change 
        if exists( select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = 'resolver_info' )
        begin
            alter table dbo.sysmergearticles alter column resolver_info nvarchar(517) NULL
            if @@error <> 0 goto error
        end

        exec @retcode = sys.sp_MSUpgradeConflictTable
        if @@ERROR<>0 or @retcode<>0
            goto error

        if object_id('MSmerge_delete_conflicts') is not NULL
        begin
            drop table dbo.MSmerge_delete_conflicts
            if @@error <> 0 goto error
        end

        /* Update the columns column sysmergearticles by counting the number of columns int the
           table. 70 did not have vertical partitioning so the columns column is NULL */
        declare articlescurs cursor LOCAL FAST_FORWARD 
        for (select sma.name, sma.objid, sma.pubid from dbo.sysmergearticles sma 
                    where sma.columns is NULL and sys.fn_MSmerge_islocalpubid(pubid) = 1)
        
        for read only
        open articlescurs
        fetch articlescurs into @article, @objid, @pubid
        while(@@fetch_status <> -1)
        begin


            SELECT @cnt = max(column_id), @idx = 1 FROM sys.columns WHERE object_id = @objid 
            SELECT @columns = NULL
            WHILE @idx <= @cnt
            BEGIN
                /* to make sure column holes will not be included */
                if exists (select * from sys.columns where column_id=@idx and object_id=@objid and 
                    (is_computed<>1 and system_type_id <> type_id('timestamp')))
                begin
                    exec sys.sp_MSsetbit @bm=@columns OUTPUT, @coltoadd=@idx, @toset = 1
                    if @@ERROR<>0 or @retcode<>0
                    begin
                        close articlescurs
                        deallocate articlescurs
                        goto error
                    end

                end
                SELECT @idx = @idx + 1
            END
            UPDATE dbo.sysmergearticles SET columns = @columns WHERE name = @article AND pubid = @pubid

            fetch articlescurs into @article, @objid, @pubid
        end

        close articlescurs
        deallocate articlescurs

        if @@error <> 0 goto error

        if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and
                            name = 'excluded_col_count')
        begin
            -- make sure 7.5's column bit map does not get messed up.
            if exists (select * from dbo.sysmergearticles)
            begin
            -- enough to hold 246 columns in one table - after upgrade all columns are in partition 
            -- as in sphinx there is no way to kick any of them out.
            -- in case a publication exists in downlevel subscriber without having ran snapshot.
            -- missing_col_count would be NULL, which can cause problems when being upgraded to
            -- latest version. 
                update dbo.sysmergearticles set missing_col_count=0,missing_cols=0x00 where missing_col_count is NULL
                if @@error <> 0 goto error
            end

            alter table dbo.sysmergearticles add excluded_col_count int NULL
            if @@error <> 0 goto error
        end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'gen_cur', col_type = 'bigint NULL'-- Current generation for new generation assignment added in SQL7.0 SP1
                                                             union all
                                                             select col_name = 'excluded_cols', col_type = 'varbinary(128) NULL'-- Vertical Partitioning (Shiloh)
                                                             union all
                                                             select col_name = 'vertical_partition', col_type = 'int NULL'
                                                             union all
                                                             select col_name = 'identity_support', col_type = 'int default 0 NOT NULL '-- Auto identity management (Shiloh)
                                                             union all
                                                             select col_name = 'destination_owner', col_type = 'sysname default ''dbo'' not NULL'-- Destination owner support (Shiloh)
                                                             union all
                                                             select col_name = 'verify_resolver_signature', col_type = 'int NULL default 1'-- Resolver certificate support (Shiloh)
                                                             union all
                                                             select col_name = 'allow_interactive_resolver', col_type = 'bit NOT NULL default 0'-- Interactive resolver (Shiloh)
                                                             union all
                                                             select col_name = 'published_in_tran_pub', col_type = 'bit NOT NULL default 0'
                                                             union all
                                                             select col_name = 'fast_multicol_updateproc', col_type = 'bit NOT NULL default 0'-- Whether update proc should do one update per column or multiple columns in one update (Shiloh)
                                                             union all
                                                             select col_name = 'check_permissions', col_type = 'int NOT NULL default 0'
                                                             union all
                                                             select col_name = 'processing_order', col_type = 'int NOT NULL default 0'
                                                             union all
                                                             select col_name = 'maxversion_at_cleanup', col_type = 'int not null default 1'
                                                             union all
                                                             select col_name = 'upload_options', col_type = 'tinyint not null default 0'
                                                             union all
                                                             select col_name = 'procname_postfix', col_type = 'nchar(32) null'
                                                             union all
                                                             select col_name = 'well_partitioned_lightweight', col_type = 'bit null'
                                                             union all
                                                             select col_name = 'delete_tracking', col_type = 'bit not null default 1'
                                                             union all
                                                             select col_name = 'compensate_for_errors', col_type = 'bit not null default 0'
                                                             union all
                                                             select col_name = 'pub_range', col_type = 'bigint null'
                                                             union all
                                                             select col_name = 'range', col_type = 'bigint NULL'
                                                             union all
                                                             select col_name = 'threshold', col_type = 'int NULL'
                                                             union all
                                                             select col_name = 'stream_blob_columns', col_type = 'bit not NULL default 0'
                                                             union all
                                                             select col_name = 'deleted_cols', col_type = 'varbinary(128) NULL default 0x0'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.sysmergearticles', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
			select @alter_cmd = 'alter table dbo.sysmergearticles add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
		    exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs

		-- add default constraint on verify_resolver_signature column
		if not exists(
		select * 
        from sysconstraints as con join sys.columns as col 
            on con.colid = col.column_id
                and con.id = col.object_id
                and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
                and col.object_id = object_id('dbo.sysmergearticles')
                and col.name = 'verify_resolver_signature')and exists 
				(select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = 'verify_resolver_signature')
		begin
			exec('alter table dbo.sysmergearticles add default 1 for verify_resolver_signature')  
			if @@error <> 0 goto error	
		end

		
		-- change default constraint on compensate_for_errors column to be 0 and also update all values for this column to be 0
		-- we think that compensate_for_errors=1 (sql2k default)is not very usefull and can cause more harm then good.
		if exists(select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and name = 'compensate_for_errors')
		begin
			declare @default_compensate_for_errors_constraint_name nvarchar(258)
			select @default_compensate_for_errors_constraint_name = obj.name 
			from sysconstraints as con join sys.columns as col 
				on con.colid = col.column_id 
					and con.id = col.object_id
					and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
					and col.object_id = object_id('dbo.sysmergearticles')
					and col.name = 'compensate_for_errors'
					join sys.objects as obj
					on obj.object_id=con.constid

			if(@default_compensate_for_errors_constraint_name is not  null)
			begin
				select @default_compensate_for_errors_constraint_name = quotename(@default_compensate_for_errors_constraint_name)
				exec ('alter table dbo.sysmergearticles  drop constraint ' + @default_compensate_for_errors_constraint_name)
				if @@error <> 0 goto error
			end			
			exec('alter table dbo.sysmergearticles add default 0 for compensate_for_errors')  
			if @@error <> 0 goto error
			exec('update dbo.sysmergearticles set compensate_for_errors = 0 ')			
			if @@error <> 0 goto error
		end

        -- gen_cur is int in SQL8 and earlier, bigint in SQL9
        if 56 = (select system_type_id from sys.columns where 
                        object_id = object_id('dbo.sysmergearticles') and name = 'gen_cur')
        begin
            alter table dbo.sysmergearticles alter column gen_cur bigint null
            if @@error <> 0 goto error
        end
        
        -- Set default value of column destination_owner if NULL - could happen if first upgraded
        -- from 7.0 to Beta 2, which does not have the default value and then to 80 RTM.

        -- Destination owner support (Shiloh)
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergearticles') and
                        name = 'destination_owner')
        begin
            exec ('update dbo.sysmergearticles set destination_owner=''dbo'' where destination_owner is NULL')
            if @@error <> 0 goto error
        end
        
        if not exists (select * from sys.indexes where name = 'nc2sysmergearticles')
        begin
            create nonclustered index nc2sysmergearticles on sysmergearticles(processing_order) 
            if @@error <> 0 goto error
        end
        
        if not exists (select * from sys.indexes where name = 'nc3sysmergearticles')
        begin
            create unique nonclustered index nc3sysmergearticles on dbo.sysmergearticles(objid, pubid) 
            if @@ERROR <> 0    goto error
        end
        --
        -- add default for compensate_for_errors
        --
        if not exists (select dc.name 
                        from sys.default_constraints as dc 
                            join sys.columns as c
                                on dc.parent_object_id = c.object_id
                                    and dc.parent_column_id = c.column_id
                        where c.object_id = object_id(N'dbo.sysmergearticles')
                            and c.name = N'compensate_for_errors')
        begin
            alter table dbo.sysmergearticles add default 0 for compensate_for_errors
        end
        --
        -- add default for excluded_col_count
        --
        if not exists (select dc.name 
                        from sys.default_constraints as dc 
                            join sys.columns as c
                                on dc.parent_object_id = c.object_id
                                    and dc.parent_column_id = c.column_id
                        where c.object_id = object_id(N'dbo.sysmergearticles')
                            and c.name = N'excluded_col_count')
        begin
            alter table dbo.sysmergearticles add default 0 for excluded_col_count
        end
        --
        -- add default for vertical_partition
        --
        if not exists (select dc.name 
                        from sys.default_constraints as dc 
                            join sys.columns as c
                                on dc.parent_object_id = c.object_id
                                    and dc.parent_column_id = c.column_id
                        where c.object_id = object_id(N'dbo.sysmergearticles')
                            and c.name = N'vertical_partition')
        begin
            alter table dbo.sysmergearticles add default 0 for vertical_partition
        end
        --
        -- remove the default for column destination_owner
        --
        select @column_name = NULL
        select @column_name = dc.name 
        from sys.default_constraints as dc 
            join sys.columns as c
                on dc.parent_object_id = c.object_id
                    and dc.parent_column_id = c.column_id
        where c.object_id = object_id('dbo.sysmergearticles')
            and c.name = N'destination_owner'
        if (@column_name is not null)
        begin
            select @alter_cmd = N'alter table dbo.sysmergearticles drop constraint ' + quotename(@column_name)
            exec(@alter_cmd)
            if @@error <> 0 
                return 1
        end

        -- this table exists at publisher and subscriber dbs
        if object_id('MSmerge_identity_range') is NULL
        begin
            create table dbo.MSmerge_identity_range (
                subid               uniqueidentifier not NULL,
                artid               uniqueidentifier not NULL,
                range_begin         numeric(38,0) NULL,
                range_end           numeric(38,0) NULL,
                next_range_begin    numeric(38,0) NULL,
                next_range_end      numeric(38,0) NULL,
                is_pub_range        bit not NULL,
                max_used            numeric(38,0) NULL
            )
            if @@error <> 0 goto error
            
            exec dbo.sp_MS_marksystemobject MSmerge_identity_range
            if @@error <> 0 goto error
            
            create unique clustered index uclidrange on MSmerge_identity_range(subid, artid, is_pub_range)
            if @@error <> 0 goto error
        end

        if object_id('MSmerge_settingshistory') is NULL
        begin
            --raiserror('Creating table MSmerge_settingshistory',0,1)

            --This table records the history of when merge related settings
            --were changed. It can also bo used to record important events
            --that affect behavior of merge replication.

            --eventtype can have one of the following values
            --  1   Initial publication level property setting.
            --  2   Change in publication property.
            --  101 Initial article level property setting.
            --  102 Change in article property.
            --  In future add publication related event below 100 and
            --  article related events about 100 to make searching easier

            create table dbo.MSmerge_settingshistory
            (
                eventtime        datetime           null default getdate(),
                pubid            uniqueidentifier    NOT NULL,
                artid           uniqueidentifier    NULL,
                eventtype         tinyint                NOT NULL,
                propertyname    sysname             NULL,
                   previousvalue   sysname             NULL,
                newvalue        sysname             NULL,
                eventtext        nvarchar(2000)         NULL    
            )
                    
            if @@error <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_settingshistory
            if @@error <> 0 goto error

            create clustered index c1MSmerge_settingshistory on MSmerge_settingshistory(pubid,eventtype) 
            if @@error <> 0 goto error

        end
        else
        begin
        if exists (select * from sys.columns where object_id = object_id('dbo.MSmerge_settingshistory') and name = N'eventtime' and is_nullable = 0)
        	alter table dbo.MSmerge_settingshistory alter column eventtime datetime           null 
        end

        if object_id('sysmergepartitioninfo') is NULL
        begin
            create table dbo.sysmergepartitioninfo 
            (
                artid                           uniqueidentifier     NOT NULL,
                pubid                           uniqueidentifier     NOT NULL,
                partition_view_id               int                  NULL,
                repl_view_id                    int                  NULL,
                partition_deleted_view_rule     nvarchar(max)        NULL,
                partition_inserted_view_rule    nvarchar(max)        NULL,
                membership_eval_proc_name       sysname              NULL,
                column_list                     nvarchar(max)        NULL,
                column_list_blob                nvarchar(max)        NULL,
                expand_proc                     sysname              NULL,
                logical_record_parent_nickname  int                  NULL,
                logical_record_view             int                  NULL,
                logical_record_deleted_view_rule nvarchar(max)       NULL,
                logical_record_level_conflict_detection bit       null   default 0,
                logical_record_level_conflict_resolution bit      null   default 0,
                partition_options               tinyint        null      default 0
            )
            if @@error <> 0 goto error
            
            create unique clustered index uc1sysmergepartitioninfo
                on dbo.sysmergepartitioninfo(artid, pubid) 
            if @@error <> 0 goto error
            
            exec dbo.sp_MS_marksystemobject sysmergepartitioninfo
            if @@error <> 0 goto error

            -- we need to insert a row for every article in sysmergearticles into sysmergepartitioninfo
            insert dbo.sysmergepartitioninfo (artid, pubid)
                select artid, pubid from dbo.sysmergearticles
            if @@error <> 0 goto error
            
        end
        else
        begin
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepartitioninfo') and name = N'logical_record_level_conflict_detection' and is_nullable = 0)
        	alter table dbo.sysmergepartitioninfo alter column logical_record_level_conflict_detection bit null
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepartitioninfo') and name = N'logical_record_level_conflict_resolution' and is_nullable = 0)
        	alter table dbo.sysmergepartitioninfo alter column logical_record_level_conflict_resolution bit null
        if exists (select * from sys.columns where object_id = object_id('dbo.sysmergepartitioninfo') and name = N'partition_options' and is_nullable = 0)
        	alter table dbo.sysmergepartitioninfo alter column partition_options tinyint null
        end

        if object_id('sysmergepartitioninfoview') is not NULL
        begin
            drop view dbo.sysmergepartitioninfoview
        end
        
        exec ('create view dbo.sysmergepartitioninfoview as
            select sma.*, smaw.partition_view_id, 
                smaw.repl_view_id,
                smaw.partition_deleted_view_rule,
                smaw.partition_inserted_view_rule,
                smaw.membership_eval_proc_name,
                smaw.column_list,
                smaw.column_list_blob,
                smaw.expand_proc,
                smaw.logical_record_parent_nickname,
                smaw.logical_record_view,
                smaw.logical_record_deleted_view_rule,
                smaw.logical_record_level_conflict_detection,
                smaw.logical_record_level_conflict_resolution,
                smaw.partition_options
           from dbo.sysmergearticles sma, dbo.sysmergepartitioninfo smaw
           where sma.artid = smaw.artid and sma.pubid = smaw.pubid')            
        if @@error <> 0 goto error

        exec dbo.sp_MS_marksystemobject sysmergepartitioninfoview
        if @@error <> 0 goto error

        -- Schema only articles (Shiloh)
        if object_id('dbo.sysmergeschemaarticles', 'U') is null
        begin
            exec @retcode= sys.sp_MScreate_sysmergeschemaarticles_table
            if @@error <> 0 or @retcode <> 0 goto error
        end
        else
        begin
            if not exists (select * from sys.columns where object_id = object_id('sysmergeschemaarticles') and
                        name = 'processing_order')
            begin
                alter table sysmergeschemaarticles add processing_order int NOT NULL default 0
                if @@error <> 0 goto error
            end
        end

        -- create view now that sysmergearticles is altered and sysmergeextendedarticles is created
        if object_id('sysmergeextendedarticlesview') is not NULL
        begin
            drop view dbo.sysmergeextendedarticlesview
        end    

        -- cannot create view directly in proc
        exec @retcode= sys.sp_MScreate_sysmergeextendedarticlesview
        if @@error<>0 or @retcode<>0 goto error

        exec dbo.sp_MS_marksystemobject sysmergeextendedarticlesview

        -- vertical partitioning requires a view based sync obj; SQL7.0 used zero as sync_objid
        -- when a non-partitioned article was created; later versions must use the explicit base table
        -- as sync_objid; fixup article sync_objid's prior to remaking the article procs (Shiloh)
        update dbo.sysmergearticles set sync_objid = objid where sync_objid = 0

        -- Do not regenerate views, procs if this is called from sp_restoredbreplication. Restore only
        -- needs to update schema, then it can call existing system procs to remove db replication cleanly
        if @remove_repl = 0
        begin
            -- when upgrading to yukon we have a huge amount of metadata upgrade to do. We do not want to 
            -- do this as part of the upgrade or restore process. We want the subsequent merge or snapshot to
            -- take care of it. Hence here we will drop all triggers on publisher tables and create triggers
            -- which do not allow the DML to happen. When the snapshot or merge has been run valid triggers will
            -- be created and change tracking with work fine after the metadata has been upgraded.
            select @artnick = min(nickname) from dbo.sysmergearticles
            while @artnick is not null
            begin
                select @objid = NULL
                select @source_object = NULL
                select top 1 @objid = objid, @artid = artid from dbo.sysmergearticles where nickname = @artnick
                select @source_owner = schema_name(schema_id), @source_object = name from sys.objects where object_id = @objid
                if @objid is NULL or @source_object is NULL
                    goto error
                    
                exec @retcode = sys.sp_MSdroparticletriggers @source_object, @source_owner
                if @retcode<>0 or @@error<>0
                    goto error
            
                -- generate the disable dml trigger
                exec sys.sp_MScreatedisabledmltrigger @source_object, @source_owner
                if @retcode<>0 or @@error<>0
                    goto error

                -- set the article status to inactive so that the subsequent snapshot prepares the article
                update dbo.sysmergearticles set status = 1 where artid = @artid and status = 2
                update dbo.sysmergearticles set status = 5 where artid = @artid and status = 6

                -- find next article
                select @artnick = min(nickname) from dbo.sysmergearticles where nickname > @artnick
            end -- end article while
        end -- end @remove_repl

        -- MSmerge_errorlineage (Shiloh)
        if object_id('MSmerge_errorlineage') is NULL
        begin
            create table dbo.MSmerge_errorlineage (
            tablenick          int NOT NULL,
            rowguid            uniqueidentifier NOT NULL,
            lineage            varbinary(311) null
            )
            exec dbo.sp_MS_marksystemobject MSmerge_errorlineage
            if @@ERROR <> 0
                goto error
                
            create unique clustered index uc1errorlineage on MSmerge_errorlineage(tablenick, rowguid)
            if @@ERROR <> 0
                goto error
        end
        else
        begin
        if exists (select * from sys.columns where object_id = object_id('dbo.MSmerge_errorlineage') and name = N'lineage' and is_nullable = 0)
        	alter table dbo.MSmerge_errorlineage alter column lineage varbinary(311) null
        end

        -- lineage is varbinary(255) in Shiloh and before, varbinary(311) in Yukon and after
        -- in addition, the format has changed from 4 to 6 byte nicknames, and there are new mergenicks
        if 311 > col_length('MSmerge_errorlineage', 'lineage')
        begin
            begin tran
            save tran tran_errlin80to90
                alter table dbo.MSmerge_errorlineage alter column lineage varbinary(311) not null
                if @@error<>0 goto err_errlin80to90
                update dbo.MSmerge_errorlineage set lineage= {fn LINEAGE_80_TO_90(lineage)}
                if @@error<>0 goto err_errlin80to90
            commit tran
            goto after_errlin80to90

            err_errlin80to90:
            rollback tran tran_errlin80to90
            commit tran
            goto error
        end

        after_errlin80to90:
            
        -- MSmerge_altsyncpartners (Shiloh)
        if object_id('MSmerge_altsyncpartners') is NULL
        begin
            create table dbo.MSmerge_altsyncpartners (
                subid           uniqueidentifier    not null,
                alternate_subid uniqueidentifier    not null,
                description     nvarchar(255)       NULL
            )
            if @@ERROR <> 0
                goto error

            exec dbo.sp_MS_marksystemobject MSmerge_altsyncpartners
            if @@ERROR <> 0
                goto error

            create unique clustered index uciMSmerge_altsyncpartners on 
                dbo.MSmerge_altsyncpartners(subid, alternate_subid)            
            if @@ERROR <> 0
                goto error
        end

        -- new tables added for the first time after SQL2000.
        if object_id('MSmerge_partition_groups') is NULL
        begin
            create table dbo.MSmerge_partition_groups (partition_id int identity not null primary key clustered, 
            					publication_number smallint not null, maxgen_whenadded bigint null, 
            					using_partition_groups bit null default 0, is_partition_active bit default 1 not null)
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_partition_groups
            if @@ERROR <> 0 goto error

            create nonclustered index nc1MSmerge_partition_groups on dbo.MSmerge_partition_groups (publication_number)
            if @@ERROR <> 0 goto error

            grant select on dbo.MSmerge_partition_groups to public    
        end
        else
        begin
            -- Column is_partition_active was added after IDW15 for SQL 2005
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_partition_groups') and
                        name = 'is_partition_active')
            begin
                alter table MSmerge_partition_groups add is_partition_active bit default 1 not null
                if @@error <> 0 goto error
            end            
            
	        if exists (select * from sys.columns where object_id = object_id('dbo.MSmerge_partition_groups') and name = N'using_partition_groups' and is_nullable = 0)
	        	alter table dbo.MSmerge_partition_groups alter column using_partition_groups bit null
        end

        if object_id('MSmerge_generation_partition_mappings') is NULL
        begin
            create table dbo.MSmerge_generation_partition_mappings 
                (
                publication_number smallint not null, 
                generation bigint not null, 
                partition_id int not null,
                changecount int NOT NULL default 0
                )
            if @@ERROR <> 0 goto error
            
            exec dbo.sp_MS_marksystemobject MSmerge_generation_partition_mappings
            if @@ERROR <> 0 goto error

            create clustered index cMSmerge_generation_partition_mappings on dbo.MSmerge_generation_partition_mappings (partition_id, publication_number)
            if @@ERROR <> 0 goto error

            create nonclustered index nc1MSmerge_generation_partition_mappings on dbo.MSmerge_generation_partition_mappings (generation) include (changecount)
            if @@ERROR <> 0 goto error
        end

        if object_id('MSmerge_current_partition_mappings') is NULL
        begin
            create table dbo.MSmerge_current_partition_mappings (publication_number smallint not null, tablenick int not null, rowguid uniqueidentifier not null, partition_id int not null)
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_current_partition_mappings
            if @@ERROR <> 0 goto error

            create clustered index cMSmerge_current_partition_mappings on dbo.MSmerge_current_partition_mappings (tablenick, rowguid)
            if @@ERROR <> 0
                goto error

            create nonclustered index ncMSmerge_current_partition_mappings on dbo.MSmerge_current_partition_mappings (publication_number, partition_id)
            if @@ERROR <> 0
                goto error
            
        end

        if object_id('MSmerge_past_partition_mappings') is NULL
        begin
            create table dbo.MSmerge_past_partition_mappings (publication_number smallint not null, tablenick int not null, rowguid uniqueidentifier not null, partition_id int not null, generation bigint null, reason tinyint not null default(0))
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_past_partition_mappings
            if @@ERROR <> 0 goto error

            create clustered index cMSmerge_past_partition_mappings on dbo.MSmerge_past_partition_mappings (tablenick, rowguid)
            if @@ERROR <> 0
                goto error

            create nonclustered index nc1MSmerge_past_partition_mappings on dbo.MSmerge_past_partition_mappings (publication_number, partition_id)
            if @@ERROR <> 0
                goto error
                
            create nonclustered index nc2MSmerge_past_partition_mappings on dbo.MSmerge_past_partition_mappings (generation)
            if @@ERROR <> 0
                goto error
        end
        
        if object_id('MSmerge_dynamic_snapshots') is NULL
        begin
            create table dbo.MSmerge_dynamic_snapshots (
                    partition_id int not null primary key clustered foreign key references dbo.MSmerge_partition_groups(partition_id) on delete cascade, 
                    dynamic_snapshot_location nvarchar(255) null, 
                    last_updated datetime null,
                    last_started datetime null)
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_dynamic_snapshots            
            if @@ERROR <> 0 goto error
        end
        else
        begin
			if not exists (select * from sys.columns where object_id = object_id('MSmerge_dynamic_snapshots') and
                        name = 'last_started')
            begin
                alter table MSmerge_dynamic_snapshots add last_started datetime NULL
                if @@error <> 0 goto error
            end
        end

        -- Added in Yukon
        if object_id('MSmerge_supportability_settings') is NULL
        begin
            create table dbo.MSmerge_supportability_settings (
                    pubid                uniqueidentifier    NULL,
                    subid                uniqueidentifier    NULL,
                    web_server           sysname             NULL,                
                    constraint           unique_supportpubsrvdb     unique nonclustered (pubid, subid, web_server),
                    support_options      int NOT NULL default(0),    -- check the SUPPORT_OPTIONS enum in agent code.
                    log_severity         int NOT NULL default(2),
                    log_modules          int NOT NULL default(0),
                    log_file_path        nvarchar(255) NULL,
                    log_file_name        sysname NULL,
                    log_file_size        int NOT NULL default(10000000),
                    no_of_log_files      int NOT NULL default(5),
                    upload_interval      int NOT NULL default(0),
                    delete_after_upload  int NOT NULL default(0),                    
                    custom_script        nvarchar(2048) NULL,
                    message_pattern      nvarchar(2000) NULL,
                    last_log_upload_time datetime            NULL,
                    agent_xe               varbinary(max) NULL,
                    agent_xe_ring_buffer  varbinary(max) NULL,
                    sql_xe                   varbinary(max) NULL
                    )
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_supportability_settings            
        end
        else
        begin
            -- Column agent_xe  was added for SQL 11
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_supportability_settings') and
                        name = 'agent_xe')
            begin
                alter table MSmerge_supportability_settings add agent_xe varbinary(max)
                if @@ERROR <> 0 goto error
            end            
            
            -- Column agent_xe_ring_buffer  was added for SQL 11
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_supportability_settings') and
                        name = 'agent_xe_ring_buffer')
            begin
                alter table MSmerge_supportability_settings add agent_xe_ring_buffer varbinary(max)
                if @@ERROR <> 0 goto error
            end           

            -- Column sql_xe  was added for SQL 11
            if not exists (select * from sys.columns where object_id = object_id('MSmerge_supportability_settings') and
                        name = 'sql_xe')
            begin
                alter table MSmerge_supportability_settings add sql_xe varbinary(max)
                if @@ERROR <> 0 goto error
            end            

        end
        
        -- Added in Yukon
        if object_id('MSmerge_log_files') is NULL
        begin
            create table dbo.MSmerge_log_files (
                    id                   int identity(1,1),
                    pubid                uniqueidentifier    NULL,
                    subid                uniqueidentifier    NULL,
                    web_server           sysname             NULL,
                    file_name            nvarchar(2000)      NOT NULL,
                    upload_time          datetime              NOT NULL default getdate(),
                    log_file_type        int                 NOT NULL, -- Check UPLOAD_LOG_FILE_TYPE enum in agent code.
                    log_file             varbinary(max)      NULL
                    )
            if @@ERROR <> 0 goto error

            create clustered index ucMSmerge_log_files on MSmerge_log_files(pubid, subid, id) 
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_log_files            
        end

        -- Added in Yukon
        if object_id('dbo.MSmerge_metadataaction_request', 'U') is null
        begin
            create table dbo.MSmerge_metadataaction_request
            (
                tablenick int not null,
                rowguid uniqueidentifier not null,
                action tinyint not null,
                generation bigint null, -- for hws cleanup
                changed int null -- for lws cleanup
            )
            if @@ERROR <> 0 goto error

            create clustered index ucMSmerge_metadataaction_request on MSmerge_metadataaction_request(tablenick, rowguid) 
            if @@ERROR <> 0 goto error

            exec dbo.sp_MS_marksystemobject MSmerge_metadataaction_request
            if @@ERROR <> 0 goto error
        end

        -- Added in Yukon
        if object_id('dbo.MSmerge_agent_parameters', 'U') is null
        begin
            --raiserror('Creating table MSmerge_agent_parameters',0,1)
            
            create table dbo.MSmerge_agent_parameters
            (
            profile_name         sysname        NOT NULL,
            parameter_name       sysname        NOT NULL,
            value                nvarchar(255)  NOT NULL
            )
        
            if @@ERROR <> 0
                goto error

            exec dbo.sp_MS_marksystemobject MSmerge_agent_parameters
            if @@ERROR <> 0
                goto error        
        end

        -- we will now set the snapshot_ready status of all local publications. We invalidate the snapshot
        -- so that the metadata upgrade can be run at snapshot time.
        update dbo.sysmergepublications set snapshot_ready=2 
            where UPPER(publisher) collate database_default = UPPER(publishingservername()) collate database_default and publisher_db = db_name()
        
        -- revoke select access to public on table which were previously granted to public
        if object_id('dbo.sysmergepublications') is not NULL
            revoke select on dbo.sysmergepublications from public
            
        if object_id('dbo.MSmerge_errorlineage') is not NULL
            revoke select on dbo.MSmerge_errorlineage from public
            
        if object_id('dbo.sysmergearticles') is not NULL
        begin
            revoke select on dbo.sysmergearticles from public
            grant select(nickname, maxversion_at_cleanup, objid) on dbo.sysmergearticles to public
        end
        
        if object_id('dbo.sysmergesubscriptions') is not NULL
            revoke select on dbo.sysmergesubscriptions from public
            
        if object_id('dbo.MSmerge_replinfo') is not NULL
            revoke select on dbo.MSmerge_replinfo from public

        if object_id('dbo.MSmerge_tombstone') is not NULL
            revoke select on dbo.MSmerge_tombstone from public
            
        if object_id('dbo.MSmerge_contents') is not NULL
            revoke select on dbo.MSmerge_contents from public
        
        if object_id('dbo.MSmerge_genhistory') is not NULL
            revoke select on dbo.MSmerge_genhistory from public
        
        if object_id('dbo.sysmergeschemachange') is not NULL
            revoke select on dbo.sysmergeschemachange from public
        
        if object_id('dbo.sysmergesubsetfilters') is not NULL
            revoke select on dbo.sysmergesubsetfilters from public

    end -- end dbo.sysmergearticles modifications

    if object_id('sysmergearticles') is not NULL
    begin
        -- always drop down level triggers since in yukon the triggers are named differently
        if exists (select * from dbo.sysmergearticles)
        begin
            declare @artidstr sysname
            declare @instrigger nvarchar(517)
            declare @updtrigger nvarchar(517)
            declare @deltrigger nvarchar(517)
            
            select @artnick = min(nickname) from dbo.sysmergearticles
            while @artnick is not null
            begin
                select @objid = NULL
                select @source_object = NULL
                select top 1 @objid = objid, @artid = artid from dbo.sysmergearticles where nickname = @artnick
                select @source_owner = schema_name(schema_id), @source_object = name from sys.objects where object_id = @objid
                if @objid is NULL or @source_object is NULL
                    goto error

                exec @retcode=sys.sp_MSguidtostr @artid, @artidstr out
                if @retcode<>0 or @@ERROR<>0 
                    goto error
                    
                -- the following are downlevel trigger names
                select @instrigger = QUOTENAME(@source_owner) + '.ins_' + @artidstr
                select @updtrigger = QUOTENAME(@source_owner) + '.upd_' + @artidstr
                select @deltrigger = QUOTENAME(@source_owner) + '.del_' + @artidstr
                if object_id(@instrigger) is not NULL
                begin
                    exec ('drop trigger ' + @instrigger)
                    if @@ERROR<>0 return (1)
                end
                if object_id(@updtrigger) is not NULL
                begin
                    exec ('drop trigger ' + @updtrigger)
                    if @@ERROR<>0 return (1)
                end
                if object_id(@deltrigger) IS NOT NULL
                begin
                    exec ('drop trigger ' + @deltrigger)
                    if @@ERROR<>0 return (1)
                end
                
                -- find next article
                select @artnick = min(nickname) from dbo.sysmergearticles where nickname > @artnick
            end -- end article while
        end
    end
    
    

    
    /* Merge dynamic snapshot */

    /* Make sure that the database is enabled for merge replication before MSdynamicsnapshotviews is created */
    if @remove_repl = 0 and object_id('sysmergepublications') is not NULL
    begin

        /*
        ** MSdynamicsnapshotviews -- Created from Shiloh Beta2 onwards
        */

        if object_id('MSdynamicsnapshotviews') is NULL
        begin
            create table dbo.MSdynamicsnapshotviews (
                dynamic_snapshot_view_name sysname primary key,
            )
            if @@ERROR <> 0 goto error
        end

        exec dbo.sp_MS_marksystemobject MSdynamicsnapshotviews

        /* 
        ** MSdynamicsnapshotjobs -- Created from Shiloh Beta2 onwards
        */
        if object_id('MSdynamicsnapshotjobs') is NULL
        begin
            create table dbo.MSdynamicsnapshotjobs (
                id int identity,
                name sysname not null unique,
                pubid uniqueidentifier not null,
                job_id uniqueidentifier not null,
                agent_id int not null default 0,
                dynamic_filter_login sysname null,
                dynamic_filter_hostname sysname null,
                dynamic_snapshot_location nvarchar(255) not null,
                partition_id int not NULL default -1,
                computed_dynsnap_location bit not NULL default 0
            )
            if @@ERROR <> 0 goto error
        end 

        -- Update MSdynamicsnapshotjobs so that it has:
        --		agent_id default 1
        --    	partition_id default -1
        -- We are know herer that dbo.MSdynamicsnapshotjobs is not null so no need to check.
        
        -- Adding DEFAULT on column agent_id
	 if exists(select * from sys.columns where object_id = object_id('dbo.MSdynamicsnapshotjobs') 
	 	and name = 'agent_id')
	 begin
	        select @defaultname = null
	        select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
						(sysdc.parent_object_id = sysc.object_id and 
							sysdc.parent_column_id = sysc.column_id)		
					where sysc.object_id = object_id('dbo.MSdynamicsnapshotjobs') and
						sysc.name = 'agent_id'
		 if @defaultname is not null -- check if default exists
		 begin
		 	-- delete default if one exists
		 	select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs drop constraint ' + QUOTENAME(@defaultname)
		 	exec (@alter_cmd)
		 end
		 -- add the default we want
		 select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs add default 0 for agent_id'
		 exec (@alter_cmd)
	 end
	 if exists(select * from sys.columns where object_id = object_id('dbo.MSdynamicsnapshotjobs') 
	 	and name = 'partition_id')
	 begin
		 --Adding DEFAULT on column partition_id
		 select @defaultname = null
		 select top 1 @defaultname = sysdc.name from sys.default_constraints sysdc join sys.columns sysc on
						(sysdc.parent_object_id = sysc.object_id and 
							sysdc.parent_column_id = sysc.column_id)		
					where sysc.object_id = object_id('dbo.MSdynamicsnapshotjobs') and
						sysc.name = 'partition_id'
		 if @defaultname is not null -- check if default exists
		 begin
		 	-- delete default if one exists
		 	select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs drop constraint ' + QUOTENAME(@defaultname)
		 	exec (@alter_cmd)
		 end
		 -- add the default we want
		 select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs add default -1 for partition_id'
		 exec (@alter_cmd)
	 end

        declare colcurs cursor LOCAL FAST_FORWARD 
        for (select col_name, col_type from (select col_name = 'partition_id', col_type = 'int not NULL default -1'
                                                             union all
                                                             select col_name = 'agent_id', col_type = 'int not NULL default 0'
                                                             union all
                                                             select col_name = 'computed_dynsnap_location', col_type = 'bit not NULL default 0'
                                                             ) as t1                                                             
               left outer join
               sys.columns as t2
               on (t1.col_name = t2.name and t2.object_id = object_id('dbo.MSdynamicsnapshotjobs', 'U'))
               where t2.name is null) --This query gives all the columns in t1 that are not in syscolums
        for read only
        open colcurs
        fetch colcurs into @column_name, @column_type
        if (@@fetch_status <> -1)
        begin
            select @alter_cmd = 'alter table dbo.MSdynamicsnapshotjobs add ' + @column_name + ' ' + @column_type
            fetch colcurs into @column_name, @column_type
            while(@@fetch_status <> -1)
            begin
                select @alter_cmd = @alter_cmd + ', ' + @column_name + ' ' + @column_type
                fetch colcurs into @column_name, @column_type
            end
            exec (@alter_cmd)
            if @@error <> 0 goto error
        end
        close colcurs
        deallocate colcurs

        if not exists (select * 
                         from sys.indexes 
                        where object_id = object_id('MSdynamicsnapshotjobs') 
                          and name = ('uciMSdynamicsnapshotjobs'))
        begin
            create unique clustered index uciMSdynamicsnapshotjobs on 
                dbo.MSdynamicsnapshotjobs(job_id, pubid)
            if @@ERROR <> 0 goto error
        end
        
        if not exists (select * 
                         from sys.indexes 
                        where object_id = object_id('MSdynamicsnapshotjobs') 
                          and name = ('nciMSdynamicsnapshotjobs'))
        begin
            create nonclustered index nciMSdynamicsnapshotjobs on 
                dbo.MSdynamicsnapshotjobs(partition_id)
            if @@ERROR <> 0 goto error
        end

        exec dbo.sp_MS_marksystemobject MSdynamicsnapshotjobs
        if @@ERROR <> 0 goto error
    end
	
    -- Index updates (SQL7.0 SP1)
    if @remove_repl = 0
    begin
        SELECT @table_name = N'sysmergepublications'
        IF object_id('sysmergepublications') is not NULL
        BEGIN 
            IF EXISTS ( SELECT pubid
                FROM dbo.sysmergepublications
                GROUP BY pubid
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 4, @table_name)
                goto error
            end
            ELSE
                IF NOT EXISTS ( SELECT * FROM sys.indexes WHERE name = 'nc1sysmergepublications' AND
                    object_id = object_id('dbo.sysmergepublications') )
                    CREATE UNIQUE NONCLUSTERED INDEX nc1sysmergepublications
                        ON dbo.sysmergepublications(pubid)
                 if @@ERROR <> 0 goto error

				 IF NOT EXISTS ( SELECT * FROM sys.indexes WHERE name = 'nc2sysmergepublications' AND
                    object_id = object_id('dbo.sysmergepublications') )
					CREATE NONCLUSTERED INDEX nc2sysmergepublications 
					ON sysmergepublications(status)
                 if @@ERROR <> 0 goto error


				 -- add default constraint on allow_anonymous column 
				if not exists(
				select * 
				from sysconstraints as con join sys.columns as col 
					on con.colid = col.column_id
						and con.id = col.object_id
						and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
						and col.object_id = object_id('dbo.sysmergepublications')
						and col.name = 'allow_anonymous')and exists 
				(select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'allow_anonymous')
				begin
					exec(N'alter table dbo.sysmergepublications add default 1 for allow_anonymous')
				end


				-- Changing default constraint on publisher from @@servername to publishingservername() 
				if exists(select * from sys.columns where object_id = object_id('dbo.sysmergepublications') and name = 'publisher')
				begin
					declare @default_publisher_c_name nvarchar(258)
					select @default_publisher_c_name = obj.name 
					from sysconstraints as con join sys.columns as col 
						on con.colid = col.column_id 
							and con.id = col.object_id
							and OBJECTPROPERTY ( con.constid , 'IsDefaultCnst' ) = 1 
							and col.object_id = object_id('dbo.sysmergepublications')
							and col.name = 'publisher'
							join sys.objects as obj
							on obj.object_id=con.constid

					if(@default_publisher_c_name is not null)
					begin
						select @default_publisher_c_name = quotename(@default_publisher_c_name)
						exec (N'alter table dbo.sysmergepublications  drop constraint ' + @default_publisher_c_name)
						exec(N'alter table dbo.sysmergepublications add default publishingservername()  for publisher') 					
					end
				end

        END 


        --  MSmerge_conflicts_info
        SELECT @table_name = N'MSmerge_conflicts_info'
        IF object_id('MSmerge_conflicts_info') is not NULL
        BEGIN
            IF EXISTS ( SELECT tablenick, rowguid, origin_datasource, conflict_type
                FROM MSmerge_conflicts_info
                GROUP BY tablenick, rowguid, origin_datasource, conflict_type
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 6, @table_name)
                goto error
            end
            ELSE
                IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc1MSmerge_conflicts_info' AND
                    id = OBJECT_ID('MSmerge_conflicts_info') )
                    CREATE UNIQUE NONCLUSTERED INDEX nc1MSmerge_conflicts_info 
                        ON MSmerge_conflicts_info(tablenick, rowguid, origin_datasource, conflict_type)
            if @@ERROR <> 0 goto error
        END

        --  sysmergeschemachange
        SELECT @table_name = N'sysmergeschemachange'
        IF object_id('sysmergeschemachange') is not NULL
        BEGIN
            IF EXISTS ( SELECT schemaversion, pubid
                FROM dbo.sysmergeschemachange
                GROUP BY schemaversion, pubid
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 7, @table_name)
                goto error
            end
            ELSE
            BEGIN
                IF EXISTS ( SELECT * FROM sys.indexes WHERE name = 'schemachangeversion' AND
                    object_id = OBJECT_ID('sysmergeschemachange') )            
                    DROP INDEX sysmergeschemachange.schemachangeversion
                if @@ERROR <> 0 goto error
            
                -- Recreate this index as unique clustered with one more field in index key.
                CREATE UNIQUE CLUSTERED INDEX schemachangeversion ON sysmergeschemachange(schemaversion, pubid) 
                if @@ERROR <> 0 goto error
            END

            -- In Yukon we no longer have a schema version SCHEMA_TYPE_SYSTABLE which
            -- indicates a system table schema script file. This type was deprecated because
            -- it was used only by JET consumers. In Yukon Jet subscribers are not supported
            -- Hence deleting any entries which have schema type SCHEMA_TYPE_SYSTABLE (20)
            delete from dbo.sysmergeschemachange where schematype=20        
            
            -- Adding schemastatus column. 
            if not exists (select * from sys.columns where object_id = object_id('dbo.sysmergeschemachange') and
                            name = 'schemastatus')
            begin
                alter table dbo.sysmergeschemachange add schemastatus        tinyint        NOT NULL default(1)
                if @@error <> 0 goto error
            end

            if not exists (select * from sys.columns where object_id = object_id('sysmergeschemachange') and
                        name = 'schemasubtype')
            begin
                alter table sysmergeschemachange add schemasubtype int NOT NULL default 0
                if @@error <> 0 goto error
            end

            -- Modifying the type of the schematext from nvarchar(2000) to nvarchar(max)
            if exists (select * from sys.columns where object_id = object_id('dbo.sysmergeschemachange') and
                            name = 'schematext')
            begin
                exec (' alter table dbo.sysmergeschemachange alter column schematext nvarchar(max) NOT NULL ')
                if @@error <> 0 goto error
            end
            
         END


        --  sysmergesubsetfilters    
        SELECT @table_name = N'sysmergesubsetfilters'
        IF object_id('sysmergesubsetfilters') is not NULL
        BEGIN
            IF EXISTS ( SELECT join_filterid
                FROM dbo.sysmergesubsetfilters
                GROUP BY join_filterid
                HAVING COUNT(*) > 1 )
            begin
                RAISERROR (21203, 10, 8, @table_name)
                goto error
            end
            ELSE
                IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'nc1sysmergesubsetfilters' AND
                    id = OBJECT_ID('sysmergesubsetfilters') )
                begin
                    CREATE UNIQUE NONCLUSTERED INDEX nc1sysmergesubsetfilters ON dbo.sysmergesubsetfilters(join_filterid, pubid)
                    if @@ERROR <> 0 goto error
                end
                
            IF NOT EXISTS ( SELECT * FROM sysindexes WHERE name = 'uc2sysmergesubsetfilters' AND
                id = OBJECT_ID('sysmergesubsetfilters') )
            begin
                CREATE UNIQUE CLUSTERED INDEX uc2sysmergesubsetfilters ON dbo.sysmergesubsetfilters(pubid, filtername)
                if @@ERROR <> 0 goto error
            end
                
            if not exists (select * from sys.columns where object_id = object_id('sysmergesubsetfilters') and
                            name = 'filter_type')
            begin
                alter table dbo.sysmergesubsetfilters add filter_type tinyint NOT NULL default 1
                if @@ERROR <> 0 goto error
            end
        END -- end index updates from SQL7.0 SP1


        declare @binames table (biname sysname)
        insert into @binames select name from sys.objects where type='U' and is_ms_shipped=1 and name like 'MS_bi%'
        declare @biname sysname
        set @biname= (select top 1 biname from @binames)
        while @biname is not null
        begin
            set @cmd= 'drop index ' + quotename(@biname) + '.' + quotename(@biname + '_gen')
            exec sys.sp_executesql @cmd
            if @@ERROR <> 0 goto error
            set @cmd= 'create clustered index ' + quotename(@biname + '_gen') + ' on ' + quotename(@biname) + '(generation)'
            exec sys.sp_executesql @cmd
            if @@ERROR <> 0 goto error
            delete from @binames where biname=@biname
            set @biname= (select top 1 biname from @binames)
        end


        if object_id('sysmergearticles') is not NULL
        begin
            exec sys.sp_MScreate_common_dist_tables @subside=1
            if @@error <> 0
               goto error
        end
        
        -- in Yukon we will create the merge ddl triggers, only for databases that have merge replication enabled.
        if object_id('sysmergepublications', 'U') is NOT NULL
        begin
            if exists (select * from sys.triggers where name = 'MSmerge_tr_altertable' and type = 'TR')
            begin
                execute @retcode= sys.sp_MSrepl_ddl_triggers @type='merge', @mode='drop'
                if @@ERROR <> 0 or @retcode <> 0 
                    goto error
            end
            if not exists (select * from sys.triggers where name = 'MSmerge_tr_altertable' and type = 'TR')
            begin
                execute @retcode= sys.sp_MSrepl_ddl_triggers @type='merge', @mode='add'
                if @@ERROR <> 0 or @retcode <> 0 
                    goto error
            end
        end
        -- we will also add a system table MSmerge_upgrade_in_progress which would indicate that
        -- the metadata upgrade has not been completed yet.
        if @remove_repl=0 and object_id('sysmergearticles') is not NULL
        begin
            if object_id('MSmerge_upgrade_in_progress', 'U') is NULL
            begin
                create table dbo.MSmerge_upgrade_in_progress
                (
                    status tinyint not NULL
                )
                if @@ERROR <> 0 goto error
            end
        end
    end
    
    -- Call the v2 proc when we change the implementation of that proc.  Right now it is empty and hence the following code is commented out.
    --Declare @dbname sysname = DB_NAME()
    --EXEC @retcode = master.sys.sp_vupgrade_mergetables_v2 @remove_repl = @remove_repl, @dbname = @dbname
    --if @@ERROR <> 0 or @retcode <> 0
    --    goto error

    
    commit tran
    return 0
    
error:
    rollback tran vupgrade_mergetables
    commit tran
    return 1
    
end


