SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_addmergearticle
    @publication            sysname,                            /* publication name */
    @article                sysname,                            /* article name */
    @source_object          sysname,                            /* source object name */
    @type                   sysname = 'table',                  /* article type */
    @description            nvarchar(255)= NULL,                /* article description */
    @column_tracking        nvarchar(10) = 'false',             /* column level tracking */
    @status                 nvarchar(10) = 'unsynced',          /* unsynced, active */
    @pre_creation_cmd       nvarchar(10) = 'drop',              /* 'none', 'drop', 'delete', 'truncate' */
    @creation_script        nvarchar(255)= NULL,                /* article schema script */
    @schema_option          varbinary(8)   = NULL,              /* article schema creation options */
    @subset_filterclause    nvarchar(1000) = '',                /* filter clause */
    @article_resolver       nvarchar(255)= NULL,                /* custom resolver for article */
    @resolver_info          nvarchar(517) = NULL,               /* custom resolver info */
    @source_owner           sysname = NULL,
    @destination_owner      sysname = NULL,
    @vertical_partition     nvarchar(5) = 'FALSE',             /* vertical partitioning or not */
    @auto_identity_range    nvarchar(5) = NULL,                /* this parameter is deprecated. use @identityrangemanagementoption */
    @pub_identity_range     bigint    = NULL,
    @identity_range         bigint = NULL,
    @threshold              int = NULL,
    @verify_resolver_signature     int = 1,                    /* 0=do not verify signature, 1=verify that signature is from trusted source, more values may be added later */
    @destination_object            sysname = @source_object,
    @allow_interactive_resolver    nvarchar(5) = 'false',        /* whether article allows interactive resolution or not */
    @fast_multicol_updateproc      nvarchar(5) = 'true',        /* whether update proc should update multiple columns in one update statement or not. if 0, then separate update issued for each column changed. */
    @check_permissions         int = 0, /* bitmap where 0x00 for nochecks, 0x01 for insert check, 0x2 for update check, 0x4 for delete check */
    @force_invalidate_snapshot bit = 0, /* Force invalidate existing snapshot */
    @published_in_tran_pub     nvarchar(5) = 'false', /* Indicates that this article could be published in a transactional publication as well */
    @force_reinit_subscription bit = 0, /* Force reinit subscription */
    @logical_record_level_conflict_detection nvarchar(5) = 'false',
    @logical_record_level_conflict_resolution nvarchar(5) = 'false',
    @partition_options tinyint = 0, -- 0, 1, 2 or 3 meaning none, no out of partition dml, partition based and subscription based
    @processing_order int = 0,
    @subscriber_upload_options tinyint = 0, -- possible values are 0, 1, and 2 meaning 'allow uploads', 'disables uplods', 'disable uploads and prohibit subscriber changes'
    @identityrangemanagementoption nvarchar(10) = NULL, -- NONE, MANUAL, AUTO
    @delete_tracking    nvarchar(5) = 'true',    --'true' = replicate deletes as usual.  false'= do not track deletes in triggers..
    @compensate_for_errors    nvarchar(5) = 'false',
    @stream_blob_columns nvarchar(5) = 'false'    -- 'true'= use blob optimization, 'false' = disable blob optimization.
    AS

    set nocount on

    declare @max_range              numeric(38,0)
    declare @min_range              numeric(38,0)
    declare @max_used               numeric(38,0)
    declare @ident_incr             numeric(38,0)
    declare @publisher              sysname
    declare @publisher_db           sysname
    declare @already_published      bit
    declare @identity_so_far        bigint
    declare @ver_partition          int
    declare @sp_resolver            sysname
    declare @num_columns            smallint
    declare @pubid                  uniqueidentifier                /* Publication id */
    declare @db                     sysname
    declare @identity_support       int
    declare @object                 sysname
    declare @owner                  sysname
    declare @retcode                int
    declare @objid                  int
    declare @sync_objid             int
    declare @index_objid            int
    declare @typeid                 smallint
    declare @replnick               binary(6)
    declare @merge_pub_object_bit   int
    declare @column_tracking_id     int
    declare @cmd                    nvarchar(2000)
    declare @statusid               tinyint --1: inactive; 2: active; 5:new_inactive 6:new_active
    declare @next_seed              bigint
    declare @precmdid               int
    declare @resolver_clsid         nvarchar(50)
    declare @resolver_clsid_old     nvarchar(50)
    declare @tablenick              int
    declare @artid                  uniqueidentifier
    declare @i                      int
    declare @max_identity           bigint
    declare @colname                sysname
    declare @idxname                sysname
    declare @indid                  int
    declare @pkkey                  sysname
    declare @dbname                 sysname
    declare @db_name                sysname
    declare @subset                 int
    declare @is_publisher           int
    declare @row_size               int
    declare @sp_name                sysname
    declare @sp_owner               sysname
    declare @qualified_name         nvarchar(517)
    declare @snapshot_ready         tinyint
    declare @sync_mode              tinyint
    declare @allow_interactive_bit  bit
    declare @fast_multicol_updateproc_bit bit
    declare @additive_resolver       sysname
    declare @average_resolver        sysname
    declare @mindate_resolver        sysname
    declare @needs_pickup            bit
    declare @maxdate_resolver        sysname
    declare @minimum_resolver        sysname
    declare @maximum_resolver        sysname
    declare @mergetxt_resolver       sysname
    declare @pricolumn_resolver      sysname
    declare @xtype                   int
    declare @xprec                   int
    declare @bump_to_80              bit
    declare @gen                     bigint
    declare @genguid                 uniqueidentifier
    declare @dt                      datetime
    declare @qualname                nvarchar(517)
    declare @compatlevel             int
    declare @allow_partition_realignment bit
    declare @logical_record_level_conflict_detection_id bit
    declare @logical_record_level_conflict_resolution_id bit
    declare @published_in_tran_pub_bit bit
    declare @allow_anonymous bit
    declare @compensateforerrors bit
    declare @deletetracking bit
    declare @reinit_subscriptions bit
    declare @article_has_dynamic_filters bit
    declare @publication_has_dynamic_filters bit
    declare @REPOLEVersion_80SP3 int
    declare @stream_blob_columns_bit bit
    declare @missing_col_count       int
    declare @missing_cols            varbinary(128)
    declare @pubname_lessthan90compat sysname
    declare @preserve_rowguidcol bit
    declare @automatic_reinitialization_policy bit
    declare @use_partition_groups smallint
    declare @pub_number smallint
    declare @functions_in_subset_filter nvarchar(500)
    declare @dynamic_filters_function_list nvarchar(500)
    declare @got_merge_admin_applock bit
            	,@obj_name sysname
    declare @filestream_col_published int
    declare @has_filestream int
    declare @fFileTable bit

    select @got_merge_admin_applock = 0
    select @filestream_col_published = 0
    select @has_filestream = 0

    -- Security Check
    exec @retcode= sys.sp_MSreplcheck_publish
    if @@error <> 0 or @retcode <> 0 return (1)

    /* make sure current database is enabled for merge replication */
    exec @retcode=sys.sp_MSCheckmergereplication
    if @@ERROR<>0 or @retcode<>0
        return (1)

    /*
    ** Initializations 
    */
    set @REPOLEVersion_80SP3= 60
    select @is_publisher = 0
    select @needs_pickup = 0
    select @bump_to_80 = 0
    select @already_published = 0
    select @publisher = publishingservername()
    select @publisher_db = db_name()
    select @max_identity    = NULL
    select @next_seed        = NULL
    select @statusid        = 0
    select @resolver_clsid    = NULL
    select @subset            = 1        /* Const: publication type 'subset' */
    select @merge_pub_object_bit    = 128
    select @db_name = db_name()
    select @additive_resolver   = formatmessage(21701)
    select @average_resolver    = formatmessage(21702)
    select @mindate_resolver    = formatmessage(21703)
    select @maxdate_resolver    = formatmessage(21704)
    select @minimum_resolver    = formatmessage(21706)
    select @mergetxt_resolver   = formatmessage(21707)
    select @maximum_resolver    = formatmessage(21708)
    select @pricolumn_resolver  = formatmessage(21709)
    select @sp_resolver         = formatmessage(21712)
    select @reinit_subscriptions = 0
    select @article_has_dynamic_filters = 0
    select @publication_has_dynamic_filters = 0
    select @pubname_lessthan90compat = NULL

    if @subscriber_upload_options not in (0, 1, 2)
    begin
        raiserror (22542, 16, -1)
        return (1)
    end

    if 'false' = lower(@compensate_for_errors collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @compensateforerrors= 0
    end
    else if 'true' = lower(@compensate_for_errors collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @compensateforerrors= 1
    end
    else
    begin
        raiserror (14148, 16, -1, '@compensate_for_errors')
        return (1)
    end

    if 'false' = lower(@delete_tracking collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @deletetracking= 0
    end
    else if 'true' = lower(@delete_tracking collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @deletetracking= 1
    end
    else
    begin
        raiserror (14148, 16, -1, '@delete_tracking')
        return (1)
    end

    if 'false' = lower(@stream_blob_columns collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @stream_blob_columns_bit= 0
    end
    else if 'true' = lower(@stream_blob_columns collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @stream_blob_columns_bit= 1
    end
    else
    begin
        raiserror (14148, 16, -1, '@stream_blob_columns')
        return (1)
    end

    if @source_owner is NULL
    begin
        select @source_owner = SCHEMA_NAME(schema_id) from sys.objects where object_id = object_id(QUOTENAME(@source_object))
        if @source_owner is NULL  
        begin
            raiserror (14027, 11, -1, @source_object)
            return (1)
        end
    end

    select @qualified_name = QUOTENAME(@source_owner) + '.' + QUOTENAME(@source_object)

    /*
    **    Get the id of the @qualified_name
    */
    select @objid = OBJECT_ID(@qualified_name)
    if @objid is NULL
    begin
        raiserror (14027, 11, -1, @qualified_name)
        return (1)
    end
    
    -- If the article that we are trying to add is a natively compiled stored proc
    -- raise error.
    if (ObjectProperty(@objid, 'ExecIsWithNativeCompilation') <> 0)
    begin
        raiserror (12336, 16, 1, 35473)
        return @@ERROR
    end
    
    -- If the article that we are trying to add is an in-memory table
    -- raise error.
    if (ObjectProperty(@objid, 'TableIsMemoryOptimized') <> 0)
    begin
        raiserror (12336, 16, 1, 35472)
        return @@ERROR
    end

    -- If the article that we are trying to add is a system-versioned temporal table or history table
    -- raise error.
    if exists (select 1 from sys.tables where object_id = @objid and temporal_type in (1, 2))
    begin
        raiserror (13570, 16, 1, @qualified_name)
        return @@ERROR
    end
	
	-- If the article that we are trying to add is a graph node or edge table raise error.
    if exists (select 1 from sys.tables where object_id = @objid and (is_node = 1 or is_edge = 1))
    begin
		raiserror (13926, 16, 2, @source_object)
		return @@ERROR
    end

    -- If the article that we are trying to add is a stretched table
    -- raise error.
    if exists (select 1 from sys.tables where object_id = @objid and is_remote_data_archive_enabled = 1)
    begin
        raiserror (14915, 16, 2, @qualified_name)
        return @@ERROR
    end

    -- If the article that we are trying to add is a ledger table or history table
    -- raise error.
    if exists (select 1 from sys.tables where object_id = @objid and ledger_type != 0)
    begin
        raiserror (37443, 16, 2, @qualified_name)
        return @@ERROR
    end

    -- check if the object is marked as ms shipped. If so it cannot be published
    if exists (select 1 from sys.objects where object_id = @objid and is_ms_shipped=1)
    begin
        raiserror (20696, 16, -1, @qualified_name)
        return (1)
    end

    -- check if the object contains json/vector column. If so it cannot be merge-published.
    if ([sys].[fn_contains_json_type](@objid) = 1)
    begin
        raiserror(13687, 16, -1)
        return @@ERROR
    end
    if ([sys].[fn_contains_vector_type](@objid) = 1)
    begin
        raiserror(42240, 16, -1)
        return @@ERROR
    end
    
    if @destination_owner is NULL
        select @destination_owner='dbo'

    /*
    ** Pad out the specified schema option to the left
    */
    select @schema_option = fn_replprepadbinary8(@schema_option)

    /*
    ** Parameter Check: @publication.
    ** The @publication id cannot be NULL and must conform to the rules
    ** for identifiers.
    */     
        
    if @publication is NULL
    begin
        raiserror (14043, 16, -1, '@publication', 'sp_addmergearticle')
        return (1)
    end

    select @pubid = pubid, 
           @snapshot_ready = snapshot_ready, 
           @sync_mode=sync_mode, 
           @compatlevel=backward_comp_level, 
           @allow_anonymous = allow_anonymous,
           @use_partition_groups = use_partition_groups,
           @pub_number = publication_number,
           @publication_has_dynamic_filters = dynamic_filters,
           @allow_partition_realignment = allow_partition_realignment,
           @automatic_reinitialization_policy = automatic_reinitialization_policy,
           @dynamic_filters_function_list = dynamic_filters_function_list
    from dbo.sysmergepublications 
    where name = @publication and UPPER(publisher) collate database_default = UPPER(@publisher) collate database_default and publisher_db=@publisher_db
    if @pubid is NULL
    begin
        raiserror (20026, 16, -1, @publication)
        return (1)
    end

    if lower(@article)='all'
    begin
        raiserror(21401, 16, -1)
        return (1)
    end

    if  (0=@allow_partition_realignment and 0=@subscriber_upload_options)
    begin
        raiserror(22543, 16, -1)
        return (1)
    end

    -- Compensate for errors can be turned on only when upload options allows subscriber uploads.
    if 1=@compensateforerrors and (1=@subscriber_upload_options or 2=@subscriber_upload_options)
    begin
        raiserror(20022, 10, -1)
    end

    -- Parameter check @subset_filterclause
    if @subset_filterclause <> '' and @subset_filterclause is not NULL
    begin
        /* check the validity of subset_filterclause */
        exec ('declare @test int select @test=1 from ' + @qualified_name + ' where (1=2) and ' + @subset_filterclause)
        if @@ERROR<>0
        begin
            raiserror(21256, 16, -1, @subset_filterclause, @article)
            return (1)
        end

        -- check if the subsetfilter clause contains a computed column. To do this get a list of computed columns
        -- for the given article. Then check if the filter name is like the computed column
        declare @computedcolname sysname
        
        declare compted_columns_cursor cursor LOCAL FAST_FORWARD
        for (select name from sys.columns where object_id = @objid and is_computed=1)
        open compted_columns_cursor
        fetch compted_columns_cursor into @computedcolname
        while (@@fetch_status <> -1)
        begin
            
            if sys.fn_MSisfilteredcolumn(@subset_filterclause, @computedcolname, @objid) = 1 
            begin
                raiserror(20656, 16, -1)
                return (1)
            end
            fetch compted_columns_cursor into @computedcolname
        end
        close compted_columns_cursor
        deallocate compted_columns_cursor


        -- check if the subsetfilter clause contains any column of type that is not supported in
        --  a subset filter.

        if exists    (
                        select * from sys.columns 
                        where object_id = @objid and 
                            (
                            --(sys.fn_IsTypeBlob(sc.system_type_id,sc.max_length) = 1) -- Blob type text,ntext,xml
                              (system_type_id in (type_id('image'), type_id('text'), type_id('ntext'), type_id('xml')))
                              or max_length = -1
                              or system_type_id = 240    -- CLR-UDTs
                            )
                        and 
                        sys.fn_MSisfilteredcolumn(@subset_filterclause, name, @objid) = 1 
                    )
        begin
            raiserror(22518, 16, -1, @qualified_name)
            return (1)
        end

    end
      
    /*
    ** Parameter Check: @type
    ** If the article is added as a 'indexed view schema only' article,
    ** make sure that the source object is a schema-bound view.
    ** Conversely, a schema-bound view cannot be published as a 
    ** 'view schema only' article.
    */
    select @type = lower(@type collate SQL_Latin1_General_CP1_CS_AS)

    if @type = N'indexed view schema only' and objectproperty(object_id(@qualified_name), 'IsSchemaBound') <> 1
    begin
        raiserror (21277, 11, -1, @qualified_name)          
        return (1)      
    end
    else if @type = N'view schema only' and objectproperty(object_id(@qualified_name), 'IsSchemaBound') = 1
    begin
        raiserror (21275, 11, -1, @qualified_name)
        return (1)
    end

    /*
    ** Only publisher can call sp_addmergearticle
    */
    EXEC @retcode = sys.sp_MScheckatpublisher @pubid
    IF @@ERROR <> 0 or @retcode <>    0
    BEGIN
        RAISERROR (20073, 16, -1)
        RETURN (1)
    END
    
    /*
    ** Parameter Check: @article.
    ** Check to see that the @article is local, that it conforms
    ** to the rules for identifiers, and that it is a table, and not
    ** a view or another database object.
    */

    exec @retcode = sys.sp_MSreplcheck_name @article, '@article', 'sp_addmergearticle'
    if @@ERROR <> 0 or @retcode <> 0
        return(1)
        

    /*
    ** Set the precmdid.  The default type is 'drop'.
    **
    **        @precmdid    pre_creation_cmd
    **        =========    ================
    **              0        none
    **              1        drop
    **              2        delete
    **              3        truncate
    */
    IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('none', 'drop', 'delete', 'truncate')
    BEGIN
      RAISERROR (14061, 16, -1)
      RETURN (1)
    END

    /*
    ** Determine the integer value for the pre_creation_cmd.
    */
    IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'none'
       select @precmdid = 0
    ELSE IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'drop'
       select @precmdid = 1
    ELSE IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'delete'
       select @precmdid = 2
    ELSE IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'truncate'
       select @precmdid = 3


    /*
    ** Set the typeid.    The default type is table.    It can 
    ** be one of following.
    **
    **        @typeid        type
    **        =======        ========
    **           0xa        table
    **          0x20        proc schema only
    **          0x40        view schema only
    **          0x80        func schema only
    **          0x40        indexed view schema only (overloaded)
    **          0xA0        synonym schema only    
    */          

    IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('table', 'proc schema only', 'view schema only', 'func schema only', 'indexed view schema only', 'synonym schema only')
       BEGIN
            RAISERROR (21276, 16, -1)
            RETURN (1)
       END

    IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'table'
    BEGIN
       SET @typeid = 0x0a
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'proc schema only'
    BEGIN
       SET @typeid = 0x20 
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'view schema only'
    BEGIN
       SET @typeid = 0x40
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'indexed view schema only'
    BEGIN
       SET @typeid = 0x40
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'func schema only'
    BEGIN
       SET @typeid = 0x80
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'synonym schema only'
    BEGIN
       SET @typeid = 0xA0
    END


    select @sync_objid = OBJECT_ID(@qualified_name)
    if @sync_objid is NULL
        begin
            raiserror (14027, 11, -1, @qualified_name)
            return (1)
        end


    if @typeid in (0x20,0x40,0x80, 0xA0)
    begin
        if exists (select * from syscomments
                    where id = @sync_objid
                      and encrypted = 1)
        begin
            raiserror(21004, 16, -1, @source_object)
            return 1
        end
    end

    /*
    ** Parameter Check:     @article, @publication.
    ** Check if the article already exists in this publication.
    */

    IF EXISTS (SELECT *
                FROM dbo.sysmergeextendedarticlesview
                WHERE pubid = @pubid
                  AND name = @article)
        BEGIN
            raiserror (21292, 16, -1, @article)
            RETURN (1)
        END

    --if @compatlevel < 90 and @processing_order <> 0
    --begin
    --    raiserror(21585, 16, -1, @publication)
        --return 1
    --end
        
    /*
    ** At this point, all common parameter validations 
    ** for table and schema only articles have been 
    ** performed, so branch out here to handle schema
    ** only articles as a special case.
    */

    IF @typeid in (0x20, 0x40, 0x80, 0xA0)
    BEGIN
        IF @destination_object IS NULL OR @destination_object = N''
        BEGIN
            SELECT @destination_object = @source_object
        END

        IF @schema_option IS NULL
        BEGIN
            SELECT @schema_option = 0x0000000000000001
        END
        EXEC @retcode = sys.sp_MSaddmergeschemaarticle 
                @pubid = @pubid,
                @article = @article,
                @source_object = @source_object,
                @type = @typeid,
                @description = @description,
                @status = @status,
                @pre_creation_command = @precmdid,
                @creation_script = @creation_script,
                @source_owner = @source_owner,
                @destination_owner = @destination_owner,
                @schema_option = @schema_option,
                @destination_object = @destination_object,
                @qualified_name = @qualified_name,     
                @publication = @publication,
                @snapshot_ready = @snapshot_ready,
                @force_invalidate_snapshot = @force_invalidate_snapshot,
                @processing_order = @processing_order

       RETURN (@retcode)
    END

    /*
    ** Make sure that the table name specified is a table.
    */

    if OBJECT_ID(@qualified_name, 'U') is NULL
    begin
        raiserror (20074, 16, -1)
        return (1)
    end

    /*
    ** Don't allow filetables to be added as articles
    */
    SELECT @fFileTable = is_filetable from sys.tables where object_id = @objid
    If (@fFileTable = 1)
    BEGIN
        RAISERROR (33435, 16, -1, @qualified_name)
        return (1)
    END

    /*
        Refer to the Yukon_Merge_Feature_Reference_Tables.doc for
        the detailed schema_option table.
    */
    IF @schema_option IS NULL
    BEGIN
        IF @compatlevel < 90
        BEGIN
            IF @sync_mode = 0   -- native
            BEGIN
    	        SELECT @schema_option = 0x0000000030034FF1
            END
            ELSE                -- character (SSCE)
            BEGIN
    	        SELECT @schema_option = 0x0000000030034FF1
            END
        END
        ELSE    -- 90 compatlevel
        BEGIN
            IF @sync_mode = 0   -- native
            BEGIN
    	        SELECT @schema_option = 0x000000000C034FD1
            END
            ELSE                -- character (SSCE)
            BEGIN
    	        SELECT @schema_option = 0x0000000008034FF1
            END
        END
    END

    /*
       Verify that the schema option being set is compatible with
       publication compatibility level.
    */
    -- Since only the lower 32 bits of @schema_option are 
    -- used, the following check is sufficient. Note that @schema_option is
    -- already padded out to the left at the beginning of this procedure.
    -- whenever anything here is changed also change sp_MSmap_subscriber_type
    declare @schema_option_lodword int
    declare @schema_option_hidword int
    declare @schema_option_xml_to_ntext int
    declare @schema_option_max_to_nonmax int
    declare @schema_option_create_schema int
    declare @schema_option_xml_indexes int
    declare @schema_option_katmaidatetime_to_string int
    declare @schema_option_hierarchyid_to_varbinarymax int
    declare @schema_option_largeUDT_to_varbinarymax int
    declare @schema_option_spatial_indexes int
    declare @schema_option_spatial_to_varbinarymax int
    declare @schema_option_udt_to_base_types int
    declare @schema_option_filtered_indexes int
    declare @schema_option_filestream_attribute int
    declare @schema_option_str sysname

    declare @schema_option_compression int
    select @schema_option_lodword = sys.fn_replgetbinary8lodword(@schema_option)
    select @schema_option_hidword = sys.fn_replgetbinary8hidword(@schema_option)
    select @schema_option_xml_to_ntext  = 0x10000000 -- this has to be on  for < 90RTM
    select @schema_option_max_to_nonmax = 0x20000000 -- this has to be on  for < 90RTM
    select @schema_option_create_schema = 0x08000000 -- this has to be off for < 90RTM
    select @schema_option_xml_indexes   = 0x04000000 -- this has to be off for < 90RTM
    select @schema_option_katmaidatetime_to_string = 0x00000002 

    select @schema_option_hierarchyid_to_varbinarymax = 0x00000020 -- this has to be on for < 100RTM    
    select @schema_option_largeUDT_to_varbinarymax = 0x00000010 -- this has to be on for < 100RTM    
    select @schema_option_spatial_indexes = 0x00000100 -- this has to be off for < 100RTM
    select @schema_option_spatial_to_varbinarymax = 0x00000080 -- this has to be on for < 100RTM
    select @schema_option_udt_to_base_types = 0x00000020
    select @schema_option_filtered_indexes = 0x00000040 -- this has to be off for < 100RTM    
    select @schema_option_filestream_attribute = 0x00000001  
    select @schema_option_compression   = 0x00000004 -- this has to be off for < 100RTM and SSCE
    select @schema_option_filtered_indexes = 0x00000040 -- this has to be off for < 100RTM    

    declare @schema_option_has_changed bit;
    select @schema_option_has_changed = 0;  

 
    /*
    ** If compatlevel is less than 100RTM then make sure the schema option to map down
    ** 100 datatypes to equivalent downlevel datatypes is set. 
    ** * Namely, map large UDT down to varbinary(max)
    ** * If spatial_indexes are set for replication, disable if compatlevel < 100
    ** * Map spatial types to varbinary(max)
    ** * Make sure the schema option to map down 100 datetime types to downlevel data types.
    ** * Disable compression if it is enabled
    ** * Map hierarchyid to varbinary(max)
    */
    if (@compatlevel < 100 and 
       ((@schema_option_hidword & @schema_option_largeUDT_to_varbinarymax = 0) or
       (@schema_option_hidword & @schema_option_compression <> 0) or
       (@schema_option_hidword & @schema_option_hierarchyid_to_varbinarymax = 0) or
       (@schema_option_hidword & @schema_option_spatial_to_varbinarymax = 0) or
       (@schema_option_hidword & @schema_option_spatial_indexes <> 0) or
       (@schema_option_hidword & @schema_option_filtered_indexes <> 0) or
       (@schema_option_hidword & @schema_option_katmaidatetime_to_string = 0)))
    begin
        select @schema_option_hidword = @schema_option_hidword | @schema_option_largeUDT_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_compression)
        select @schema_option_hidword = @schema_option_hidword | @schema_option_hierarchyid_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword | @schema_option_spatial_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_spatial_indexes)
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_filtered_indexes)
        select @schema_option_hidword = @schema_option_hidword | @schema_option_katmaidatetime_to_string

        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;
    end


    /*
    ** For hierarchy we will map it to varbinarymax for 100 RTM compatibility level in character sync mode (for SSCE subscriber).
    ** Turn off the data compression for SSCE subscriber
    */
    if(@compatlevel = 100 and
	 @sync_mode <>0 and
	((@schema_option_hidword & @schema_option_hierarchyid_to_varbinarymax = 0) or
	(@schema_option_hidword & @schema_option_compression <> 0) ))
    begin
    	select @schema_option_hidword = @schema_option_hidword | @schema_option_hierarchyid_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_compression)
        
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)        
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;    	      
    end
    

    /*
    ** If a spatial type is being mapped to non spatial type on the subscriber,
    ** ensure that the spatial index schema option is disabled.
    */
    if (@schema_option_hidword & @schema_option_spatial_indexes <> 0 and
       ((@schema_option_hidword & @schema_option_spatial_to_varbinarymax <> 0) or
       (@schema_option_lodword & @schema_option_udt_to_base_types <> 0)))
    begin
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_spatial_indexes)
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;
    end    

    /*
    ** If compatlevel is less than 90RTM then make sure the schema option to map down
    ** 90 datatypes to equivalent downlevel datatypes is set.
    */
    if (@compatlevel < 90 and 
        (((@schema_option_lodword & (@schema_option_xml_to_ntext | @schema_option_max_to_nonmax)) <> 
        (@schema_option_xml_to_ntext | @schema_option_max_to_nonmax)) or
        ((@schema_option_lodword & (@schema_option_create_schema | @schema_option_xml_indexes)) <> 0)))
    begin
        select @schema_option_lodword = @schema_option_lodword | @schema_option_xml_to_ntext | @schema_option_max_to_nonmax
        select @schema_option_lodword = @schema_option_lodword & ~(@schema_option_create_schema | @schema_option_xml_indexes)
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;
    end

   
    /*      
    ** If we changed what the user originally input, output a message telling them what
    ** the new schema_option value is.
    */
    if @schema_option_has_changed = 1    
    begin
        RAISERROR (20732, 10, -1, @schema_option_str)
    end

    /*
    ** If filestream attribute is enabled, enable stream_blob_columns since
    ** this will lead to lower memory utilization during sync.
    */
    if @schema_option_hidword & @schema_option_filestream_attribute <> 0 and
       @stream_blob_columns_bit = 0
    begin
        RAISERROR (20737, 10, -1)
        select @stream_blob_columns_bit = 1
    end

    /*
    ** If scheme option contains collation or extended properties, 
    ** bump up the compatibility-level
    */      
    declare @xprop_schema_option int
    declare @collation_schema_option int
    select @xprop_schema_option = 0x00002000
    select @collation_schema_option = 0x00001000
    if (@schema_option_lodword & @collation_schema_option) <> 0 and @compatlevel < 40
    begin     
        raiserror(21389, 10, -1, @publication)
        select @bump_to_80 = 1
    end
    if (@schema_option_lodword & @xprop_schema_option) <> 0 and @compatlevel < 40
    begin    
        raiserror(21390, 10, -1, @publication)
        select @bump_to_80 = 1
    end

    /*
    ** Merge table articles does not really support destination object. It has the same value as source
    */
    if @destination_object <> @source_object
    begin
        raiserror(20638, 10, -1)
    end

    select @destination_object = @source_object

    /*
    select @row_size=sum(max_length) from sys.columns where object_id=OBJECT_ID(@qualified_name)
    if @row_size>6000 
        begin
            RAISERROR (21062, 16, -1, @qualified_name)    
            -- RETURN (1)
        end
    */
    IF LOWER(@vertical_partition collate SQL_Latin1_General_CP1_CS_AS) = 'false'
    begin
        select @ver_partition = 0
    end
    else
    begin
        select @ver_partition = 1
    end
    select @num_columns=count(*) from sys.columns where object_id = object_id(@qualified_name)

    -- After Yukon beta 1, this will be changed to 1024.
    if @num_columns > 246 and LOWER(@vertical_partition collate SQL_Latin1_General_CP1_CS_AS) = 'false'
    begin
        RAISERROR (20068, 16, -1, @qualified_name, 246)
        RETURN (1)
    end

    -- Colvs can only handle 246 columns so we can not allow more than 246 columns to exist on a table
    -- if column tracking is being used. The reason is that we track all the columns even when vertical
    -- partitioning is used and only some of the columns are published.
    if @num_columns > 246 and LOWER(@column_tracking collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
    begin
        RAISERROR (25020, 16, -1, @qualified_name, 246)
        RETURN (1)
    end

    -- If the input param @subscriber_upload_options contradicts to the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                where objid = @objid and upload_options <> @subscriber_upload_options)
    begin
        raiserror (20053, 11, -1, 'subscriber_upload_options', @qualified_name)
        return (1)
    end

    -- If the input param @delete_tracking contradicts with the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                    where objid = @objid and delete_tracking <> @deletetracking)
    begin
        raiserror (20648, 16, -1, @qualified_name)
        return (1)
    end

    -- If the input param @stream_blob_columns contradicts with the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                    where objid = @objid and stream_blob_columns <> @stream_blob_columns_bit)
    begin
        raiserror (20053, 11, -1, 'stream_blob_columns', @qualified_name)
        return (1)
    end

    -- If the input param @compensate_for_errors contradicts to the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                    where objid = @objid and compensate_for_errors <> @compensateforerrors)
    begin
        raiserror (20053, 11, -1, 'compensate_for_errors', @qualified_name)
        return (1)
    end

    -- Subscribers below 80SP3 build 858 will ignore the @compensate_for_errors=false setting.
    -- Raise a warning.
    if @compensateforerrors = 0 and
       @compatlevel <= @REPOLEVersion_80SP3
    begin
        raiserror(20004, 10, -1, @publication, 'compensate_for_errors', '8.00.0858')
    end

    -- articles with >=246 columns can only be added to Yukon-compatible publications
    -- if the article is about to be republished, there might already be missing/excluded columns
    --
    if @compatlevel <= 90    -- After Yukon beta 1, we will change this to " < 90".
    begin
        declare @cCols int -- number of columns in the table
        declare @cMissing int -- number of missing cols
        
        select @cCols= count(*) from sys.columns where object_id = @objid and is_computed <> 1 and system_type_id <> type_id('timestamp')
        set @cMissing= coalesce((select max(missing_col_count) from dbo.sysmergearticles where objid = @objid), 0)
        if ((@cCols + @cMissing) > 246) and LOWER(@vertical_partition collate SQL_Latin1_General_CP1_CS_AS) = 'false'
        begin
            --raiserror(21522,16,1,@article,@publication, 246)
            RAISERROR (20068, 16, -1, @qualified_name, 246)
            return (1)
        end
    end
    
    /*
    ** If current publication contains a non-sync subscription, all articles to be added in it
    ** has to contain a rowguidcol.
    */
    if exists (select * from dbo.sysmergesubscriptions where pubid = @pubid and sync_type = 2)
    begin
        if not exists (select * from sys.columns
            where object_id=@objid and is_rowguidcol = 1)
            begin
                raiserror(20086 , 16, -1, @publication)
                return (1)
            end
    end

    --
    -- Parameter Check:  @identityrangemanagementoption.
    -- We will override the value specified in @auto_identity_range if
    -- @identityrangemanagementoption is not null
    --
    if (@identityrangemanagementoption is NULL)
    begin
        --
        -- @identityrangemanagementoption is null
        -- Check @auto_identity_range
        --
        IF @auto_identity_range IS NULL 
        begin
            --
            -- user did not specify any explicit values for identity management
            --
            select @identityrangemanagementoption = 'none'
        end
        else if LOWER(@auto_identity_range collate SQL_Latin1_General_CP1_CS_AS) IN ('true', 'false')
        begin
            -- map the value of @auto_identity_range to @identityrangemanagementoption
            select @identityrangemanagementoption = case 
                when LOWER(@auto_identity_range collate SQL_Latin1_General_CP1_CS_AS) = N'true' 
                then N'auto' else N'none' end
            -- Issue warning on deprecation of this option
            raiserror (21767, 10, 1, '@auto_identity_range', '@identityrangemanagementoption')
        end
        else
        BEGIN
            -- invalid value for @auto_identity_range
            RAISERROR (14148, 16, -1, '@auto_identity_range')
            return (1)
        END
    end
    else
    begin
        --
        -- validate @identityrangemanagementoption
        --
        if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('none', 'manual', 'auto')
        BEGIN
            -- invalid value for @identityrangemanagementoption
            RAISERROR (20644, 16, -1, @identityrangemanagementoption)
            return (1)
        END
        select @identityrangemanagementoption = lower(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS)
    end

    if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) <> 'auto' and (@identity_range is not NULL or @threshold is not NULL or @pub_identity_range is not NULL)
    begin
        raiserror(21282, 16, -1)
        return (1)
    end


    if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto'
    begin
        /*
        ** If you want to have identity support, @range and threshold can not be NULL
        */
        if (@identity_range is NULL or @pub_identity_range is NULL)
        begin
            raiserror(21193, 16, -1)
            return (1)
        end

        if @compatlevel < 90 and @threshold is NULL
        begin
            raiserror(21193, 16, -1)
            return (1)
        end

        exec @retcode = sys.sp_MScheck_autoident_parameters
                                    @qualified_name,
                                    @pub_identity_range,
                                    @identity_range,
                                    @threshold

        if @retcode<>0 or @@error<>0
        begin
            raiserror(20707, 16, -1)
            return 1
        end

        select @ident_incr = IDENT_INCR(@qualified_name)
                
        select @identity_support = 1

        exec @retcode = sys.sp_MScompute_maxmin_identity @objid, @max_range output, @min_range output
        if @retcode<>0 or @@error<>0
        begin
            raiserror(20707, 16, -1)
            return 1
        end

        if @ident_incr < 0
        begin
            select @pub_identity_range = -1*@pub_identity_range
            select @identity_range = -1*@identity_range
        end
    end
    else
        select @identity_support = 0

    /*
    ** If the table contains one more columns of type bigint or sql_variant, 
    ** we bump up the backward compatibility level.
    */
    if EXISTS (SELECT * FROM sys.columns c WHERE c.object_id = @sync_objid
                AND (c.system_type_id = type_id('bigint') or c.system_type_id = type_id('sql_variant'))) and @compatlevel < 40
    begin
        raiserror(21357, 10, -1, @publication)
        select    @bump_to_80 = 1
    end

    /*
    ** 7.0 subscribers do not like data type 'timestamp'
    */
    if EXISTS (select * from sys.columns where object_id=@sync_objid and system_type_id = type_id('timestamp')) and @compatlevel < 40
    begin
        raiserror(21358, 10, -1, @publication)
        select @bump_to_80 = 1
    end
        
    /*
    ** Validate the column tracking
    */
    if @column_tracking IS NULL OR LOWER(@column_tracking collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
    BEGIN
        RAISERROR (14148, 16, -1, '@column_tracking')
        RETURN (1)
    END
    if LOWER(@column_tracking collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
        SET @column_tracking_id = 1
    else 
        SET @column_tracking_id = 0


    /*
    ** Check for partioned tables. Not supported when sync_mode is 1 (SSCE)
    */

    if @sync_mode = 1 
    begin
        -- Check if the table is partitioned.
        if exists (select * from (sys.indexes as i INNER JOIN sys.partition_schemes as ps
                                                              ON (i.data_space_id = ps.data_space_id))
                        where (i.object_id = object_id(@qualified_name)) and
                            (i.index_id IN (0,1)))  -- to ensure that we are dealing with tables
        begin
            RAISERROR (22534, 16, -1)
            RETURN (1)
        end        
    end
    
    /*
    ** Replication not supported on a table with clustered columnstore index.
    */

    -- Check if the table has clustered columnstore index.
    select TOP 1 @index_objid=object_id, @idxname=name from sys.indexes
                    where (object_id = @objid) and
                        (type = 5)
    if @index_objid is not NULL
    begin
        RAISERROR (35353, 16, -1, N'Replication', @idxname, @source_object)
        RETURN (1)
    end

    /*
    ** Parameter Check: @allow_interactive_resolver     
    */
    if LOWER(@allow_interactive_resolver collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@allow_interactive_resolver')
            RETURN (1)
        END
    if LOWER(@allow_interactive_resolver collate SQL_Latin1_General_CP1_CS_AS) = 'true'
        set @allow_interactive_bit = 1
    else 
        set @allow_interactive_bit = 0
        
    /*
    ** Parameter Check: @published_in_tran_pub     
    */
    if LOWER(@published_in_tran_pub collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@published_in_tran_pub')
            RETURN (1)
        END
    if LOWER(@published_in_tran_pub collate SQL_Latin1_General_CP1_CS_AS) = 'true'
    BEGIN
        set @published_in_tran_pub_bit = 1
    END
    else 
        set @published_in_tran_pub_bit = 0

    /*
    ** Parameter Check: @fast_multicol_updateproc  
    */
    if LOWER(@fast_multicol_updateproc collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
    BEGIN
        RAISERROR (14148, 16, -1, '@fast_multicol_updateproc')
        RETURN (1)
    END
    if LOWER(@fast_multicol_updateproc collate SQL_Latin1_General_CP1_CS_AS) = 'true'
        set @fast_multicol_updateproc_bit = 1
    else 
        set @fast_multicol_updateproc_bit = 0

    if @partition_options not in (0, 1, 2, 3)
    begin
        RAISERROR (22526, 16, -1, '@partition_options')
        RETURN (1)
    end

    if @partition_options in (1, 2, 3) and @compatlevel < 90
    begin
        raiserror(20681, 16, -1, @publication)
        return 1
    end    

    execute @retcode = sys.sp_MSgetreplnick @pubid = @pubid, @replnick = @replnick output
    if (@@error <> 0) or @retcode <> 0 or @replnick IS NULL 
    begin
        RAISERROR (14055, 11, -1)
        RETURN(1)
    end                    

    /*
    ** Validate the article resolver
    */
    if @article_resolver IS NOT NULL
    begin
        if @article_resolver = 'default' OR @article_resolver = ''
            begin
                select @article_resolver = NULL
                select @resolver_clsid = NULL
            end                    
        else
            begin
                EXECUTE @retcode = sys.sp_lookupcustomresolver @article_resolver, @resolver_clsid OUTPUT
                IF @retcode <> 0 or @resolver_clsid IS NULL
                BEGIN
                      RAISERROR (20020, 16, -1, @article_resolver)
                      RETURN (1)
                END
            end
    end

    /* 
    ** A resolver clsid of '00000000-0000-0000-0000-000000000000' indicates a .NET Assembly resolver , ensure that the 
    ** resolver_info contains the name of the class that implements the Microsoft.SqlServer.Replication.BusinessLogicSupport.BusinessLogicModule
    ** interface.    
    */
    IF @resolver_clsid = '00000000-0000-0000-0000-000000000000'
        begin
            declare @is_dotnet_assembly bit
            declare @dotnet_assembly_name nvarchar(255)
            declare @dotnet_class_name nvarchar(255)

            EXECUTE @retcode = sys.sp_lookupcustomresolver @article_resolver, @resolver_clsid OUTPUT, @is_dotnet_assembly OUTPUT, @dotnet_assembly_name OUTPUT, @dotnet_class_name OUTPUT
            if @dotnet_assembly_name IS NULL 
                begin
                    RAISERROR (21856, 16, -1, @article_resolver)
                    return (1)
                end
            if @dotnet_class_name IS NULL 
                begin
                    RAISERROR (21808, 16, -1, @article_resolver)
                    return (1)
                end
            select @article_resolver = @dotnet_assembly_name
            /* If passed in resolver_info contains a .NET class name, do not override it with the default */
            if @resolver_info is null
                select @resolver_info = @dotnet_class_name
        end


    /*
    ** If article resolver is 'SP resolver', make sure that resolver_info refers to an SP or XP;
    ** Also make sure it is stored with owner qualification
    */
    if    @article_resolver = @sp_resolver
        begin
            if not exists (select * from sys.objects where object_id = object_id(@resolver_info) and ( type = 'P' or type = 'X'))
                begin
                    raiserror(21343, 16, -1, @resolver_info)
                    return (1)
                end
                
            select @sp_name = name, @sp_owner=SCHEMA_NAME(schema_id) from sys.objects where object_id = object_id(@resolver_info)
            select @resolver_info = QUOTENAME(@sp_owner) + '.' + QUOTENAME(@sp_name) 
        end

    /* The following resolvers expect the @resolver_info to be NON NULL */
    if    @article_resolver = @sp_resolver or 
        @article_resolver = @additive_resolver or
        @article_resolver = @average_resolver or
        @article_resolver = @minimum_resolver or
        @article_resolver = @maximum_resolver or
        @article_resolver = @mindate_resolver or
        @article_resolver = @maxdate_resolver or
        @article_resolver = @mergetxt_resolver or
        @article_resolver = @pricolumn_resolver
        begin
            if @resolver_info IS NULL 
                begin
                    RAISERROR (21301, 16, -1, @article_resolver)
                    return (1)
                end
        end
    /*
    ** If article resolver uses column names, make sure that resolver_info refers to a valid column.
    */
    if    @article_resolver = @pricolumn_resolver or
        @article_resolver = @additive_resolver or
        @article_resolver = @average_resolver or
        @article_resolver = @minimum_resolver or
        @article_resolver = @maximum_resolver
        begin
            if not exists (select * from sys.columns where object_id = @objid and name=@resolver_info)
                begin
                    RAISERROR (21501, 16, -1, @article_resolver)
                    return (1)
                end
        end
    /*
    ** If article resolver is 'mindate/maxdate resolver', make sure that resolver_info refers to a column that is of datatype 'datetime' or smalldatetime
    */
    if  @article_resolver = @mindate_resolver or
        @article_resolver = @maxdate_resolver
    begin
        if not exists (select * from sys.columns where object_id = @objid and name=@resolver_info and (system_type_id=type_id('datetime') or system_type_id=type_id('smalldatetime') 
           or system_type_id=type_id('datetime2')
           or system_type_id=type_id('date')
	    or system_type_id=type_id('time')
           )) /*need to add version condition >=100*/
        begin
            RAISERROR (21302, 16, -1, @article_resolver)
            return (1)
        end
    end

    /* The following resolvers expect the article to be column tracked - warn that the default resolver will be used */
    if    @article_resolver = @additive_resolver or
        @article_resolver = @average_resolver or
        @article_resolver = @mergetxt_resolver
    begin
        if @column_tracking_id = 0
        begin
            RAISERROR (21303, 10, -1, @article, @article_resolver)
        end
    end

    if @resolver_info IS NOT NULL and @article_resolver IS NULL
    begin
        RAISERROR (21300, 10, -1, @article)
        set @resolver_info = NULL
    end

    /*
    ** Parameter Check: logical_record_level_conflict_detection
    */
    if @column_tracking IS NULL OR LOWER(@logical_record_level_conflict_detection collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@logical_record_level_conflict_detection')
            RETURN (1)
        END
    if LOWER(@logical_record_level_conflict_detection collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
        SET @logical_record_level_conflict_detection_id = 1
    else 
        SET @logical_record_level_conflict_detection_id = 0
        
    /*
    ** Parameter Check: logical_record_level_conflict_resolution
    */
    if @column_tracking IS NULL OR LOWER(@logical_record_level_conflict_resolution collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@logical_record_level_conflict_resolution')
            RETURN (1)
        END
    if LOWER(@logical_record_level_conflict_resolution collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
        SET @logical_record_level_conflict_resolution_id = 1
    else 
        SET @logical_record_level_conflict_resolution_id = 0

    if @logical_record_level_conflict_detection_id = 1 and @logical_record_level_conflict_resolution_id = 0
    begin
        raiserror (21728, 16, -1)
        return 1
    end
    
    if @logical_record_level_conflict_detection_id = 1 or @logical_record_level_conflict_resolution_id = 1
    begin
		-- Only supported with publications that have 90 compatibility level.
		select top 1 @pubname_lessthan90compat = name from dbo.sysmergepublications 
		where backward_comp_level < 90
		and (pubid = @pubid or
			 pubid in 
				(select pubid from dbo.sysmergearticles where objid = @objid))
	        
		if @pubname_lessthan90compat is not null
		begin
			raiserror(21574, 16, -1, @article, @pubname_lessthan90compat)
			return 1
		end
		
		if exists (select * from dbo.sysmergepublications where pubid = @pubid and sync_mode = 1)
		begin
			raiserror(22541, 16, -1, @publication)
			return 1
		end
		
		if exists (select * from dbo.sysmergepublications where pubid = @pubid and allow_web_synchronization = 1)
		begin
			raiserror(22545, 16, -1, @publication)
			return 1
		end

		-- Cannot use Logical records and BusinessLogicResolvers at the same time.
		IF @resolver_clsid = '00000000-0000-0000-0000-000000000000'
        begin
			raiserror(20708, 16, -1)
			return 1
        end

		
		-- based on usability feeback, we should set the allow_subscriber_initiated_snapshot
		-- option to 1 rather than raise an error.
		update dbo.sysmergepublications set allow_subscriber_initiated_snapshot = 1
			where pubid = @pubid
	end
	
	/* Make sure that coltracking option matches */
    if exists (select * from dbo.sysmergearticles where objid = @objid and
            identity_support <> @identity_support)
    begin
        raiserror (21240, 16, -1, @source_object)
        return (1)
    end

    -- Do not allow the table to be published by both merge and queued tran
    if object_id('syspublications') is not NULL
    begin
        if exists (select * from syspublications p, sysarticles a where 
            p.allow_queued_tran = 1 and
            p.pubid = a.pubid and
            a.objid = @objid)
        begin
            select @obj_name = object_name(@objid)
            raiserror(21266, 16, -1, @obj_name)
            return (1)
        end

        -- Do not allow the table to be published in both merge tran using automatic identity range management
        if exists (select * from  sysarticles sa, sysarticleupdates au, syspublications pub where 
                sa.objid = @objid and
                au.artid = sa.artid and
                au.pubid = pub.pubid and
                au.identity_support = 1) and
           @identity_support = 1
        begin
            raiserror(20677, 16, -1, @article)
            return (1)
        end
    end
	--co-existance of uploadable merge article on queued subscription table may cause non-convergence in tran pub since queued trigger is NFR
	--allow it in case some customer rely on this already, write warning to errorlog so we can track this condition
	if (0 = @subscriber_upload_options) and (object_id('dbo.MSsubscription_articles') is not null)
	begin
		if exists(select * from dbo.MSsubscription_articles where object_id(quotename(owner) + N'.' + quotename(dest_table)) = @objid)
		begin
			select @obj_name = object_name(@objid)
			raiserror(21860, 10, -1, @obj_name, @db_name) WITH LOG
		end
	end
	
    if exists (select * from dbo.sysmergearticles where objid=@objid and sys.fn_MSmerge_islocalpubid(pubid)=1)
        select @already_published = 1

    if @already_published = 1 and LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto'
    begin
        if @compatlevel < 40
        begin
            raiserror(21359, 10, -1, @publication)
            select @bump_to_80 = 1
        end
        if exists (select * from dbo.sysmergearticles where objid=@objid and sys.fn_MSmerge_islocalpubid(pubid)=1 and
            ((pub_range<>@pub_identity_range) or (range <> @identity_range) or (threshold <> @threshold)))
        begin
            raiserror(21291, 16, -1)
            return (1)
        end
    end

    if 0 <> @subscriber_upload_options and @compatlevel < 90
    begin
        raiserror(21522, 16, -1, 'subscriber_upload_options', @publication)
        return 1
    end

    --Do not allow the table to be published if it contains sparse columns or sparse column_set
    if exists (select * from sys.columns where object_id = @objid and (is_sparse = 1 or is_column_set=1) )
    begin
        raiserror(20738, 16, -1, @article);
        return (1)
    end
   

    /*
    **    Add article to dbo.sysmergearticles and update sys.objects category bit.
    */
    begin tran
    save TRAN sp_addmergearticle

    exec @retcode = sys.sp_MSgetmergeadminapplock @timeout = 0, -- no wait
                                                  @lockowner = N'Transaction'
    if @retcode<>0 or @@error<>0
    begin
        raiserror(20713, 16, -1, 'sp_addmergearticle', @publication)
        goto FAILURE
    end

    select @got_merge_admin_applock = 1

    -- Parameter check @subset_filterclause
    if @subset_filterclause <> '' and @subset_filterclause is not NULL
    begin
        -- check if this is a dynamically filtered article and this is not a dynamically filtered publication
        select @article_has_dynamic_filters = 0
        exec @retcode = sys.sp_check_subset_filter 
                                @qualified_name, 
                                @subset_filterclause, 
                                @article_has_dynamic_filters output, 
                                @functions_in_subset_filter output
        if @retcode<>0 or @@ERROR<>0
        begin
            raiserror(20641, 16, -1)
            goto FAILURE
        end
        if @article_has_dynamic_filters = 1 and 
           (@publication_has_dynamic_filters = 0 or @functions_in_subset_filter <> @dynamic_filters_function_list)
        begin
            if @snapshot_ready > 0 and
               ((@allow_anonymous = 1 and @compatlevel < 90) or 
                 exists (select * from dbo.sysmergesubscriptions where pubid=@pubid and subid<>pubid and status=1)) and
               @force_reinit_subscription = 0
            begin
                raiserror(20642, 16, -1, @article, @subset_filterclause, @publication)
                goto FAILURE
            end
            select @reinit_subscriptions = 1
        end
        
        -- If the newly added article has dynamic filters and the publication is already using partition groups
        -- then setup the correct metadata in merge system tables  such that rerun of snapshot agent sets the partition groups metadata correctly
        if @article_has_dynamic_filters = 1 and @use_partition_groups > 0 and @functions_in_subset_filter <> @dynamic_filters_function_list
        begin
             if @use_partition_groups > 0
             begin
                 delete from dbo.MSmerge_current_partition_mappings where publication_number = @pub_number
                 if @@error <> 0 goto FAILURE
                 delete from dbo.MSmerge_past_partition_mappings where publication_number = @pub_number
                 if @@error <> 0 goto FAILURE
                 delete from dbo.MSmerge_generation_partition_mappings where publication_number = @pub_number
                 if @@error <> 0 goto FAILURE
                 update dbo.sysmergepublications set use_partition_groups = 2 where pubid = @pubid
                 if @@error <> 0 goto FAILURE
             end
             
             exec @retcode = sys.sp_MSdropmergedynamicsnapshotjob @publication = @publication
             if @@error <> 0 or @retcode <> 0
                 goto FAILURE
 
             delete from dbo.MSmerge_dynamic_snapshots where partition_id in
                 (select partition_id from dbo.MSmerge_partition_groups where publication_number = @pub_number)
             if @@error <> 0 goto FAILURE
                 
             delete from dbo.MSdynamicsnapshotjobs where partition_id in
                 (select partition_id from dbo.MSmerge_partition_groups where publication_number = @pub_number)
             if @@error <> 0 goto FAILURE
             
             delete from dbo.MSmerge_partition_groups where publication_number = @pub_number
             if @@error <> 0 goto FAILURE
 
             update dbo.sysmergepublications 
             set dynamic_filters_function_list = NULL,
                dynamic_filters = 0
                where pubid = @pubid
             if @@error <> 0 goto FAILURE
              
             -- Since this is called from sp_addmergearticle, make sure it doesn't raise errors since it is premature stage of the publication. 
             -- The snapshot calls this with @dont_raise_error = NULL which should raise appropriate errors
             exec @retcode = sys.sp_MSset_dynamic_filter_options @publication = @publication, @dynamic_filters = @publication_has_dynamic_filters OUTPUT, @dont_raise_error = 1
             if @retcode<>0 or @@ERROR<>0 goto FAILURE
       end              
    end

    /*
    ** We used to prevent an article from being added to a publication whose snapshot
    ** has been run already. Now we change this so that it is acceptable by doing reinit.
    */
    if @snapshot_ready > 0 
    begin
        if @force_invalidate_snapshot = 0 and @snapshot_ready = 1
        begin
            raiserror(21364, 16, -1, @article)
            goto FAILURE
        end
        update dbo.sysmergepublications set snapshot_ready=2 where pubid=@pubid
        if @@ERROR<>0
            goto FAILURE
    end

    /* 
    ** article status 5 or 6 means there is at least one new article after snapshot is ready
    ** hence all articles added after that point will be new articles as well, regardless of snapshot_ready value.
    */
    if @snapshot_ready>0 or exists (select * from dbo.sysmergearticles where pubid=@pubid and (status=5 or status=6))
    begin
        select @needs_pickup=1
    end

    if @reinit_subscriptions = 1
    begin
        exec @retcode = sys.sp_MSreinitmergepublication 
                                @publication = @publication,
                                @upload_first = @automatic_reinitialization_policy
        if @retcode<>0 or @@ERROR<>0 return 1
    end

    -- if because this article is dynamically filtered the publication is going to change from being 
    -- a static to a dynamic publication, we need to delete all entries in sysmergeschemachange.
    if @publication_has_dynamic_filters = 0 and @article_has_dynamic_filters = 1
    begin
        declare @SCHEMA_TYPE_DROPARTICLE int

        select @SCHEMA_TYPE_DROPARTICLE = 28
        -- don't delete dropmergearticle related schema changes.
        delete from dbo.sysmergeschemachange where pubid = @pubid and schematype not in (@SCHEMA_TYPE_DROPARTICLE)
        if @reinit_subscriptions = 1
            select @needs_pickup = 0
    end

    -- Acquire sch-M lock up-front on the published object 
    exec sys.sp_MSget_qualified_name @objid, @qualname OUTPUT
    if @qualname is null
        goto FAILURE
            
    exec %%Object(MultiName = @qualname).LockMatchID(ID = @objid, Exclusive = 1, BindInternal = 0)
    --exec %%Object(MultiName = @qualname).LockExclusiveMatchID(ID = @objid)
    if @@error <> 0
        goto FAILURE

    select @artid = artid, 
           @preserve_rowguidcol= preserve_rowguidcol
       from dbo.sysmergearticles where objid = @objid

    -- If that article is already in another publication, we reuse its preserve_rowguidcol.
    -- If the article is added the first time, we set preserve_rowguidcol depending
    -- on whether there already is a rowguidcol.
    if @preserve_rowguidcol is null
    begin
        if ObjectProperty(object_id(@qualified_name), 'tablehasrowguidcol') = 1
        begin
            set @preserve_rowguidcol= 1
        end
        else
        begin
            set @preserve_rowguidcol= 0
        end
    end

    if @snapshot_ready > 0
    begin
        /* 
        ** Add the guid column to the user table if needed, cause snapshot_ready>0 would imply
        ** this article has got a rowguid column. No need to add index, triggers, or procedures
        ** as snapshot run will take care of those.
        */
        execute @retcode = sys.sp_MSaddguidcolumn @source_owner, @source_object
        if @@ERROR <> 0 OR    @retcode <> 0  -- NOTE: new change
            goto FAILURE
        execute @retcode = sys.sp_MSaddguidindex @publication, @source_owner, @source_object
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE
    end
    
    --
    -- Need to change sys.columns status before generating sync procs/custom procs 
    -- because the status will be used to decide whether or not call set identity insert. Enable
    -- NFR property if identityrangemanagementoption is MANUAL or AUTO. If 
    -- identityrangemanagementoption is NONE then we will not explicity enable NFR.
    --
    -- This is to change identity column to 'not for replication' if not having been so already
    IF @identityrangemanagementoption in ('auto', 'manual' )
    begin
        select @colname = name
        from sys.columns
        where object_id = @objid and
            is_identity = 1 and -- is identity
            ColumnProperty(object_id, name, 'IsIdNotForRepl') = 0 -- No 'not for repl' property
        if @colname is not null
        begin
            -- Mark 'not for repl'
            EXEC %%ColumnEx(ObjectID = @objid, Name = @colname).SetIdentityNotForRepl(Value = 1)
            IF @@ERROR <> 0
                GOTO FAILURE
        end
    end

    select @statusid = 1  -- default status is inactive

    if @artid is NULL
    begin
        set @artid = newid()
        if @@ERROR <> 0
            goto FAILURE
        execute @retcode = sys.sp_MSgentablenickname @tablenick output, @replnick, @objid
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE
    end
    -- Clone the article properties if article has already been published (in a different pub)
    else
    begin
        /*
        ** Parameter Check:     @article, @publication.
        ** Check if the table already exists in this publication.
        */
        if exists (select * from dbo.sysmergearticles
            where pubid = @pubid AND artid = @artid)
        begin
            raiserror (21292, 16, -1, @source_object)
            goto FAILURE
        end
        
        /* Make sure that coltracking option matches */
        if exists (select * from dbo.sysmergearticles where artid = @artid and
                     column_tracking <> @column_tracking_id)
            begin
                raiserror (20030, 16, -1, @article)
                goto FAILURE
            end
                    
        /* Reuse the article nickname if article has already been published (in a different pub)*/
        select @tablenick = nickname from dbo.sysmergearticles where artid = @artid
        if @tablenick IS NULL
            goto FAILURE
            
        /* Make sure that @resolver_clsid matches the existing resolver_clsid */
        select @resolver_clsid_old = resolver_clsid from dbo.sysmergearticles where artid = @artid 
        if ((@resolver_clsid IS NULL AND @resolver_clsid_old IS NOT NULL) OR
            (@resolver_clsid IS NOT NULL AND @resolver_clsid_old IS NULL) OR
            (@resolver_clsid IS NOT NULL AND @resolver_clsid_old IS NOT NULL AND @resolver_clsid_old <> @resolver_clsid))
        begin
            raiserror (20037, 16, -1, @article)
            goto FAILURE
        end

        /* If publisher could be subscribing the article from another publisher. If so then select the missing_cols info from an existing entry. */

        select @missing_cols = 0x00
        select @missing_col_count = 0
        select @missing_cols = missing_cols,  @missing_col_count = missing_col_count from dbo.sysmergearticles 
            where artid = @artid and sys.fn_MSmerge_islocalpubid(pubid) = 0

        /* Insert to articles, copying some stuff from other article row */
        insert into dbo.sysmergearticles (name, type, objid, sync_objid, artid, description,
                pre_creation_command, pubid, nickname, column_tracking, status,
                creation_script, article_resolver,
                resolver_clsid, schema_option, 
                destination_object, destination_owner, subset_filterclause, view_type, resolver_info, gen_cur, 
                missing_cols, missing_col_count, excluded_cols, excluded_col_count, identity_support,
                before_image_objid, before_view_objid, verify_resolver_signature, allow_interactive_resolver, 
                fast_multicol_updateproc, check_permissions, processing_order, upload_options, published_in_tran_pub, before_upd_view_objid,
                delete_tracking, compensate_for_errors, pub_range, range, threshold, stream_blob_columns, preserve_rowguidcol)
            -- use top 1, distinct could return more than one matching row if status different on partitioned articles
            select top 1 @article, type, objid, @sync_objid, @artid, @description, @precmdid,
                @pubid, nickname, column_tracking, @statusid, @creation_script,
                article_resolver, resolver_clsid, @schema_option, @destination_object, @destination_owner, @subset_filterclause, 
                0, resolver_info, gen_cur, @missing_cols, @missing_col_count, 0x00,0, identity_support,
                before_image_objid, before_view_objid, verify_resolver_signature, allow_interactive_resolver, 
                fast_multicol_updateproc, @check_permissions, @processing_order, @subscriber_upload_options, @published_in_tran_pub_bit, before_upd_view_objid,
                @deletetracking, @compensateforerrors, @pub_identity_range, @identity_range, @threshold, @stream_blob_columns_bit, @preserve_rowguidcol
            from dbo.sysmergearticles where artid = @artid
                
        if (@@rowcount = 1)
        begin
            insert into dbo.sysmergepartitioninfo (artid, pubid, logical_record_level_conflict_detection, logical_record_level_conflict_resolution, partition_options) 
                values (@artid, @pubid, @logical_record_level_conflict_detection_id, @logical_record_level_conflict_resolution_id, @partition_options)
        
            exec @retcode = sys.sp_MScreate_article_repl_view @pubid, @artid
            if @retcode <> 0 or @@error <> 0
                goto FAILURE    
        end
        
        -- need to validate well-partitioned articles even if this particular one
        -- may not be well-partitioned (e.g. with the same article already existing as well-partitioned
        -- in a different publication or subscription).    
        exec @retcode = sys.sp_MSvalidate_wellpartitioned_articles @publication
        if @@error <> 0 or @retcode <> 0
            goto FAILURE
            
        -- identity range setup code
        -- we will only get here if this is a re-publisher or the article is being published in more than one
        -- publication on the root publisher
        -- if @already_published is 1 then the article is being published in more than one publication on the
        -- root publisher. In that case we need not do anything here. However, if @already_publisher is 0, this is a republisher 
        -- and we need to setup the ranges for the republished publication
        if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto' and @already_published = 0
        begin        
            if @compatlevel < 40
            begin
                raiserror(21359, 10, -1, @publication)
                select @bump_to_80 = 1
            end

            exec @retcode = sys.sp_MScheck_republisher_ranges @qualified_name, @artid, @pub_identity_range, @identity_range
            if @retcode<>0 or @@error<>0
                goto FAILURE
                
        end -- end identity range code
      
        /* Jump to end of transaction  */
        goto DONE_TRAN
    end

    /* Add the specific GUID based replication columns to dbo.sysmergearticles */
    insert into dbo.sysmergearticles (name, objid, sync_objid, artid, type, description, pubid, nickname, 
            column_tracking, status, schema_option, pre_creation_command, destination_object, destination_owner, 
            article_resolver, resolver_clsid, subset_filterclause, view_type, resolver_info, columns,
            missing_cols, missing_col_count, excluded_cols, excluded_col_count, identity_support,
            before_image_objid, before_view_objid, verify_resolver_signature, creation_script, allow_interactive_resolver, 
            fast_multicol_updateproc, check_permissions, processing_order, upload_options, published_in_tran_pub, before_upd_view_objid,
            delete_tracking, compensate_for_errors, pub_range, range, threshold, stream_blob_columns, preserve_rowguidcol)
    values (@article, @objid, @sync_objid, @artid, @typeid, @description, @pubid, @tablenick, 
            @column_tracking_id, @statusid, @schema_option, @precmdid, @destination_object, @destination_owner, 
            @article_resolver, @resolver_clsid, @subset_filterclause, 0, @resolver_info, NULL,
             0x00, 0, 0x00,0, @identity_support, NULL, NULL, @verify_resolver_signature, @creation_script, @allow_interactive_bit, 
             @fast_multicol_updateproc_bit, @check_permissions, @processing_order, @subscriber_upload_options, @published_in_tran_pub_bit, NULL,
             @deletetracking, @compensateforerrors, @pub_identity_range, @identity_range, @threshold, @stream_blob_columns_bit, @preserve_rowguidcol)
    if @@ERROR <> 0
        goto FAILURE

    insert into dbo.sysmergepartitioninfo (artid, pubid, logical_record_level_conflict_detection, logical_record_level_conflict_resolution, partition_options) 
        values (@artid, @pubid, @logical_record_level_conflict_detection_id, @logical_record_level_conflict_resolution_id, @partition_options)
    if @@error <> 0
        goto FAILURE

    /*
    ** identity range setup
    */
    if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto' and @already_published = 0
    begin
        declare @range_begin numeric(38,0)
        declare @range_end numeric(38,0)
        declare @next_range_begin numeric(38,0)
        declare @next_range_end numeric(38,0)

        if @compatlevel < 40
        begin
            raiserror(21359, 10, -1, @publication)
            select @bump_to_80 = 1
        end

        -- the following statement will ensure that the ident_curr in now equal to the highest values stored in the 
        -- identity column (lowest value for negative increments). This to account for incorrect reseeds
        -- IDENT_CURRENT Returns the last identity value generated for a specified table in any session and any scope. 
        DBCC CHECKIDENT(@qualified_name, RESEED) with no_infomsgs

        select @max_used = IDENT_CURRENT(@qualified_name)

        if @max_used is NULL
            select @max_used = IDENT_SEED(@qualified_name)

        -- max_used only matters in prepare merge article when we acually allocating a range        
        -- insert the publisher's entry into MSmerge_identity_range table
        -- set max_used to NULL hence
        if @ident_incr > 0
            insert dbo.MSmerge_identity_range(subid, artid, range_begin, range_end, is_pub_range, max_used)
                values(@pubid, @artid, @max_used, @max_range, 1, NULL)
        else
            insert dbo.MSmerge_identity_range(subid, artid, range_begin, range_end, is_pub_range, max_used)
                values(@pubid, @artid, @max_used, @min_range, 1, NULL)
        if @@error <> 0
            goto FAILURE
    end -- end identity range code
    
    -- need to validate well-partitioned articles even if this particular one
    -- may not be well-partitioned (e.g. with the same article already existing as well-partitioned
    -- in a different publication or subscription).    
    exec @retcode = sys.sp_MSvalidate_wellpartitioned_articles @publication
    if @@error <> 0 or @retcode <> 0
        goto FAILURE
    
    exec @retcode = sys.sp_MScreate_article_repl_view @pubid, @artid
    if @retcode <> 0 or @@error <> 0
        goto FAILURE    

    exec %%Relation(ID = @objid).SetMergePublished(Value = 1 , SetColumns=0)
    if @@ERROR <> 0
        goto FAILURE

    /* set up the article's gen-cur */
    set @genguid = newid()
    set @dt = getdate()

    exec @retcode= sys.sp_MSgetreplnick @replnick = @replnick out
    if @retcode<>0 or @@error<>0 
        goto FAILURE

    /*
    ** If there are no zero generation tombstones or rows, add a dummy row in there. 
    */
    if not exists (select * from dbo.MSmerge_genhistory)
    begin
        begin tran

        set identity_insert dbo.MSmerge_genhistory on

        insert into dbo.MSmerge_genhistory (guidsrc, genstatus, generation, art_nick, nicknames, coldate) values
            (@genguid, 1, 1, 0, @replnick + 0xFF, @dt)
        if (@@error <> 0)
            goto FAILURE
            
        set identity_insert dbo.MSmerge_genhistory off

        commit tran
    end

    -- If the article status is active then prepare the article for merge replication
    -- For now, the rowguid column will be added at this time. This is needed until the merge partition group functionality has
    -- a way to defer the initial work done to set up the publication. 
    if @status = 'active' -- or exists (select * from dbo.sysmergepublications where pubid = @pubid and use_partition_groups = 1)
    begin
        /* Get a holdlock on the underlying table */
        select @cmd = 'select * into #tab1 from '
        select @cmd = @cmd + @qualified_name 
        select @cmd = @cmd + 'with (TABLOCK HOLDLOCK) where 1 = 2 '
        execute(@cmd)

        /* Add the guid column to the user table */
        execute @retcode = sys.sp_MSaddguidcolumn @source_owner, @source_object
        if @@ERROR <> 0 OR    @retcode <> 0  -- NOTE: new change
            goto FAILURE

        /* Create an index on the rowguid column in the user table */
        execute @retcode = sys.sp_MSaddguidindex @publication, @source_owner, @source_object
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE

        /* Create the merge triggers on the base table */
        execute @retcode = sys.sp_MSaddmergetriggers @qualified_name, NULL, @column_tracking_id
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE 

        /* Create the merge insert/update stored procedures for the base table */
        execute @retcode = sys.sp_MSsetartprocs @publication, @article, 0, @pubid
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE

        /* Set the article status to be active so that Snapshot does not do this again */
        select @statusid = 2 /* Active article */
        update dbo.sysmergearticles set status = @statusid where artid = @artid
        if @@ERROR <> 0 
            goto FAILURE
    end

DONE_TRAN:                

    if @needs_pickup=1
    begin
        declare @needs_pick_value int 
        select @needs_pick_value=5 --new_inactive status
        update dbo.sysmergearticles set status=@needs_pick_value where artid = @artid and pubid=@pubid
        if @@ERROR<>0
            goto FAILURE
    end

    /*
    ** Set all bits to '1' in the columns column to include all columns.
    */
    IF @ver_partition = 0 --meanning no vertical partition needed.
    BEGIN
        -- Indicate that this is an internal caller of sp_mergearticlecolumn
        EXEC @retcode = sys.sp_MSsetcontext_internalcaller @onoff=1
        IF @@ERROR <> 0 or @retcode <> 0
            goto FAILURE

        EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication=@publication, @article=@article, @schema_replication='true'              
        IF @@ERROR <> 0 OR @retcode <> 0
        BEGIN
            RAISERROR(21198, 16, -1)
            goto FAILURE
        END

        -- Turn off indication that this is an internal caller of sp_mergearticlecolumn
        EXEC @retcode = sys.sp_MSsetcontext_internalcaller @onoff=0
        IF @@ERROR <> 0 or @retcode <> 0
            goto FAILURE

        -- check if table has filestream column
        if exists ( select * from sys.columns where object_id = @objid and is_filestream =1 )
            select @filestream_col_published = 1
    END

    /*
    **    Set all bits to '1' for all columns in the primary key.
    */
    ELSE
    BEGIN

        -- varbinary(max) filestream column cannot be a part of PK
        select @filestream_col_published = 0

        SELECT @indid = index_id FROM sys.indexes WHERE object_id = @objid AND is_primary_key <> 0    /* PK index */
        /*
        **  First we'll figure out what the keys are.
        */
        SELECT @i = 1
        WHILE (@i <= 16)
        BEGIN
            SELECT @pkkey = INDEX_COL(@qualified_name, @indid, @i)
            if @pkkey is NULL
                break
            EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication, @article, @pkkey, 'add'
            IF @@ERROR <> 0 OR @retcode <> 0
            BEGIN
                RAISERROR(21198, 16, -1)
                goto FAILURE
            END            
            select @i = @i + 1
        END
        /*
        ** make sure any existing rowguidcol is in the partition. We can not live without it.
        */
        select @colname=NULL
        select @colname = name from sys.columns where object_id = @objid 
            and is_rowguidcol = 1
        if @colname is not NULL
        BEGIN
            EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication, @article, @colname, 'add'
            if @@error<>0 or @retcode<>0
                goto FAILURE
        END

        /*
        ** If autoidentitymanagement make sure any existing rowguidcol is in the partition. We can not live without it.
        */
        if @identity_support = 1
        BEGIN
            select @colname=NULL
            select @colname = name from sys.columns where object_id = @objid 
                and is_identity = 1
            if @colname is not NULL
            BEGIN
                EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication, @article, @colname, 'add'
                if @@error<>0 or @retcode<>0
                    goto FAILURE
            END
        END

        -- update the sysmergearticles entry to say that we are using vertical partitioning.
        update dbo.sysmergearticles set vertical_partition=1 where pubid=@pubid and artid=@artid
    END

    declare @schema_option_filestream int
    select @schema_option_lodword = sys.fn_replgetbinary8lodword(@schema_option)
    select @schema_option_hidword = sys.fn_replgetbinary8hidword(@schema_option)
    select @schema_option_max_to_nonmax = 0x20000000 -- this has to be on  for < 90RTM
    select @schema_option_filestream = 0x00000001
   
    -- varbinary(max) column with filestream attribute cannot be converted to base type (image)
    -- irrespective of whether filestream is repl. as filestream or varbinary(max)
    if ( @filestream_col_published = 1 and (@schema_option_lodword & @schema_option_max_to_nonmax = @schema_option_max_to_nonmax))
    begin
        RAISERROR(22583, 16, -1, @article, @publication)
        goto FAILURE    
    end

    -- if compat level is Yukon and turn OFF the schema_option to replicate filestream attribute, if ON
    if ( @filestream_col_published = 1 and @compatlevel >= 90 and @compatlevel < 100 and  
    (@schema_option_hidword & @schema_option_filestream = @schema_option_filestream ))
    begin
        declare @schema_option_strg sysname
        select @schema_option_lodword = sys.fn_replgetbinary8lodword(@schema_option)
        select @schema_option_hidword = @schema_option_hidword & ~( @schema_option_filestream)
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        UPDATE dbo.sysmergearticles SET schema_option = @schema_option WHERE artid = @artid AND pubid = @pubid
        select @schema_option_strg = sys.fn_varbintohexstr(@schema_option)
        RAISERROR (22584, 10, -1, @schema_option_strg)
    end


    exec @retcode = sys.sp_MSfillupmissingcols @publication, @qualified_name
    if @retcode<>0 or @@ERROR<>0
        goto FAILURE

    /*
    ** For articles with subset filter clause - set the pub type to subset
    */
    if len(@subset_filterclause) > 0
    begin
        execute @retcode = sys.sp_MSsubsetpublication @publication
        if @@ERROR <> 0 or @retcode<>0
            goto FAILURE
    end                        

    -- set up deleted col info
    declare @deleted_cols varbinary(128)
    execute sp_MSfillup_deleted_cols @objid, @deleted_cols output
    update dbo.sysmergearticles set deleted_cols=@deleted_cols 
        where artid = @artid and pubid=@pubid

    if @bump_to_80=1
    begin
        exec @retcode = sys.sp_MSBumpupCompLevel @pubid, 40
        if @@ERROR<>0 or @retcode<>0
            goto FAILURE
    end
    
    exec sys.sp_MSreleasemergeadminapplock @lockowner = N'Transaction'
    COMMIT TRAN 

    return (0)

FAILURE:
    RAISERROR (20009, 16, -1, @article, @publication)
    if @@TRANCOUNT > 0
    begin
        if @got_merge_admin_applock=1
            exec sys.sp_MSreleasemergeadminapplock @lockowner = N'Transaction'
        ROLLBACK TRANSACTION sp_addmergearticle
        COMMIT TRANSACTION
    end
    return (1)


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_addmergearticle
    @publication            sysname,                            /* publication name */
    @article                sysname,                            /* article name */
    @source_object          sysname,                            /* source object name */
    @type                   sysname = 'table',                  /* article type */
    @description            nvarchar(255)= NULL,                /* article description */
    @column_tracking        nvarchar(10) = 'false',             /* column level tracking */
    @status                 nvarchar(10) = 'unsynced',          /* unsynced, active */
    @pre_creation_cmd       nvarchar(10) = 'drop',              /* 'none', 'drop', 'delete', 'truncate' */
    @creation_script        nvarchar(255)= NULL,                /* article schema script */
    @schema_option          varbinary(8)   = NULL,              /* article schema creation options */
    @subset_filterclause    nvarchar(1000) = '',                /* filter clause */
    @article_resolver       nvarchar(255)= NULL,                /* custom resolver for article */
    @resolver_info          nvarchar(517) = NULL,               /* custom resolver info */
    @source_owner           sysname = NULL,
    @destination_owner      sysname = NULL,
    @vertical_partition     nvarchar(5) = 'FALSE',             /* vertical partitioning or not */
    @auto_identity_range    nvarchar(5) = NULL,                /* this parameter is deprecated. use @identityrangemanagementoption */
    @pub_identity_range     bigint    = NULL,
    @identity_range         bigint = NULL,
    @threshold              int = NULL,
    @verify_resolver_signature     int = 1,                    /* 0=do not verify signature, 1=verify that signature is from trusted source, more values may be added later */
    @destination_object            sysname = @source_object,
    @allow_interactive_resolver    nvarchar(5) = 'false',        /* whether article allows interactive resolution or not */
    @fast_multicol_updateproc      nvarchar(5) = 'true',        /* whether update proc should update multiple columns in one update statement or not. if 0, then separate update issued for each column changed. */
    @check_permissions         int = 0, /* bitmap where 0x00 for nochecks, 0x01 for insert check, 0x2 for update check, 0x4 for delete check */
    @force_invalidate_snapshot bit = 0, /* Force invalidate existing snapshot */
    @published_in_tran_pub     nvarchar(5) = 'false', /* Indicates that this article could be published in a transactional publication as well */
    @force_reinit_subscription bit = 0, /* Force reinit subscription */
    @logical_record_level_conflict_detection nvarchar(5) = 'false',
    @logical_record_level_conflict_resolution nvarchar(5) = 'false',
    @partition_options tinyint = 0, -- 0, 1, 2 or 3 meaning none, no out of partition dml, partition based and subscription based
    @processing_order int = 0,
    @subscriber_upload_options tinyint = 0, -- possible values are 0, 1, and 2 meaning 'allow uploads', 'disables uplods', 'disable uploads and prohibit subscriber changes'
    @identityrangemanagementoption nvarchar(10) = NULL, -- NONE, MANUAL, AUTO
    @delete_tracking    nvarchar(5) = 'true',    --'true' = replicate deletes as usual.  false'= do not track deletes in triggers..
    @compensate_for_errors    nvarchar(5) = 'false',
    @stream_blob_columns nvarchar(5) = 'false'    -- 'true'= use blob optimization, 'false' = disable blob optimization.
    AS

    set nocount on

    declare @max_range              numeric(38,0)
    declare @min_range              numeric(38,0)
    declare @max_used               numeric(38,0)
    declare @ident_incr             numeric(38,0)
    declare @publisher              sysname
    declare @publisher_db           sysname
    declare @already_published      bit
    declare @identity_so_far        bigint
    declare @ver_partition          int
    declare @sp_resolver            sysname
    declare @num_columns            smallint
    declare @pubid                  uniqueidentifier                /* Publication id */
    declare @db                     sysname
    declare @identity_support       int
    declare @object                 sysname
    declare @owner                  sysname
    declare @retcode                int
    declare @objid                  int
    declare @sync_objid             int
    declare @index_objid            int
    declare @typeid                 smallint
    declare @replnick               binary(6)
    declare @merge_pub_object_bit   int
    declare @column_tracking_id     int
    declare @cmd                    nvarchar(2000)
    declare @statusid               tinyint --1: inactive; 2: active; 5:new_inactive 6:new_active
    declare @next_seed              bigint
    declare @precmdid               int
    declare @resolver_clsid         nvarchar(50)
    declare @resolver_clsid_old     nvarchar(50)
    declare @tablenick              int
    declare @artid                  uniqueidentifier
    declare @i                      int
    declare @max_identity           bigint
    declare @colname                sysname
    declare @idxname                sysname
    declare @indid                  int
    declare @pkkey                  sysname
    declare @dbname                 sysname
    declare @db_name                sysname
    declare @subset                 int
    declare @is_publisher           int
    declare @row_size               int
    declare @sp_name                sysname
    declare @sp_owner               sysname
    declare @qualified_name         nvarchar(517)
    declare @snapshot_ready         tinyint
    declare @sync_mode              tinyint
    declare @allow_interactive_bit  bit
    declare @fast_multicol_updateproc_bit bit
    declare @additive_resolver       sysname
    declare @average_resolver        sysname
    declare @mindate_resolver        sysname
    declare @needs_pickup            bit
    declare @maxdate_resolver        sysname
    declare @minimum_resolver        sysname
    declare @maximum_resolver        sysname
    declare @mergetxt_resolver       sysname
    declare @pricolumn_resolver      sysname
    declare @xtype                   int
    declare @xprec                   int
    declare @bump_to_80              bit
    declare @gen                     bigint
    declare @genguid                 uniqueidentifier
    declare @dt                      datetime
    declare @qualname                nvarchar(517)
    declare @compatlevel             int
    declare @allow_partition_realignment bit
    declare @logical_record_level_conflict_detection_id bit
    declare @logical_record_level_conflict_resolution_id bit
    declare @published_in_tran_pub_bit bit
    declare @allow_anonymous bit
    declare @compensateforerrors bit
    declare @deletetracking bit
    declare @reinit_subscriptions bit
    declare @article_has_dynamic_filters bit
    declare @publication_has_dynamic_filters bit
    declare @REPOLEVersion_80SP3 int
    declare @stream_blob_columns_bit bit
    declare @missing_col_count       int
    declare @missing_cols            varbinary(128)
    declare @pubname_lessthan90compat sysname
    declare @preserve_rowguidcol bit
    declare @automatic_reinitialization_policy bit
    declare @use_partition_groups smallint
    declare @pub_number smallint
    declare @functions_in_subset_filter nvarchar(500)
    declare @dynamic_filters_function_list nvarchar(500)
    declare @got_merge_admin_applock bit
            	,@obj_name sysname
    declare @filestream_col_published int
    declare @has_filestream int
    declare @fFileTable bit

    select @got_merge_admin_applock = 0
    select @filestream_col_published = 0
    select @has_filestream = 0

    -- Security Check
    exec @retcode= sys.sp_MSreplcheck_publish
    if @@error <> 0 or @retcode <> 0 return (1)

    /* make sure current database is enabled for merge replication */
    exec @retcode=sys.sp_MSCheckmergereplication
    if @@ERROR<>0 or @retcode<>0
        return (1)

    /*
    ** Initializations 
    */
    set @REPOLEVersion_80SP3= 60
    select @is_publisher = 0
    select @needs_pickup = 0
    select @bump_to_80 = 0
    select @already_published = 0
    select @publisher = publishingservername()
    select @publisher_db = db_name()
    select @max_identity    = NULL
    select @next_seed        = NULL
    select @statusid        = 0
    select @resolver_clsid    = NULL
    select @subset            = 1        /* Const: publication type 'subset' */
    select @merge_pub_object_bit    = 128
    select @db_name = db_name()
    select @additive_resolver   = formatmessage(21701)
    select @average_resolver    = formatmessage(21702)
    select @mindate_resolver    = formatmessage(21703)
    select @maxdate_resolver    = formatmessage(21704)
    select @minimum_resolver    = formatmessage(21706)
    select @mergetxt_resolver   = formatmessage(21707)
    select @maximum_resolver    = formatmessage(21708)
    select @pricolumn_resolver  = formatmessage(21709)
    select @sp_resolver         = formatmessage(21712)
    select @reinit_subscriptions = 0
    select @article_has_dynamic_filters = 0
    select @publication_has_dynamic_filters = 0
    select @pubname_lessthan90compat = NULL

    if @subscriber_upload_options not in (0, 1, 2)
    begin
        raiserror (22542, 16, -1)
        return (1)
    end

    if 'false' = lower(@compensate_for_errors collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @compensateforerrors= 0
    end
    else if 'true' = lower(@compensate_for_errors collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @compensateforerrors= 1
    end
    else
    begin
        raiserror (14148, 16, -1, '@compensate_for_errors')
        return (1)
    end

    if 'false' = lower(@delete_tracking collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @deletetracking= 0
    end
    else if 'true' = lower(@delete_tracking collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @deletetracking= 1
    end
    else
    begin
        raiserror (14148, 16, -1, '@delete_tracking')
        return (1)
    end

    if 'false' = lower(@stream_blob_columns collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @stream_blob_columns_bit= 0
    end
    else if 'true' = lower(@stream_blob_columns collate SQL_Latin1_General_CP1_CS_AS)
    begin
        set @stream_blob_columns_bit= 1
    end
    else
    begin
        raiserror (14148, 16, -1, '@stream_blob_columns')
        return (1)
    end

    if @source_owner is NULL
    begin
        select @source_owner = SCHEMA_NAME(schema_id) from sys.objects where object_id = object_id(QUOTENAME(@source_object))
        if @source_owner is NULL  
        begin
            raiserror (14027, 11, -1, @source_object)
            return (1)
        end
    end

    select @qualified_name = QUOTENAME(@source_owner) + '.' + QUOTENAME(@source_object)

    /*
    **    Get the id of the @qualified_name
    */
    select @objid = OBJECT_ID(@qualified_name)
    if @objid is NULL
    begin
        raiserror (14027, 11, -1, @qualified_name)
        return (1)
    end
    
    -- If the article that we are trying to add is a natively compiled stored proc
    -- raise error.
    if (ObjectProperty(@objid, 'ExecIsWithNativeCompilation') <> 0)
    begin
        raiserror (12336, 16, 1, 35473)
        return @@ERROR
    end
    
    -- If the article that we are trying to add is an in-memory table
    -- raise error.
    if (ObjectProperty(@objid, 'TableIsMemoryOptimized') <> 0)
    begin
        raiserror (12336, 16, 1, 35472)
        return @@ERROR
    end

    -- If the article that we are trying to add is a system-versioned temporal table or history table
    -- raise error.
    if exists (select 1 from sys.tables where object_id = @objid and temporal_type in (1, 2))
    begin
        raiserror (13570, 16, 1, @qualified_name)
        return @@ERROR
    end
	
	-- If the article that we are trying to add is a graph node or edge table raise error.
    if exists (select 1 from sys.tables where object_id = @objid and (is_node = 1 or is_edge = 1))
    begin
		raiserror (13926, 16, 2, @source_object)
		return @@ERROR
    end

    -- If the article that we are trying to add is a stretched table
    -- raise error.
    if exists (select 1 from sys.tables where object_id = @objid and is_remote_data_archive_enabled = 1)
    begin
        raiserror (14915, 16, 2, @qualified_name)
        return @@ERROR
    end

    -- If the article that we are trying to add is a ledger table or history table
    -- raise error.
    if exists (select 1 from sys.tables where object_id = @objid and ledger_type != 0)
    begin
        raiserror (37443, 16, 2, @qualified_name)
        return @@ERROR
    end

    -- check if the object is marked as ms shipped. If so it cannot be published
    if exists (select 1 from sys.objects where object_id = @objid and is_ms_shipped=1)
    begin
        raiserror (20696, 16, -1, @qualified_name)
        return (1)
    end
    
    if @destination_owner is NULL
        select @destination_owner='dbo'

    /*
    ** Pad out the specified schema option to the left
    */
    select @schema_option = fn_replprepadbinary8(@schema_option)

    /*
    ** Parameter Check: @publication.
    ** The @publication id cannot be NULL and must conform to the rules
    ** for identifiers.
    */     
        
    if @publication is NULL
    begin
        raiserror (14043, 16, -1, '@publication', 'sp_addmergearticle')
        return (1)
    end

    select @pubid = pubid, 
           @snapshot_ready = snapshot_ready, 
           @sync_mode=sync_mode, 
           @compatlevel=backward_comp_level, 
           @allow_anonymous = allow_anonymous,
           @use_partition_groups = use_partition_groups,
           @pub_number = publication_number,
           @publication_has_dynamic_filters = dynamic_filters,
           @allow_partition_realignment = allow_partition_realignment,
           @automatic_reinitialization_policy = automatic_reinitialization_policy,
           @dynamic_filters_function_list = dynamic_filters_function_list
    from dbo.sysmergepublications 
    where name = @publication and UPPER(publisher) collate database_default = UPPER(@publisher) collate database_default and publisher_db=@publisher_db
    if @pubid is NULL
    begin
        raiserror (20026, 16, -1, @publication)
        return (1)
    end

    if lower(@article)='all'
    begin
        raiserror(21401, 16, -1)
        return (1)
    end

    if  (0=@allow_partition_realignment and 0=@subscriber_upload_options)
    begin
        raiserror(22543, 16, -1)
        return (1)
    end

    -- Compensate for errors can be turned on only when upload options allows subscriber uploads.
    if 1=@compensateforerrors and (1=@subscriber_upload_options or 2=@subscriber_upload_options)
    begin
        raiserror(20022, 10, -1)
    end

    -- Parameter check @subset_filterclause
    if @subset_filterclause <> '' and @subset_filterclause is not NULL
    begin
        /* check the validity of subset_filterclause */
        exec ('declare @test int select @test=1 from ' + @qualified_name + ' where (1=2) and ' + @subset_filterclause)
        if @@ERROR<>0
        begin
            raiserror(21256, 16, -1, @subset_filterclause, @article)
            return (1)
        end

        -- check if the subsetfilter clause contains a computed column. To do this get a list of computed columns
        -- for the given article. Then check if the filter name is like the computed column
        declare @computedcolname sysname
        
        declare compted_columns_cursor cursor LOCAL FAST_FORWARD
        for (select name from sys.columns where object_id = @objid and is_computed=1)
        open compted_columns_cursor
        fetch compted_columns_cursor into @computedcolname
        while (@@fetch_status <> -1)
        begin
            
            if sys.fn_MSisfilteredcolumn(@subset_filterclause, @computedcolname, @objid) = 1 
            begin
                raiserror(20656, 16, -1)
                return (1)
            end
            fetch compted_columns_cursor into @computedcolname
        end
        close compted_columns_cursor
        deallocate compted_columns_cursor


        -- check if the subsetfilter clause contains any column of type that is not supported in
        --  a subset filter.

        if exists    (
                        select * from sys.columns 
                        where object_id = @objid and 
                            (
                            --(sys.fn_IsTypeBlob(sc.system_type_id,sc.max_length) = 1) -- Blob type text,ntext,xml
                              (system_type_id in (type_id('image'), type_id('text'), type_id('ntext'), type_id('xml')))
                              or max_length = -1
                              or system_type_id = 240    -- CLR-UDTs
                            )
                        and 
                        sys.fn_MSisfilteredcolumn(@subset_filterclause, name, @objid) = 1 
                    )
        begin
            raiserror(22518, 16, -1, @qualified_name)
            return (1)
        end

    end
      
    /*
    ** Parameter Check: @type
    ** If the article is added as a 'indexed view schema only' article,
    ** make sure that the source object is a schema-bound view.
    ** Conversely, a schema-bound view cannot be published as a 
    ** 'view schema only' article.
    */
    select @type = lower(@type collate SQL_Latin1_General_CP1_CS_AS)

    if @type = N'indexed view schema only' and objectproperty(object_id(@qualified_name), 'IsSchemaBound') <> 1
    begin
        raiserror (21277, 11, -1, @qualified_name)          
        return (1)      
    end
    else if @type = N'view schema only' and objectproperty(object_id(@qualified_name), 'IsSchemaBound') = 1
    begin
        raiserror (21275, 11, -1, @qualified_name)
        return (1)
    end

    /*
    ** Only publisher can call sp_addmergearticle
    */
    EXEC @retcode = sys.sp_MScheckatpublisher @pubid
    IF @@ERROR <> 0 or @retcode <>    0
    BEGIN
        RAISERROR (20073, 16, -1)
        RETURN (1)
    END
    
    /*
    ** Parameter Check: @article.
    ** Check to see that the @article is local, that it conforms
    ** to the rules for identifiers, and that it is a table, and not
    ** a view or another database object.
    */

    exec @retcode = sys.sp_MSreplcheck_name @article, '@article', 'sp_addmergearticle'
    if @@ERROR <> 0 or @retcode <> 0
        return(1)
        

    /*
    ** Set the precmdid.  The default type is 'drop'.
    **
    **        @precmdid    pre_creation_cmd
    **        =========    ================
    **              0        none
    **              1        drop
    **              2        delete
    **              3        truncate
    */
    IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('none', 'drop', 'delete', 'truncate')
    BEGIN
      RAISERROR (14061, 16, -1)
      RETURN (1)
    END

    /*
    ** Determine the integer value for the pre_creation_cmd.
    */
    IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'none'
       select @precmdid = 0
    ELSE IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'drop'
       select @precmdid = 1
    ELSE IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'delete'
       select @precmdid = 2
    ELSE IF LOWER(@pre_creation_cmd collate SQL_Latin1_General_CP1_CS_AS) = 'truncate'
       select @precmdid = 3


    /*
    ** Set the typeid.    The default type is table.    It can 
    ** be one of following.
    **
    **        @typeid        type
    **        =======        ========
    **           0xa        table
    **          0x20        proc schema only
    **          0x40        view schema only
    **          0x80        func schema only
    **          0x40        indexed view schema only (overloaded)
    **          0xA0        synonym schema only    
    */          

    IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('table', 'proc schema only', 'view schema only', 'func schema only', 'indexed view schema only', 'synonym schema only')
       BEGIN
            RAISERROR (21276, 16, -1)
            RETURN (1)
       END

    IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'table'
    BEGIN
       SET @typeid = 0x0a
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'proc schema only'
    BEGIN
       SET @typeid = 0x20 
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'view schema only'
    BEGIN
       SET @typeid = 0x40
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'indexed view schema only'
    BEGIN
       SET @typeid = 0x40
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'func schema only'
    BEGIN
       SET @typeid = 0x80
    END
    ELSE IF LOWER(@type collate SQL_Latin1_General_CP1_CS_AS) = N'synonym schema only'
    BEGIN
       SET @typeid = 0xA0
    END


    select @sync_objid = OBJECT_ID(@qualified_name)
    if @sync_objid is NULL
        begin
            raiserror (14027, 11, -1, @qualified_name)
            return (1)
        end


    if @typeid in (0x20,0x40,0x80, 0xA0)
    begin
        if exists (select * from syscomments
                    where id = @sync_objid
                      and encrypted = 1)
        begin
            raiserror(21004, 16, -1, @source_object)
            return 1
        end
    end

    /*
    ** Parameter Check:     @article, @publication.
    ** Check if the article already exists in this publication.
    */

    IF EXISTS (SELECT *
                FROM dbo.sysmergeextendedarticlesview
                WHERE pubid = @pubid
                  AND name = @article)
        BEGIN
            raiserror (21292, 16, -1, @article)
            RETURN (1)
        END

    --if @compatlevel < 90 and @processing_order <> 0
    --begin
    --    raiserror(21585, 16, -1, @publication)
        --return 1
    --end
        
    /*
    ** At this point, all common parameter validations 
    ** for table and schema only articles have been 
    ** performed, so branch out here to handle schema
    ** only articles as a special case.
    */

    IF @typeid in (0x20, 0x40, 0x80, 0xA0)
    BEGIN
        IF @destination_object IS NULL OR @destination_object = N''
        BEGIN
            SELECT @destination_object = @source_object
        END

        IF @schema_option IS NULL
        BEGIN
            SELECT @schema_option = 0x0000000000000001
        END
        EXEC @retcode = sys.sp_MSaddmergeschemaarticle 
                @pubid = @pubid,
                @article = @article,
                @source_object = @source_object,
                @type = @typeid,
                @description = @description,
                @status = @status,
                @pre_creation_command = @precmdid,
                @creation_script = @creation_script,
                @source_owner = @source_owner,
                @destination_owner = @destination_owner,
                @schema_option = @schema_option,
                @destination_object = @destination_object,
                @qualified_name = @qualified_name,     
                @publication = @publication,
                @snapshot_ready = @snapshot_ready,
                @force_invalidate_snapshot = @force_invalidate_snapshot,
                @processing_order = @processing_order

       RETURN (@retcode)
    END

    /*
    ** Make sure that the table name specified is a table.
    */

    if OBJECT_ID(@qualified_name, 'U') is NULL
    begin
        raiserror (20074, 16, -1)
        return (1)
    end

    /*
    ** Don't allow filetables to be added as articles
    */
    SELECT @fFileTable = is_filetable from sys.tables where object_id = @objid
    If (@fFileTable = 1)
    BEGIN
        RAISERROR (33435, 16, -1, @qualified_name)
        return (1)
    END

    /*
        Refer to the Yukon_Merge_Feature_Reference_Tables.doc for
        the detailed schema_option table.
    */
    IF @schema_option IS NULL
    BEGIN
        IF @compatlevel < 90
        BEGIN
            IF @sync_mode = 0   -- native
            BEGIN
    	        SELECT @schema_option = 0x0000000030034FF1
            END
            ELSE                -- character (SSCE)
            BEGIN
    	        SELECT @schema_option = 0x0000000030034FF1
            END
        END
        ELSE    -- 90 compatlevel
        BEGIN
            IF @sync_mode = 0   -- native
            BEGIN
    	        SELECT @schema_option = 0x000000000C034FD1
            END
            ELSE                -- character (SSCE)
            BEGIN
    	        SELECT @schema_option = 0x0000000008034FF1
            END
        END
    END

    /*
       Verify that the schema option being set is compatible with
       publication compatibility level.
    */
    -- Since only the lower 32 bits of @schema_option are 
    -- used, the following check is sufficient. Note that @schema_option is
    -- already padded out to the left at the beginning of this procedure.
    -- whenever anything here is changed also change sp_MSmap_subscriber_type
    declare @schema_option_lodword int
    declare @schema_option_hidword int
    declare @schema_option_xml_to_ntext int
    declare @schema_option_max_to_nonmax int
    declare @schema_option_create_schema int
    declare @schema_option_xml_indexes int
    declare @schema_option_katmaidatetime_to_string int
    declare @schema_option_hierarchyid_to_varbinarymax int
    declare @schema_option_largeUDT_to_varbinarymax int
    declare @schema_option_spatial_indexes int
    declare @schema_option_spatial_to_varbinarymax int
    declare @schema_option_udt_to_base_types int
    declare @schema_option_filtered_indexes int
    declare @schema_option_filestream_attribute int
    declare @schema_option_str sysname

    declare @schema_option_compression int
    select @schema_option_lodword = sys.fn_replgetbinary8lodword(@schema_option)
    select @schema_option_hidword = sys.fn_replgetbinary8hidword(@schema_option)
    select @schema_option_xml_to_ntext  = 0x10000000 -- this has to be on  for < 90RTM
    select @schema_option_max_to_nonmax = 0x20000000 -- this has to be on  for < 90RTM
    select @schema_option_create_schema = 0x08000000 -- this has to be off for < 90RTM
    select @schema_option_xml_indexes   = 0x04000000 -- this has to be off for < 90RTM
    select @schema_option_katmaidatetime_to_string = 0x00000002 

    select @schema_option_hierarchyid_to_varbinarymax = 0x00000020 -- this has to be on for < 100RTM    
    select @schema_option_largeUDT_to_varbinarymax = 0x00000010 -- this has to be on for < 100RTM    
    select @schema_option_spatial_indexes = 0x00000100 -- this has to be off for < 100RTM
    select @schema_option_spatial_to_varbinarymax = 0x00000080 -- this has to be on for < 100RTM
    select @schema_option_udt_to_base_types = 0x00000020
    select @schema_option_filtered_indexes = 0x00000040 -- this has to be off for < 100RTM    
    select @schema_option_filestream_attribute = 0x00000001  
    select @schema_option_compression   = 0x00000004 -- this has to be off for < 100RTM and SSCE
    select @schema_option_filtered_indexes = 0x00000040 -- this has to be off for < 100RTM    

    declare @schema_option_has_changed bit;
    select @schema_option_has_changed = 0;  

 
    /*
    ** If compatlevel is less than 100RTM then make sure the schema option to map down
    ** 100 datatypes to equivalent downlevel datatypes is set. 
    ** * Namely, map large UDT down to varbinary(max)
    ** * If spatial_indexes are set for replication, disable if compatlevel < 100
    ** * Map spatial types to varbinary(max)
    ** * Make sure the schema option to map down 100 datetime types to downlevel data types.
    ** * Disable compression if it is enabled
    ** * Map hierarchyid to varbinary(max)
    */
    if (@compatlevel < 100 and 
       ((@schema_option_hidword & @schema_option_largeUDT_to_varbinarymax = 0) or
       (@schema_option_hidword & @schema_option_compression <> 0) or
       (@schema_option_hidword & @schema_option_hierarchyid_to_varbinarymax = 0) or
       (@schema_option_hidword & @schema_option_spatial_to_varbinarymax = 0) or
       (@schema_option_hidword & @schema_option_spatial_indexes <> 0) or
       (@schema_option_hidword & @schema_option_filtered_indexes <> 0) or
       (@schema_option_hidword & @schema_option_katmaidatetime_to_string = 0)))
    begin
        select @schema_option_hidword = @schema_option_hidword | @schema_option_largeUDT_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_compression)
        select @schema_option_hidword = @schema_option_hidword | @schema_option_hierarchyid_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword | @schema_option_spatial_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_spatial_indexes)
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_filtered_indexes)
        select @schema_option_hidword = @schema_option_hidword | @schema_option_katmaidatetime_to_string

        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;
    end


    /*
    ** For hierarchy we will map it to varbinarymax for 100 RTM compatibility level in character sync mode (for SSCE subscriber).
    ** Turn off the data compression for SSCE subscriber
    */
    if(@compatlevel = 100 and
	 @sync_mode <>0 and
	((@schema_option_hidword & @schema_option_hierarchyid_to_varbinarymax = 0) or
	(@schema_option_hidword & @schema_option_compression <> 0) ))
    begin
    	select @schema_option_hidword = @schema_option_hidword | @schema_option_hierarchyid_to_varbinarymax
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_compression)
        
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)        
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;    	      
    end
    

    /*
    ** If a spatial type is being mapped to non spatial type on the subscriber,
    ** ensure that the spatial index schema option is disabled.
    */
    if (@schema_option_hidword & @schema_option_spatial_indexes <> 0 and
       ((@schema_option_hidword & @schema_option_spatial_to_varbinarymax <> 0) or
       (@schema_option_lodword & @schema_option_udt_to_base_types <> 0)))
    begin
        select @schema_option_hidword = @schema_option_hidword & ~(@schema_option_spatial_indexes)
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;
    end    

    /*
    ** If compatlevel is less than 90RTM then make sure the schema option to map down
    ** 90 datatypes to equivalent downlevel datatypes is set.
    */
    if (@compatlevel < 90 and 
        (((@schema_option_lodword & (@schema_option_xml_to_ntext | @schema_option_max_to_nonmax)) <> 
        (@schema_option_xml_to_ntext | @schema_option_max_to_nonmax)) or
        ((@schema_option_lodword & (@schema_option_create_schema | @schema_option_xml_indexes)) <> 0)))
    begin
        select @schema_option_lodword = @schema_option_lodword | @schema_option_xml_to_ntext | @schema_option_max_to_nonmax
        select @schema_option_lodword = @schema_option_lodword & ~(@schema_option_create_schema | @schema_option_xml_indexes)
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        select @schema_option_str = sys.fn_varbintohexstr(@schema_option)
        select @schema_option_has_changed = 1;
    end

   
    /*      
    ** If we changed what the user originally input, output a message telling them what
    ** the new schema_option value is.
    */
    if @schema_option_has_changed = 1    
    begin
        RAISERROR (20732, 10, -1, @schema_option_str)
    end

    /*
    ** If filestream attribute is enabled, enable stream_blob_columns since
    ** this will lead to lower memory utilization during sync.
    */
    if @schema_option_hidword & @schema_option_filestream_attribute <> 0 and
       @stream_blob_columns_bit = 0
    begin
        RAISERROR (20737, 10, -1)
        select @stream_blob_columns_bit = 1
    end

    /*
    ** If scheme option contains collation or extended properties, 
    ** bump up the compatibility-level
    */      
    declare @xprop_schema_option int
    declare @collation_schema_option int
    select @xprop_schema_option = 0x00002000
    select @collation_schema_option = 0x00001000
    if (@schema_option_lodword & @collation_schema_option) <> 0 and @compatlevel < 40
    begin     
        raiserror(21389, 10, -1, @publication)
        select @bump_to_80 = 1
    end
    if (@schema_option_lodword & @xprop_schema_option) <> 0 and @compatlevel < 40
    begin    
        raiserror(21390, 10, -1, @publication)
        select @bump_to_80 = 1
    end

    /*
    ** Merge table articles does not really support destination object. It has the same value as source
    */
    if @destination_object <> @source_object
    begin
        raiserror(20638, 10, -1)
    end

    select @destination_object = @source_object

    /*
    select @row_size=sum(max_length) from sys.columns where object_id=OBJECT_ID(@qualified_name)
    if @row_size>6000 
        begin
            RAISERROR (21062, 16, -1, @qualified_name)    
            -- RETURN (1)
        end
    */
    IF LOWER(@vertical_partition collate SQL_Latin1_General_CP1_CS_AS) = 'false'
    begin
        select @ver_partition = 0
    end
    else
    begin
        select @ver_partition = 1
    end
    select @num_columns=count(*) from sys.columns where object_id = object_id(@qualified_name)

    -- After Yukon beta 1, this will be changed to 1024.
    if @num_columns > 246 and LOWER(@vertical_partition collate SQL_Latin1_General_CP1_CS_AS) = 'false'
    begin
        RAISERROR (20068, 16, -1, @qualified_name, 246)
        RETURN (1)
    end

    -- Colvs can only handle 246 columns so we can not allow more than 246 columns to exist on a table
    -- if column tracking is being used. The reason is that we track all the columns even when vertical
    -- partitioning is used and only some of the columns are published.
    if @num_columns > 246 and LOWER(@column_tracking collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
    begin
        RAISERROR (25020, 16, -1, @qualified_name, 246)
        RETURN (1)
    end

    -- If the input param @subscriber_upload_options contradicts to the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                where objid = @objid and upload_options <> @subscriber_upload_options)
    begin
        raiserror (20053, 11, -1, 'subscriber_upload_options', @qualified_name)
        return (1)
    end

    -- If the input param @delete_tracking contradicts with the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                    where objid = @objid and delete_tracking <> @deletetracking)
    begin
        raiserror (20648, 16, -1, @qualified_name)
        return (1)
    end

    -- If the input param @stream_blob_columns contradicts with the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                    where objid = @objid and stream_blob_columns <> @stream_blob_columns_bit)
    begin
        raiserror (20053, 11, -1, 'stream_blob_columns', @qualified_name)
        return (1)
    end

    -- If the input param @compensate_for_errors contradicts to the property of an already existing
    -- article for the same base table, we raise an error.
    if exists (select top 1 artid from dbo.sysmergearticles 
                    where objid = @objid and compensate_for_errors <> @compensateforerrors)
    begin
        raiserror (20053, 11, -1, 'compensate_for_errors', @qualified_name)
        return (1)
    end

    -- Subscribers below 80SP3 build 858 will ignore the @compensate_for_errors=false setting.
    -- Raise a warning.
    if @compensateforerrors = 0 and
       @compatlevel <= @REPOLEVersion_80SP3
    begin
        raiserror(20004, 10, -1, @publication, 'compensate_for_errors', '8.00.0858')
    end

    -- articles with >=246 columns can only be added to Yukon-compatible publications
    -- if the article is about to be republished, there might already be missing/excluded columns
    --
    if @compatlevel <= 90    -- After Yukon beta 1, we will change this to " < 90".
    begin
        declare @cCols int -- number of columns in the table
        declare @cMissing int -- number of missing cols
        
        select @cCols= count(*) from sys.columns where object_id = @objid and is_computed <> 1 and system_type_id <> type_id('timestamp')
        set @cMissing= coalesce((select max(missing_col_count) from dbo.sysmergearticles where objid = @objid), 0)
        if ((@cCols + @cMissing) > 246) and LOWER(@vertical_partition collate SQL_Latin1_General_CP1_CS_AS) = 'false'
        begin
            --raiserror(21522,16,1,@article,@publication, 246)
            RAISERROR (20068, 16, -1, @qualified_name, 246)
            return (1)
        end
    end
    
    /*
    ** If current publication contains a non-sync subscription, all articles to be added in it
    ** has to contain a rowguidcol.
    */
    if exists (select * from dbo.sysmergesubscriptions where pubid = @pubid and sync_type = 2)
    begin
        if not exists (select * from sys.columns
            where object_id=@objid and is_rowguidcol = 1)
            begin
                raiserror(20086 , 16, -1, @publication)
                return (1)
            end
    end

    --
    -- Parameter Check:  @identityrangemanagementoption.
    -- We will override the value specified in @auto_identity_range if
    -- @identityrangemanagementoption is not null
    --
    if (@identityrangemanagementoption is NULL)
    begin
        --
        -- @identityrangemanagementoption is null
        -- Check @auto_identity_range
        --
        IF @auto_identity_range IS NULL 
        begin
            --
            -- user did not specify any explicit values for identity management
            --
            select @identityrangemanagementoption = 'none'
        end
        else if LOWER(@auto_identity_range collate SQL_Latin1_General_CP1_CS_AS) IN ('true', 'false')
        begin
            -- map the value of @auto_identity_range to @identityrangemanagementoption
            select @identityrangemanagementoption = case 
                when LOWER(@auto_identity_range collate SQL_Latin1_General_CP1_CS_AS) = N'true' 
                then N'auto' else N'none' end
            -- Issue warning on deprecation of this option
            raiserror (21767, 10, 1, '@auto_identity_range', '@identityrangemanagementoption')
        end
        else
        BEGIN
            -- invalid value for @auto_identity_range
            RAISERROR (14148, 16, -1, '@auto_identity_range')
            return (1)
        END
    end
    else
    begin
        --
        -- validate @identityrangemanagementoption
        --
        if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('none', 'manual', 'auto')
        BEGIN
            -- invalid value for @identityrangemanagementoption
            RAISERROR (20644, 16, -1, @identityrangemanagementoption)
            return (1)
        END
        select @identityrangemanagementoption = lower(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS)
    end

    if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) <> 'auto' and (@identity_range is not NULL or @threshold is not NULL or @pub_identity_range is not NULL)
    begin
        raiserror(21282, 16, -1)
        return (1)
    end


    if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto'
    begin
        /*
        ** If you want to have identity support, @range and threshold can not be NULL
        */
        if (@identity_range is NULL or @pub_identity_range is NULL)
        begin
            raiserror(21193, 16, -1)
            return (1)
        end

        if @compatlevel < 90 and @threshold is NULL
        begin
            raiserror(21193, 16, -1)
            return (1)
        end

        exec @retcode = sys.sp_MScheck_autoident_parameters
                                    @qualified_name,
                                    @pub_identity_range,
                                    @identity_range,
                                    @threshold

        if @retcode<>0 or @@error<>0
        begin
            raiserror(20707, 16, -1)
            return 1
        end

        select @ident_incr = IDENT_INCR(@qualified_name)
                
        select @identity_support = 1

        exec @retcode = sys.sp_MScompute_maxmin_identity @objid, @max_range output, @min_range output
        if @retcode<>0 or @@error<>0
        begin
            raiserror(20707, 16, -1)
            return 1
        end

        if @ident_incr < 0
        begin
            select @pub_identity_range = -1*@pub_identity_range
            select @identity_range = -1*@identity_range
        end
    end
    else
        select @identity_support = 0

    /*
    ** If the table contains one more columns of type bigint or sql_variant, 
    ** we bump up the backward compatibility level.
    */
    if EXISTS (SELECT * FROM sys.columns c WHERE c.object_id = @sync_objid
                AND (c.system_type_id = type_id('bigint') or c.system_type_id = type_id('sql_variant'))) and @compatlevel < 40
    begin
        raiserror(21357, 10, -1, @publication)
        select    @bump_to_80 = 1
    end

    /*
    ** 7.0 subscribers do not like data type 'timestamp'
    */
    if EXISTS (select * from sys.columns where object_id=@sync_objid and system_type_id = type_id('timestamp')) and @compatlevel < 40
    begin
        raiserror(21358, 10, -1, @publication)
        select @bump_to_80 = 1
    end
        
    /*
    ** Validate the column tracking
    */
    if @column_tracking IS NULL OR LOWER(@column_tracking collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
    BEGIN
        RAISERROR (14148, 16, -1, '@column_tracking')
        RETURN (1)
    END
    if LOWER(@column_tracking collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
        SET @column_tracking_id = 1
    else 
        SET @column_tracking_id = 0


    /*
    ** Check for partioned tables. Not supported when sync_mode is 1 (SSCE)
    */

    if @sync_mode = 1 
    begin
        -- Check if the table is partitioned.
        if exists (select * from (sys.indexes as i INNER JOIN sys.partition_schemes as ps
                                                              ON (i.data_space_id = ps.data_space_id))
                        where (i.object_id = object_id(@qualified_name)) and
                            (i.index_id IN (0,1)))  -- to ensure that we are dealing with tables
        begin
            RAISERROR (22534, 16, -1)
            RETURN (1)
        end        
    end
    
    /*
    ** Replication not supported on a table with clustered columnstore index.
    */

    -- Check if the table has clustered columnstore index.
    select TOP 1 @index_objid=object_id, @idxname=name from sys.indexes
                    where (object_id = @objid) and
                        (type = 5)
    if @index_objid is not NULL
    begin
        RAISERROR (35353, 16, -1, N'Replication', @idxname, @source_object)
        RETURN (1)
    end

    /*
    ** Parameter Check: @allow_interactive_resolver     
    */
    if LOWER(@allow_interactive_resolver collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@allow_interactive_resolver')
            RETURN (1)
        END
    if LOWER(@allow_interactive_resolver collate SQL_Latin1_General_CP1_CS_AS) = 'true'
        set @allow_interactive_bit = 1
    else 
        set @allow_interactive_bit = 0
        
    /*
    ** Parameter Check: @published_in_tran_pub     
    */
    if LOWER(@published_in_tran_pub collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@published_in_tran_pub')
            RETURN (1)
        END
    if LOWER(@published_in_tran_pub collate SQL_Latin1_General_CP1_CS_AS) = 'true'
    BEGIN
        set @published_in_tran_pub_bit = 1
    END
    else 
        set @published_in_tran_pub_bit = 0

    /*
    ** Parameter Check: @fast_multicol_updateproc  
    */
    if LOWER(@fast_multicol_updateproc collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
    BEGIN
        RAISERROR (14148, 16, -1, '@fast_multicol_updateproc')
        RETURN (1)
    END
    if LOWER(@fast_multicol_updateproc collate SQL_Latin1_General_CP1_CS_AS) = 'true'
        set @fast_multicol_updateproc_bit = 1
    else 
        set @fast_multicol_updateproc_bit = 0

    if @partition_options not in (0, 1, 2, 3)
    begin
        RAISERROR (22526, 16, -1, '@partition_options')
        RETURN (1)
    end

    if @partition_options in (1, 2, 3) and @compatlevel < 90
    begin
        raiserror(20681, 16, -1, @publication)
        return 1
    end    

    execute @retcode = sys.sp_MSgetreplnick @pubid = @pubid, @replnick = @replnick output
    if (@@error <> 0) or @retcode <> 0 or @replnick IS NULL 
    begin
        RAISERROR (14055, 11, -1)
        RETURN(1)
    end                    

    /*
    ** Validate the article resolver
    */
    if @article_resolver IS NOT NULL
    begin
        if @article_resolver = 'default' OR @article_resolver = ''
            begin
                select @article_resolver = NULL
                select @resolver_clsid = NULL
            end                    
        else
            begin
                EXECUTE @retcode = sys.sp_lookupcustomresolver @article_resolver, @resolver_clsid OUTPUT
                IF @retcode <> 0 or @resolver_clsid IS NULL
                BEGIN
                      RAISERROR (20020, 16, -1, @article_resolver)
                      RETURN (1)
                END
            end
    end

    /* 
    ** A resolver clsid of '00000000-0000-0000-0000-000000000000' indicates a .NET Assembly resolver , ensure that the 
    ** resolver_info contains the name of the class that implements the Microsoft.SqlServer.Replication.BusinessLogicSupport.BusinessLogicModule
    ** interface.    
    */
    IF @resolver_clsid = '00000000-0000-0000-0000-000000000000'
        begin
            declare @is_dotnet_assembly bit
            declare @dotnet_assembly_name nvarchar(255)
            declare @dotnet_class_name nvarchar(255)

            EXECUTE @retcode = sys.sp_lookupcustomresolver @article_resolver, @resolver_clsid OUTPUT, @is_dotnet_assembly OUTPUT, @dotnet_assembly_name OUTPUT, @dotnet_class_name OUTPUT
            if @dotnet_assembly_name IS NULL 
                begin
                    RAISERROR (21856, 16, -1, @article_resolver)
                    return (1)
                end
            if @dotnet_class_name IS NULL 
                begin
                    RAISERROR (21808, 16, -1, @article_resolver)
                    return (1)
                end
            select @article_resolver = @dotnet_assembly_name
            /* If passed in resolver_info contains a .NET class name, do not override it with the default */
            if @resolver_info is null
                select @resolver_info = @dotnet_class_name
        end


    /*
    ** If article resolver is 'SP resolver', make sure that resolver_info refers to an SP or XP;
    ** Also make sure it is stored with owner qualification
    */
    if    @article_resolver = @sp_resolver
        begin
            if not exists (select * from sys.objects where object_id = object_id(@resolver_info) and ( type = 'P' or type = 'X'))
                begin
                    raiserror(21343, 16, -1, @resolver_info)
                    return (1)
                end
                
            select @sp_name = name, @sp_owner=SCHEMA_NAME(schema_id) from sys.objects where object_id = object_id(@resolver_info)
            select @resolver_info = QUOTENAME(@sp_owner) + '.' + QUOTENAME(@sp_name) 
        end

    /* The following resolvers expect the @resolver_info to be NON NULL */
    if    @article_resolver = @sp_resolver or 
        @article_resolver = @additive_resolver or
        @article_resolver = @average_resolver or
        @article_resolver = @minimum_resolver or
        @article_resolver = @maximum_resolver or
        @article_resolver = @mindate_resolver or
        @article_resolver = @maxdate_resolver or
        @article_resolver = @mergetxt_resolver or
        @article_resolver = @pricolumn_resolver
        begin
            if @resolver_info IS NULL 
                begin
                    RAISERROR (21301, 16, -1, @article_resolver)
                    return (1)
                end
        end
    /*
    ** If article resolver uses column names, make sure that resolver_info refers to a valid column.
    */
    if    @article_resolver = @pricolumn_resolver or
        @article_resolver = @additive_resolver or
        @article_resolver = @average_resolver or
        @article_resolver = @minimum_resolver or
        @article_resolver = @maximum_resolver
        begin
            if not exists (select * from sys.columns where object_id = @objid and name=@resolver_info)
                begin
                    RAISERROR (21501, 16, -1, @article_resolver)
                    return (1)
                end
        end
    /*
    ** If article resolver is 'mindate/maxdate resolver', make sure that resolver_info refers to a column that is of datatype 'datetime' or smalldatetime
    */
    if  @article_resolver = @mindate_resolver or
        @article_resolver = @maxdate_resolver
    begin
        if not exists (select * from sys.columns where object_id = @objid and name=@resolver_info and (system_type_id=type_id('datetime') or system_type_id=type_id('smalldatetime') 
           or system_type_id=type_id('datetime2')
           or system_type_id=type_id('date')
	    or system_type_id=type_id('time')
           )) /*need to add version condition >=100*/
        begin
            RAISERROR (21302, 16, -1, @article_resolver)
            return (1)
        end
    end

    /* The following resolvers expect the article to be column tracked - warn that the default resolver will be used */
    if    @article_resolver = @additive_resolver or
        @article_resolver = @average_resolver or
        @article_resolver = @mergetxt_resolver
    begin
        if @column_tracking_id = 0
        begin
            RAISERROR (21303, 10, -1, @article, @article_resolver)
        end
    end

    if @resolver_info IS NOT NULL and @article_resolver IS NULL
    begin
        RAISERROR (21300, 10, -1, @article)
        set @resolver_info = NULL
    end

    /*
    ** Parameter Check: logical_record_level_conflict_detection
    */
    if @column_tracking IS NULL OR LOWER(@logical_record_level_conflict_detection collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@logical_record_level_conflict_detection')
            RETURN (1)
        END
    if LOWER(@logical_record_level_conflict_detection collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
        SET @logical_record_level_conflict_detection_id = 1
    else 
        SET @logical_record_level_conflict_detection_id = 0
        
    /*
    ** Parameter Check: logical_record_level_conflict_resolution
    */
    if @column_tracking IS NULL OR LOWER(@logical_record_level_conflict_resolution collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('true', 'false')
        BEGIN
            RAISERROR (14148, 16, -1, '@logical_record_level_conflict_resolution')
            RETURN (1)
        END
    if LOWER(@logical_record_level_conflict_resolution collate SQL_Latin1_General_CP1_CS_AS) = 'true' 
        SET @logical_record_level_conflict_resolution_id = 1
    else 
        SET @logical_record_level_conflict_resolution_id = 0

    if @logical_record_level_conflict_detection_id = 1 and @logical_record_level_conflict_resolution_id = 0
    begin
        raiserror (21728, 16, -1)
        return 1
    end
    
    if @logical_record_level_conflict_detection_id = 1 or @logical_record_level_conflict_resolution_id = 1
    begin
		-- Only supported with publications that have 90 compatibility level.
		select top 1 @pubname_lessthan90compat = name from dbo.sysmergepublications 
		where backward_comp_level < 90
		and (pubid = @pubid or
			 pubid in 
				(select pubid from dbo.sysmergearticles where objid = @objid))
	        
		if @pubname_lessthan90compat is not null
		begin
			raiserror(21574, 16, -1, @article, @pubname_lessthan90compat)
			return 1
		end
		
		if exists (select * from dbo.sysmergepublications where pubid = @pubid and sync_mode = 1)
		begin
			raiserror(22541, 16, -1, @publication)
			return 1
		end
		
		if exists (select * from dbo.sysmergepublications where pubid = @pubid and allow_web_synchronization = 1)
		begin
			raiserror(22545, 16, -1, @publication)
			return 1
		end

		-- Cannot use Logical records and BusinessLogicResolvers at the same time.
		IF @resolver_clsid = '00000000-0000-0000-0000-000000000000'
        begin
			raiserror(20708, 16, -1)
			return 1
        end

		
		-- based on usability feeback, we should set the allow_subscriber_initiated_snapshot
		-- option to 1 rather than raise an error.
		update dbo.sysmergepublications set allow_subscriber_initiated_snapshot = 1
			where pubid = @pubid
	end
	
	/* Make sure that coltracking option matches */
    if exists (select * from dbo.sysmergearticles where objid = @objid and
            identity_support <> @identity_support)
    begin
        raiserror (21240, 16, -1, @source_object)
        return (1)
    end

    -- Do not allow the table to be published by both merge and queued tran
    if object_id('syspublications') is not NULL
    begin
        if exists (select * from syspublications p, sysarticles a where 
            p.allow_queued_tran = 1 and
            p.pubid = a.pubid and
            a.objid = @objid)
        begin
            select @obj_name = object_name(@objid)
            raiserror(21266, 16, -1, @obj_name)
            return (1)
        end

        -- Do not allow the table to be published in both merge tran using automatic identity range management
        if exists (select * from  sysarticles sa, sysarticleupdates au, syspublications pub where 
                sa.objid = @objid and
                au.artid = sa.artid and
                au.pubid = pub.pubid and
                au.identity_support = 1) and
           @identity_support = 1
        begin
            raiserror(20677, 16, -1, @article)
            return (1)
        end
    end
	--co-existance of uploadable merge article on queued subscription table may cause non-convergence in tran pub since queued trigger is NFR
	--allow it in case some customer rely on this already, write warning to errorlog so we can track this condition
	if (0 = @subscriber_upload_options) and (object_id('dbo.MSsubscription_articles') is not null)
	begin
		if exists(select * from dbo.MSsubscription_articles where object_id(quotename(owner) + N'.' + quotename(dest_table)) = @objid)
		begin
			select @obj_name = object_name(@objid)
			raiserror(21860, 10, -1, @obj_name, @db_name) WITH LOG
		end
	end
	
    if exists (select * from dbo.sysmergearticles where objid=@objid and sys.fn_MSmerge_islocalpubid(pubid)=1)
        select @already_published = 1

    if @already_published = 1 and LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto'
    begin
        if @compatlevel < 40
        begin
            raiserror(21359, 10, -1, @publication)
            select @bump_to_80 = 1
        end
        if exists (select * from dbo.sysmergearticles where objid=@objid and sys.fn_MSmerge_islocalpubid(pubid)=1 and
            ((pub_range<>@pub_identity_range) or (range <> @identity_range) or (threshold <> @threshold)))
        begin
            raiserror(21291, 16, -1)
            return (1)
        end
    end

    if 0 <> @subscriber_upload_options and @compatlevel < 90
    begin
        raiserror(21522, 16, -1, 'subscriber_upload_options', @publication)
        return 1
    end

    --Do not allow the table to be published if it contains sparse columns or sparse column_set
    if exists (select * from sys.columns where object_id = @objid and (is_sparse = 1 or is_column_set=1) )
    begin
        raiserror(20738, 16, -1, @article);
        return (1)
    end
   

    /*
    **    Add article to dbo.sysmergearticles and update sys.objects category bit.
    */
    begin tran
    save TRAN sp_addmergearticle

    exec @retcode = sys.sp_MSgetmergeadminapplock @timeout = 0, -- no wait
                                                  @lockowner = N'Transaction'
    if @retcode<>0 or @@error<>0
    begin
        raiserror(20713, 16, -1, 'sp_addmergearticle', @publication)
        goto FAILURE
    end

    select @got_merge_admin_applock = 1

    -- Parameter check @subset_filterclause
    if @subset_filterclause <> '' and @subset_filterclause is not NULL
    begin
        -- check if this is a dynamically filtered article and this is not a dynamically filtered publication
        select @article_has_dynamic_filters = 0
        exec @retcode = sys.sp_check_subset_filter 
                                @qualified_name, 
                                @subset_filterclause, 
                                @article_has_dynamic_filters output, 
                                @functions_in_subset_filter output
        if @retcode<>0 or @@ERROR<>0
        begin
            raiserror(20641, 16, -1)
            goto FAILURE
        end
        if @article_has_dynamic_filters = 1 and 
           (@publication_has_dynamic_filters = 0 or @functions_in_subset_filter <> @dynamic_filters_function_list)
        begin
            if @snapshot_ready > 0 and
               ((@allow_anonymous = 1 and @compatlevel < 90) or 
                 exists (select * from dbo.sysmergesubscriptions where pubid=@pubid and subid<>pubid and status=1)) and
               @force_reinit_subscription = 0
            begin
                raiserror(20642, 16, -1, @article, @subset_filterclause, @publication)
                goto FAILURE
            end
            select @reinit_subscriptions = 1
        end
        
        -- If the newly added article has dynamic filters and the publication is already using partition groups
        -- then setup the correct metadata in merge system tables  such that rerun of snapshot agent sets the partition groups metadata correctly
        if @article_has_dynamic_filters = 1 and @use_partition_groups > 0 and @functions_in_subset_filter <> @dynamic_filters_function_list
        begin
             if @use_partition_groups > 0
             begin
                 delete from dbo.MSmerge_current_partition_mappings where publication_number = @pub_number
                 if @@error <> 0 goto FAILURE
                 delete from dbo.MSmerge_past_partition_mappings where publication_number = @pub_number
                 if @@error <> 0 goto FAILURE
                 delete from dbo.MSmerge_generation_partition_mappings where publication_number = @pub_number
                 if @@error <> 0 goto FAILURE
                 update dbo.sysmergepublications set use_partition_groups = 2 where pubid = @pubid
                 if @@error <> 0 goto FAILURE
             end
             
             exec @retcode = sys.sp_MSdropmergedynamicsnapshotjob @publication = @publication
             if @@error <> 0 or @retcode <> 0
                 goto FAILURE
 
             delete from dbo.MSmerge_dynamic_snapshots where partition_id in
                 (select partition_id from dbo.MSmerge_partition_groups where publication_number = @pub_number)
             if @@error <> 0 goto FAILURE
                 
             delete from dbo.MSdynamicsnapshotjobs where partition_id in
                 (select partition_id from dbo.MSmerge_partition_groups where publication_number = @pub_number)
             if @@error <> 0 goto FAILURE
             
             delete from dbo.MSmerge_partition_groups where publication_number = @pub_number
             if @@error <> 0 goto FAILURE
 
             update dbo.sysmergepublications 
             set dynamic_filters_function_list = NULL,
                dynamic_filters = 0
                where pubid = @pubid
             if @@error <> 0 goto FAILURE
              
             -- Since this is called from sp_addmergearticle, make sure it doesn't raise errors since it is premature stage of the publication. 
             -- The snapshot calls this with @dont_raise_error = NULL which should raise appropriate errors
             exec @retcode = sys.sp_MSset_dynamic_filter_options @publication = @publication, @dynamic_filters = @publication_has_dynamic_filters OUTPUT, @dont_raise_error = 1
             if @retcode<>0 or @@ERROR<>0 goto FAILURE
       end              
    end

    /*
    ** We used to prevent an article from being added to a publication whose snapshot
    ** has been run already. Now we change this so that it is acceptable by doing reinit.
    */
    if @snapshot_ready > 0 
    begin
        if @force_invalidate_snapshot = 0 and @snapshot_ready = 1
        begin
            raiserror(21364, 16, -1, @article)
            goto FAILURE
        end
        update dbo.sysmergepublications set snapshot_ready=2 where pubid=@pubid
        if @@ERROR<>0
            goto FAILURE
    end

    /* 
    ** article status 5 or 6 means there is at least one new article after snapshot is ready
    ** hence all articles added after that point will be new articles as well, regardless of snapshot_ready value.
    */
    if @snapshot_ready>0 or exists (select * from dbo.sysmergearticles where pubid=@pubid and (status=5 or status=6))
    begin
        select @needs_pickup=1
    end

    if @reinit_subscriptions = 1
    begin
        exec @retcode = sys.sp_MSreinitmergepublication 
                                @publication = @publication,
                                @upload_first = @automatic_reinitialization_policy
        if @retcode<>0 or @@ERROR<>0 return 1
    end

    -- if because this article is dynamically filtered the publication is going to change from being 
    -- a static to a dynamic publication, we need to delete all entries in sysmergeschemachange.
    if @publication_has_dynamic_filters = 0 and @article_has_dynamic_filters = 1
    begin
        declare @SCHEMA_TYPE_DROPARTICLE int

        select @SCHEMA_TYPE_DROPARTICLE = 28
        -- don't delete dropmergearticle related schema changes.
        delete from dbo.sysmergeschemachange where pubid = @pubid and schematype not in (@SCHEMA_TYPE_DROPARTICLE)
        if @reinit_subscriptions = 1
            select @needs_pickup = 0
    end

    -- Acquire sch-M lock up-front on the published object 
    exec sys.sp_MSget_qualified_name @objid, @qualname OUTPUT
    if @qualname is null
        goto FAILURE
            
    exec %%Object(MultiName = @qualname).LockMatchID(ID = @objid, Exclusive = 1, BindInternal = 0)
    --exec %%Object(MultiName = @qualname).LockExclusiveMatchID(ID = @objid)
    if @@error <> 0
        goto FAILURE

    select @artid = artid, 
           @preserve_rowguidcol= preserve_rowguidcol
       from dbo.sysmergearticles where objid = @objid

    -- If that article is already in another publication, we reuse its preserve_rowguidcol.
    -- If the article is added the first time, we set preserve_rowguidcol depending
    -- on whether there already is a rowguidcol.
    if @preserve_rowguidcol is null
    begin
        if ObjectProperty(object_id(@qualified_name), 'tablehasrowguidcol') = 1
        begin
            set @preserve_rowguidcol= 1
        end
        else
        begin
            set @preserve_rowguidcol= 0
        end
    end

    if @snapshot_ready > 0
    begin
        /* 
        ** Add the guid column to the user table if needed, cause snapshot_ready>0 would imply
        ** this article has got a rowguid column. No need to add index, triggers, or procedures
        ** as snapshot run will take care of those.
        */
        execute @retcode = sys.sp_MSaddguidcolumn @source_owner, @source_object
        if @@ERROR <> 0 OR    @retcode <> 0  -- NOTE: new change
            goto FAILURE
        execute @retcode = sys.sp_MSaddguidindex @publication, @source_owner, @source_object
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE
    end
    
    --
    -- Need to change sys.columns status before generating sync procs/custom procs 
    -- because the status will be used to decide whether or not call set identity insert. Enable
    -- NFR property if identityrangemanagementoption is MANUAL or AUTO. If 
    -- identityrangemanagementoption is NONE then we will not explicity enable NFR.
    --
    -- This is to change identity column to 'not for replication' if not having been so already
    IF @identityrangemanagementoption in ('auto', 'manual' )
    begin
        select @colname = name
        from sys.columns
        where object_id = @objid and
            is_identity = 1 and -- is identity
            ColumnProperty(object_id, name, 'IsIdNotForRepl') = 0 -- No 'not for repl' property
        if @colname is not null
        begin
            -- Mark 'not for repl'
            EXEC %%ColumnEx(ObjectID = @objid, Name = @colname).SetIdentityNotForRepl(Value = 1)
            IF @@ERROR <> 0
                GOTO FAILURE
        end
    end

    select @statusid = 1  -- default status is inactive

    if @artid is NULL
    begin
        set @artid = newid()
        if @@ERROR <> 0
            goto FAILURE
        execute @retcode = sys.sp_MSgentablenickname @tablenick output, @replnick, @objid
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE
    end
    -- Clone the article properties if article has already been published (in a different pub)
    else
    begin
        /*
        ** Parameter Check:     @article, @publication.
        ** Check if the table already exists in this publication.
        */
        if exists (select * from dbo.sysmergearticles
            where pubid = @pubid AND artid = @artid)
        begin
            raiserror (21292, 16, -1, @source_object)
            goto FAILURE
        end
        
        /* Make sure that coltracking option matches */
        if exists (select * from dbo.sysmergearticles where artid = @artid and
                     column_tracking <> @column_tracking_id)
            begin
                raiserror (20030, 16, -1, @article)
                goto FAILURE
            end
                    
        /* Reuse the article nickname if article has already been published (in a different pub)*/
        select @tablenick = nickname from dbo.sysmergearticles where artid = @artid
        if @tablenick IS NULL
            goto FAILURE
            
        /* Make sure that @resolver_clsid matches the existing resolver_clsid */
        select @resolver_clsid_old = resolver_clsid from dbo.sysmergearticles where artid = @artid 
        if ((@resolver_clsid IS NULL AND @resolver_clsid_old IS NOT NULL) OR
            (@resolver_clsid IS NOT NULL AND @resolver_clsid_old IS NULL) OR
            (@resolver_clsid IS NOT NULL AND @resolver_clsid_old IS NOT NULL AND @resolver_clsid_old <> @resolver_clsid))
        begin
            raiserror (20037, 16, -1, @article)
            goto FAILURE
        end

        /* If publisher could be subscribing the article from another publisher. If so then select the missing_cols info from an existing entry. */

        select @missing_cols = 0x00
        select @missing_col_count = 0
        select @missing_cols = missing_cols,  @missing_col_count = missing_col_count from dbo.sysmergearticles 
            where artid = @artid and sys.fn_MSmerge_islocalpubid(pubid) = 0

        /* Insert to articles, copying some stuff from other article row */
        insert into dbo.sysmergearticles (name, type, objid, sync_objid, artid, description,
                pre_creation_command, pubid, nickname, column_tracking, status,
                creation_script, article_resolver,
                resolver_clsid, schema_option, 
                destination_object, destination_owner, subset_filterclause, view_type, resolver_info, gen_cur, 
                missing_cols, missing_col_count, excluded_cols, excluded_col_count, identity_support,
                before_image_objid, before_view_objid, verify_resolver_signature, allow_interactive_resolver, 
                fast_multicol_updateproc, check_permissions, processing_order, upload_options, published_in_tran_pub, before_upd_view_objid,
                delete_tracking, compensate_for_errors, pub_range, range, threshold, stream_blob_columns, preserve_rowguidcol)
            -- use top 1, distinct could return more than one matching row if status different on partitioned articles
            select top 1 @article, type, objid, @sync_objid, @artid, @description, @precmdid,
                @pubid, nickname, column_tracking, @statusid, @creation_script,
                article_resolver, resolver_clsid, @schema_option, @destination_object, @destination_owner, @subset_filterclause, 
                0, resolver_info, gen_cur, @missing_cols, @missing_col_count, 0x00,0, identity_support,
                before_image_objid, before_view_objid, verify_resolver_signature, allow_interactive_resolver, 
                fast_multicol_updateproc, @check_permissions, @processing_order, @subscriber_upload_options, @published_in_tran_pub_bit, before_upd_view_objid,
                @deletetracking, @compensateforerrors, @pub_identity_range, @identity_range, @threshold, @stream_blob_columns_bit, @preserve_rowguidcol
            from dbo.sysmergearticles where artid = @artid
                
        if (@@rowcount = 1)
        begin
            insert into dbo.sysmergepartitioninfo (artid, pubid, logical_record_level_conflict_detection, logical_record_level_conflict_resolution, partition_options) 
                values (@artid, @pubid, @logical_record_level_conflict_detection_id, @logical_record_level_conflict_resolution_id, @partition_options)
        
            exec @retcode = sys.sp_MScreate_article_repl_view @pubid, @artid
            if @retcode <> 0 or @@error <> 0
                goto FAILURE    
        end
        
        -- need to validate well-partitioned articles even if this particular one
        -- may not be well-partitioned (e.g. with the same article already existing as well-partitioned
        -- in a different publication or subscription).    
        exec @retcode = sys.sp_MSvalidate_wellpartitioned_articles @publication
        if @@error <> 0 or @retcode <> 0
            goto FAILURE
            
        -- identity range setup code
        -- we will only get here if this is a re-publisher or the article is being published in more than one
        -- publication on the root publisher
        -- if @already_published is 1 then the article is being published in more than one publication on the
        -- root publisher. In that case we need not do anything here. However, if @already_publisher is 0, this is a republisher 
        -- and we need to setup the ranges for the republished publication
        if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto' and @already_published = 0
        begin        
            if @compatlevel < 40
            begin
                raiserror(21359, 10, -1, @publication)
                select @bump_to_80 = 1
            end

            exec @retcode = sys.sp_MScheck_republisher_ranges @qualified_name, @artid, @pub_identity_range, @identity_range
            if @retcode<>0 or @@error<>0
                goto FAILURE
                
        end -- end identity range code
      
        /* Jump to end of transaction  */
        goto DONE_TRAN
    end

    /* Add the specific GUID based replication columns to dbo.sysmergearticles */
    insert into dbo.sysmergearticles (name, objid, sync_objid, artid, type, description, pubid, nickname, 
            column_tracking, status, schema_option, pre_creation_command, destination_object, destination_owner, 
            article_resolver, resolver_clsid, subset_filterclause, view_type, resolver_info, columns,
            missing_cols, missing_col_count, excluded_cols, excluded_col_count, identity_support,
            before_image_objid, before_view_objid, verify_resolver_signature, creation_script, allow_interactive_resolver, 
            fast_multicol_updateproc, check_permissions, processing_order, upload_options, published_in_tran_pub, before_upd_view_objid,
            delete_tracking, compensate_for_errors, pub_range, range, threshold, stream_blob_columns, preserve_rowguidcol)
    values (@article, @objid, @sync_objid, @artid, @typeid, @description, @pubid, @tablenick, 
            @column_tracking_id, @statusid, @schema_option, @precmdid, @destination_object, @destination_owner, 
            @article_resolver, @resolver_clsid, @subset_filterclause, 0, @resolver_info, NULL,
             0x00, 0, 0x00,0, @identity_support, NULL, NULL, @verify_resolver_signature, @creation_script, @allow_interactive_bit, 
             @fast_multicol_updateproc_bit, @check_permissions, @processing_order, @subscriber_upload_options, @published_in_tran_pub_bit, NULL,
             @deletetracking, @compensateforerrors, @pub_identity_range, @identity_range, @threshold, @stream_blob_columns_bit, @preserve_rowguidcol)
    if @@ERROR <> 0
        goto FAILURE

    insert into dbo.sysmergepartitioninfo (artid, pubid, logical_record_level_conflict_detection, logical_record_level_conflict_resolution, partition_options) 
        values (@artid, @pubid, @logical_record_level_conflict_detection_id, @logical_record_level_conflict_resolution_id, @partition_options)
    if @@error <> 0
        goto FAILURE

    /*
    ** identity range setup
    */
    if LOWER(@identityrangemanagementoption collate SQL_Latin1_General_CP1_CS_AS) = 'auto' and @already_published = 0
    begin
        declare @range_begin numeric(38,0)
        declare @range_end numeric(38,0)
        declare @next_range_begin numeric(38,0)
        declare @next_range_end numeric(38,0)

        if @compatlevel < 40
        begin
            raiserror(21359, 10, -1, @publication)
            select @bump_to_80 = 1
        end

        -- the following statement will ensure that the ident_curr in now equal to the highest values stored in the 
        -- identity column (lowest value for negative increments). This to account for incorrect reseeds
        -- IDENT_CURRENT Returns the last identity value generated for a specified table in any session and any scope. 
        DBCC CHECKIDENT(@qualified_name, RESEED) with no_infomsgs

        select @max_used = IDENT_CURRENT(@qualified_name)

        if @max_used is NULL
            select @max_used = IDENT_SEED(@qualified_name)

        -- max_used only matters in prepare merge article when we acually allocating a range        
        -- insert the publisher's entry into MSmerge_identity_range table
        -- set max_used to NULL hence
        if @ident_incr > 0
            insert dbo.MSmerge_identity_range(subid, artid, range_begin, range_end, is_pub_range, max_used)
                values(@pubid, @artid, @max_used, @max_range, 1, NULL)
        else
            insert dbo.MSmerge_identity_range(subid, artid, range_begin, range_end, is_pub_range, max_used)
                values(@pubid, @artid, @max_used, @min_range, 1, NULL)
        if @@error <> 0
            goto FAILURE
    end -- end identity range code
    
    -- need to validate well-partitioned articles even if this particular one
    -- may not be well-partitioned (e.g. with the same article already existing as well-partitioned
    -- in a different publication or subscription).    
    exec @retcode = sys.sp_MSvalidate_wellpartitioned_articles @publication
    if @@error <> 0 or @retcode <> 0
        goto FAILURE
    
    exec @retcode = sys.sp_MScreate_article_repl_view @pubid, @artid
    if @retcode <> 0 or @@error <> 0
        goto FAILURE    

    exec %%Relation(ID = @objid).SetMergePublished(Value = 1 , SetColumns=0)
    if @@ERROR <> 0
        goto FAILURE

    /* set up the article's gen-cur */
    set @genguid = newid()
    set @dt = getdate()

    exec @retcode= sys.sp_MSgetreplnick @replnick = @replnick out
    if @retcode<>0 or @@error<>0 
        goto FAILURE

    /*
    ** If there are no zero generation tombstones or rows, add a dummy row in there. 
    */
    if not exists (select * from dbo.MSmerge_genhistory)
    begin
        begin tran

        set identity_insert dbo.MSmerge_genhistory on

        insert into dbo.MSmerge_genhistory (guidsrc, genstatus, generation, art_nick, nicknames, coldate) values
            (@genguid, 1, 1, 0, @replnick + 0xFF, @dt)
        if (@@error <> 0)
            goto FAILURE
            
        set identity_insert dbo.MSmerge_genhistory off

        commit tran
    end

    -- If the article status is active then prepare the article for merge replication
    -- For now, the rowguid column will be added at this time. This is needed until the merge partition group functionality has
    -- a way to defer the initial work done to set up the publication. 
    if @status = 'active' -- or exists (select * from dbo.sysmergepublications where pubid = @pubid and use_partition_groups = 1)
    begin
        /* Get a holdlock on the underlying table */
        select @cmd = 'select * into #tab1 from '
        select @cmd = @cmd + @qualified_name 
        select @cmd = @cmd + 'with (TABLOCK HOLDLOCK) where 1 = 2 '
        execute(@cmd)

        /* Add the guid column to the user table */
        execute @retcode = sys.sp_MSaddguidcolumn @source_owner, @source_object
        if @@ERROR <> 0 OR    @retcode <> 0  -- NOTE: new change
            goto FAILURE

        /* Create an index on the rowguid column in the user table */
        execute @retcode = sys.sp_MSaddguidindex @publication, @source_owner, @source_object
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE

        /* Create the merge triggers on the base table */
        execute @retcode = sys.sp_MSaddmergetriggers @qualified_name, NULL, @column_tracking_id
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE 

        /* Create the merge insert/update stored procedures for the base table */
        execute @retcode = sys.sp_MSsetartprocs @publication, @article, 0, @pubid
        if @@ERROR <> 0 OR @retcode <> 0
            goto FAILURE

        /* Set the article status to be active so that Snapshot does not do this again */
        select @statusid = 2 /* Active article */
        update dbo.sysmergearticles set status = @statusid where artid = @artid
        if @@ERROR <> 0 
            goto FAILURE
    end

DONE_TRAN:                

    if @needs_pickup=1
    begin
        declare @needs_pick_value int 
        select @needs_pick_value=5 --new_inactive status
        update dbo.sysmergearticles set status=@needs_pick_value where artid = @artid and pubid=@pubid
        if @@ERROR<>0
            goto FAILURE
    end

    /*
    ** Set all bits to '1' in the columns column to include all columns.
    */
    IF @ver_partition = 0 --meanning no vertical partition needed.
    BEGIN
        -- Indicate that this is an internal caller of sp_mergearticlecolumn
        EXEC @retcode = sys.sp_MSsetcontext_internalcaller @onoff=1
        IF @@ERROR <> 0 or @retcode <> 0
            goto FAILURE

        EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication=@publication, @article=@article, @schema_replication='true'              
        IF @@ERROR <> 0 OR @retcode <> 0
        BEGIN
            RAISERROR(21198, 16, -1)
            goto FAILURE
        END

        -- Turn off indication that this is an internal caller of sp_mergearticlecolumn
        EXEC @retcode = sys.sp_MSsetcontext_internalcaller @onoff=0
        IF @@ERROR <> 0 or @retcode <> 0
            goto FAILURE

        -- check if table has filestream column
        if exists ( select * from sys.columns where object_id = @objid and is_filestream =1 )
            select @filestream_col_published = 1
    END

    /*
    **    Set all bits to '1' for all columns in the primary key.
    */
    ELSE
    BEGIN

        -- varbinary(max) filestream column cannot be a part of PK
        select @filestream_col_published = 0

        SELECT @indid = index_id FROM sys.indexes WHERE object_id = @objid AND is_primary_key <> 0    /* PK index */
        /*
        **  First we'll figure out what the keys are.
        */
        SELECT @i = 1
        WHILE (@i <= 16)
        BEGIN
            SELECT @pkkey = INDEX_COL(@qualified_name, @indid, @i)
            if @pkkey is NULL
                break
            EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication, @article, @pkkey, 'add'
            IF @@ERROR <> 0 OR @retcode <> 0
            BEGIN
                RAISERROR(21198, 16, -1)
                goto FAILURE
            END            
            select @i = @i + 1
        END
        /*
        ** make sure any existing rowguidcol is in the partition. We can not live without it.
        */
        select @colname=NULL
        select @colname = name from sys.columns where object_id = @objid 
            and is_rowguidcol = 1
        if @colname is not NULL
        BEGIN
            EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication, @article, @colname, 'add'
            if @@error<>0 or @retcode<>0
                goto FAILURE
        END

        /*
        ** If autoidentitymanagement make sure any existing rowguidcol is in the partition. We can not live without it.
        */
        if @identity_support = 1
        BEGIN
            select @colname=NULL
            select @colname = name from sys.columns where object_id = @objid 
                and is_identity = 1
            if @colname is not NULL
            BEGIN
                EXECUTE @retcode  = sys.sp_mergearticlecolumn @publication, @article, @colname, 'add'
                if @@error<>0 or @retcode<>0
                    goto FAILURE
            END
        END

        -- update the sysmergearticles entry to say that we are using vertical partitioning.
        update dbo.sysmergearticles set vertical_partition=1 where pubid=@pubid and artid=@artid
    END

    declare @schema_option_filestream int
    select @schema_option_lodword = sys.fn_replgetbinary8lodword(@schema_option)
    select @schema_option_hidword = sys.fn_replgetbinary8hidword(@schema_option)
    select @schema_option_max_to_nonmax = 0x20000000 -- this has to be on  for < 90RTM
    select @schema_option_filestream = 0x00000001
   
    -- varbinary(max) column with filestream attribute cannot be converted to base type (image)
    -- irrespective of whether filestream is repl. as filestream or varbinary(max)
    if ( @filestream_col_published = 1 and (@schema_option_lodword & @schema_option_max_to_nonmax = @schema_option_max_to_nonmax))
    begin
        RAISERROR(22583, 16, -1, @article, @publication)
        goto FAILURE    
    end

    -- if compat level is Yukon and turn OFF the schema_option to replicate filestream attribute, if ON
    if ( @filestream_col_published = 1 and @compatlevel >= 90 and @compatlevel < 100 and  
    (@schema_option_hidword & @schema_option_filestream = @schema_option_filestream ))
    begin
        declare @schema_option_strg sysname
        select @schema_option_lodword = sys.fn_replgetbinary8lodword(@schema_option)
        select @schema_option_hidword = @schema_option_hidword & ~( @schema_option_filestream)
        select @schema_option = sys.fn_replcombinehilodwordintobinary8(@schema_option_hidword, @schema_option_lodword)
        UPDATE dbo.sysmergearticles SET schema_option = @schema_option WHERE artid = @artid AND pubid = @pubid
        select @schema_option_strg = sys.fn_varbintohexstr(@schema_option)
        RAISERROR (22584, 10, -1, @schema_option_strg)
    end


    exec @retcode = sys.sp_MSfillupmissingcols @publication, @qualified_name
    if @retcode<>0 or @@ERROR<>0
        goto FAILURE

    /*
    ** For articles with subset filter clause - set the pub type to subset
    */
    if len(@subset_filterclause) > 0
    begin
        execute @retcode = sys.sp_MSsubsetpublication @publication
        if @@ERROR <> 0 or @retcode<>0
            goto FAILURE
    end                        

    -- set up deleted col info
    declare @deleted_cols varbinary(128)
    execute sp_MSfillup_deleted_cols @objid, @deleted_cols output
    update dbo.sysmergearticles set deleted_cols=@deleted_cols 
        where artid = @artid and pubid=@pubid

    if @bump_to_80=1
    begin
        exec @retcode = sys.sp_MSBumpupCompLevel @pubid, 40
        if @@ERROR<>0 or @retcode<>0
            goto FAILURE
    end
    
    exec sys.sp_MSreleasemergeadminapplock @lockowner = N'Transaction'
    COMMIT TRAN 

    return (0)

FAILURE:
    RAISERROR (20009, 16, -1, @article, @publication)
    if @@TRANCOUNT > 0
    begin
        if @got_merge_admin_applock=1
            exec sys.sp_MSreleasemergeadminapplock @lockowner = N'Transaction'
        ROLLBACK TRANSACTION sp_addmergearticle
        COMMIT TRANSACTION
    end
    return (1)

