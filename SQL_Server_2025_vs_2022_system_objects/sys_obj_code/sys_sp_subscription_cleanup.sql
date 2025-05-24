SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_subscription_cleanup
(
    @publisher      sysname,
    @publisher_db   sysname = NULL,
    @publication    sysname = NULL,
    @reserved       nvarchar(10) = NULL,
    @from_backup    bit = 0
)
AS
BEGIN
    DECLARE    @object_name sysname
    DECLARE    @fullname nvarchar(517)
    DECLARE    @object_type char(2)
    DECLARE    @independent_agent bit
    DECLARE    @retcode int
    DECLARE    @parent_obj int
    DECLARE    @object_id int
    DECLARE    @cmd nvarchar(4000)
    DECLARE    @publisherlinkusertodrop sysname

    SET NOCOUNT ON

    /*
    ** Security Check
    */

    EXEC @retcode = sys.sp_MSreplcheck_subscribe
    IF @@ERROR <> 0 or @retcode <> 0
        RETURN(1)
        
    IF @publication = '' OR @publication is NULL
    BEGIN
       SELECT @independent_agent = 0
    END
    ELSE
    BEGIN
        SELECT @independent_agent = 1
    END

    /*
    ** Parameter Check: @publisher_db
    */
    IF @publisher_db = 'all'
        AND @reserved != 'drop_all'
    BEGIN
        RAISERROR (14136, 16, -1)
        RETURN (1)
    END
    
    IF @publisher_db IS NULL
    BEGIN
		-- The parameter @publisher_db cannot be NULL.
        RAISERROR (14043, 16, -1, '@publisher_db', 'sp_subscription_cleanup')
        RETURN 1
    END

    EXEC @retcode = sys.sp_validname @publisher_db
    IF @@ERROR <> 0 OR @retcode <> 0
        RETURN 1
 
	-- We must cleanup the p2p meta-data prior to 
	-- dropping the MSsubscription_agents table
    EXEC @retcode = sys.sp_MScleanup_peer_metadata @type = 1,
													@publication = @publication,
                                                    @from_backup = @from_backup
	IF @retcode <> 0 or @@error <> 0
    BEGIN
		RETURN(1)
    END

    IF OBJECT_ID('MSsubscription_properties') IS NOT NULL
    BEGIN
        -- Unlink updating sub publisher information
        exec @retcode = sys.sp_unlink_publication_internal @publisher, @publisher_db, @publication, @reserved,@publisherlinkusertodrop = @publisherlinkusertodrop OUTPUT
        if (@retcode != 0 or @@error != 0)
            return (1)
    END

    IF OBJECT_ID('dbo.MSreplication_objects') IS NOT NULL
    BEGIN
        DECLARE object_cursor CURSOR LOCAL FAST_FORWARD FOR 
        SELECT DISTINCT object_name, object_type
        FROM    MSreplication_objects o, MSreplication_subscriptions s
        WHERE   (
        			-- Identify entries for a specific publication, or
					-- for all publications associated with a shared agent.
                    UPPER(o.publisher) = UPPER(@publisher) AND
                    UPPER(o.publisher) = UPPER(s.publisher) AND
                    o.publisher_db = @publisher_db AND
                    o.publisher_db = s.publisher_db AND
                    (
                      o.publication = @publication OR
                      (
                          @independent_agent = 0 AND
                          s.independent_agent = 0
                      ) 
                    )
                )
          OR    @reserved = 'drop_all' 

        OPEN    object_cursor
        FETCH    object_cursor INTO @object_name, @object_type

        WHILE (@@fetch_status <> -1)
        BEGIN
            IF @object_type in ('T' , 'P')
            BEGIN
                SELECT    @parent_obj = NULL
                
                SELECT    @parent_obj    = parent_object_id,
                        @object_id    = object_id
                FROM    sys.objects
                WHERE    name = @object_name
                
                IF @parent_obj IS NOT NULL
                BEGIN
                    -- Unmark synctran bit
                    EXEC sys.sp_MSget_qualified_name @parent_obj, @fullname output

                    IF @fullname IS NOT NULL
                    BEGIN
                        EXEC %%Object(MultiName = @fullname).LockMatchID(ID = @parent_obj, Exclusive = 1, BindInternal = 0)
                        --EXEC %%Object(MultiName = @fullname).LockExclusiveMatchID(ID = @parent_obj)

                        IF @@ERROR <> 0
                        BEGIN
                            RETURN(1)
                        END

                        EXEC %%Relation(ID = @parent_obj).SetSyncTranSubscribe(Value = 0)
                    end

                    EXEC @retcode = sys.sp_MSdrop_object @object_id = @object_id

                    IF @retcode <> 0 or @@error <> 0
                    BEGIN
                        RETURN(1)
                    END

                    -- Clean up identity range entry
                    -- Since we only support one trigger per subscriber table
                    -- we assume identity range row can not be reused by multiple
                    -- subscriptions.
                    IF OBJECT_ID('MSsub_identity_range') IS NOT NULL
                    BEGIN
                        IF EXISTS
                        (
                            SELECT    *
                            FROM    MSsub_identity_range
                            WHERE    objid = @parent_obj
                        )
                        BEGIN
                            -- Drop the identity range constraits.
                            EXEC @retcode = sys.sp_MSreseed    @objid            =  @parent_obj,
                                                            -- range or seed can be anything
                                                            @next_seed        = 10,
                                                            @range            = 10,
                                                            @is_publisher    = -1,
                                                            @check_only        = 1,
                                                            @drop_only        = 1

                            IF @retcode <> 0 or @@ERROR <> 0 
                            BEGIN
                                RETURN(1)
                            END

                            DELETE MSsub_identity_range
                            WHERE    objid = @parent_obj
                            
                            IF @@ERROR <> 0 
                            BEGIN
                                RETURN(1)
                            END
                        END

                        IF NOT EXISTS (SELECT * FROM MSsub_identity_range)
                        BEGIN
                            DROP TABLE MSsub_identity_range
                            IF @@ERROR <> 0 
                            BEGIN
                                RETURN(1)
                            END
                        END
                    END
                END
            END

            DELETE FROM MSreplication_objects
            WHERE object_name=@object_name

            FETCH object_cursor INTO @object_name, @object_type
        END
        
        CLOSE object_cursor
        DEALLOCATE object_cursor

        IF NOT EXISTS (SELECT * FROM MSreplication_objects) 
        BEGIN
            DROP TABLE MSreplication_objects

            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    --
    -- We must wait to drop the user until after the triggers are removed due
    --  to dependancy issues
    --
    if @publisherlinkusertodrop is not null
    begin
        exec @retcode = sys.sp_dropuser @name_in_db = @publisherlinkusertodrop
        if @@error <> 0 or @retcode <> 0
            return (1)        
    end

    IF OBJECT_ID('dbo.MSsavedforeignkeys', 'U') IS NOT NULL
    BEGIN
        IF @reserved = 'drop_all'
            TRUNCATE TABLE dbo.MSsavedforeignkeys
        -- 
        -- drop MSsavedforeignkeys if empty
        -- 
        IF NOT EXISTS
        (
            SELECT      *
            FROM     dbo.MSsavedforeignkeys
        )
        BEGIN
            DROP TABLE dbo.MSsavedforeignkeys
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    IF OBJECT_ID('dbo.MSsavedforeignkeycolumns', 'U') IS NOT NULL
    BEGIN
        IF @reserved = 'drop_all'
            TRUNCATE TABLE dbo.MSsavedforeignkeycolumns
        -- 
        -- drop MSsavedforeignkeycolumns if empty
        -- 
        IF NOT EXISTS
        (
            SELECT      *
            FROM     dbo.MSsavedforeignkeycolumns
        )
        BEGIN
            DROP TABLE dbo.MSsavedforeignkeycolumns
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    IF OBJECT_ID('dbo.MSsavedforeignkeyextendedproperties', 'U') IS NOT NULL
    BEGIN
        IF @reserved = 'drop_all'
            TRUNCATE TABLE dbo.MSsavedforeignkeyextendedproperties

        -- 
        -- drop MSsavedforeignkeyextendedproperties if empty
        -- 
        IF NOT EXISTS
        (
            SELECT      *
            FROM     dbo.MSsavedforeignkeyextendedproperties
        )
        BEGIN
            DROP TABLE dbo.MSsavedforeignkeyextendedproperties
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    --
    -- cleanup queued conflict tables
    --
    IF OBJECT_ID('MSsubscription_agents') IS NOT NULL
    BEGIN
        DECLARE    @agent_id int
        DECLARE    @cft_table sysname
        DECLARE    @owner sysname

        --
        -- first get the agent(s) for this queued subscription(s) and 
        -- 
        DECLARE    #agent_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT    id
        FROM    dbo.MSsubscription_agents 
        WHERE    (
                    (
                        UPPER(publisher) = UPPER(@publisher) AND
                        publisher_db = @publisher_db AND 
                        publication = @publication
                    ) 
                    OR
                    (@reserved = 'drop_all')
                )
          AND    update_mode IN (0, 2,3,4,5)

        OPEN    #agent_cursor
        FETCH    #agent_cursor INTO @agent_id

        WHILE (@@fetch_status != -1)
        BEGIN
            --
            -- drop the conflict table for each article in this subscription
            --
            IF OBJECT_ID('MSsubscription_articles') IS NOT NULL
            BEGIN
                DECLARE #object_cursor CURSOR LOCAL FAST_FORWARD FOR 
                SELECT    owner,
                        cft_table
                FROM    dbo.MSsubscription_articles
                WHERE    agent_id = @agent_id

                OPEN #object_cursor
                FETCH #object_cursor INTO @owner, @cft_table

                WHILE (@@fetch_status != -1)
                BEGIN
                    --
                    -- drop the conflict table(s) for this article - ignore errors
                    --
                    SELECT    @cmd =    CASE WHEN (@owner IS NULL) THEN
                                        N'IF OBJECT_ID(N' + 
                                        QUOTENAME(@cft_table, N'''') collate database_default + ') IS NOT NULL drop table ' + 
                                        QUOTENAME(@cft_table) collate database_default
                                    ELSE
                                        N'IF OBJECT_ID(N' + 
                                        QUOTENAME(@cft_table, N'''') collate database_default + ') IS NOT NULL drop table ' + 
                                        QUOTENAME(@owner) collate database_default + N'.' +
                                        QUOTENAME(@cft_table) collate database_default
                                    END

                    EXECUTE(@cmd)

                    -- get next row
                    FETCH #object_cursor INTO @owner, @cft_table
                END
                
                CLOSE #object_cursor
                DEALLOCATE #object_cursor
                --
                -- delete entries from MSsubscription_articles for this agent id
                --
                DELETE    dbo.MSsubscription_articles
                WHERE    agent_id = @agent_id
            END
            --
            -- delete entries from MSsubscription_articlecolumns
            --
            IF OBJECT_ID('MSsubscription_articlecolumns') IS NOT NULL
            BEGIN
                DELETE dbo.MSsubscription_articlecolumns where agent_id = @agent_id
            END

            --
            -- get the next agent
            --
            FETCH #agent_cursor INTO @agent_id
        END
        
        CLOSE #agent_cursor
        DEALLOCATE #agent_cursor
    END

    --
    -- clean discarded queued transactions
    --
    IF OBJECT_ID('dbo.MSreplication_queue') IS NOT NULL
    BEGIN
        DELETE    dbo.MSreplication_queue
        WHERE    (
                    publisher = UPPER(@publisher) AND 
                    publisher_db = @publisher_db AND
                    publication = @publication
                )
           OR    @reserved = 'drop_all'
    end
    
    IF OBJECT_ID('dbo.MSrepl_queuedtraninfo') IS NOT NULL
    BEGIN
        DELETE    dbo.MSrepl_queuedtraninfo
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                    publisher_db = @publisher_db AND
                    publication = @publication
                )
           OR    @reserved = 'drop_all'
    end

    IF OBJECT_ID('dbo.MSreplication_subscriptions') IS NOT NULL
    BEGIN
        -- Drop the subscription as long as the publication name matches even if
        -- the publication is not independent agent
        -- This behaviour is expected by sp_droppullsubscription
        DELETE    dbo.MSreplication_subscriptions 
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                      publisher_db = @publisher_db AND
                      -- independent_agent = @independent_agent and 
                      (
                          publication = @publication OR
                          (
                              @independent_agent = 0 AND
                              independent_agent = 0
                          ) 
                      )
                  )
           OR    @reserved = 'drop_all'
    END

    IF OBJECT_ID('MSsubscription_agents') IS NOT NULL
    BEGIN
        -- Drop the subscription as long as the publication name matches even if
        -- the publication is not independent agent
        -- This behaviour is expected by sp_droppullsubscription
        DELETE    dbo.MSsubscription_agents
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                    publisher_db = @publisher_db AND
                    (
                        publication = @publication OR
                        (
                            @independent_agent = 0 AND
                            publication = N'ALL'
                        )
                    )
                )
           OR    @reserved = 'drop_all'

        -- Delete the agent entry if no corresponding rows found in
        -- MSreplication_subscription table. This is to cleanup share agent entry.
        -- This behaviour is expected by sp_droppullsubscription
        IF OBJECT_ID('MSreplication_subscriptions') IS NOT NULL
        BEGIN
            IF NOT EXISTS
            (
                SELECT    *
                FROM    dbo.MSreplication_subscriptions
                WHERE    UPPER(publisher) = UPPER(@publisher) 
                  AND    publisher_db = @publisher_db
                  AND    independent_agent = 0
            )
            BEGIN
                DELETE    dbo.MSsubscription_agents
                WHERE    UPPER(publisher) = UPPER(@publisher) 
                  AND    publisher_db = @publisher_db
                  AND    publication = N'ALL'
            END
        END
        ELSE
        BEGIN
            DELETE MSsubscription_agents
        END

		--cleanup queued tables only if there is no updateable subscriptions left
		if not exists(select * from MSsubscription_agents where update_mode > 0)
		begin
			IF OBJECT_ID('dbo.MSsubscription_articles') IS NOT NULL
			BEGIN
				--
				-- drop MSsubscription_articles if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSsubscription_articles
				)
				BEGIN
				    DROP TABLE dbo.MSsubscription_articles
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END

			IF OBJECT_ID('dbo.MSsubscription_articlecolumns') IS NOT NULL
			BEGIN
				--
				-- drop MSsubscription_articlecolumns if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSsubscription_articlecolumns
				)
				BEGIN
				    DROP TABLE dbo.MSsubscription_articlecolumns
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END
			IF OBJECT_ID('dbo.MSreplication_queue') IS NOT NULL
			BEGIN
				--
				-- drop MSreplication_queue if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSreplication_queue
				)
				BEGIN
				    DROP TABLE dbo.MSreplication_queue
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END
			IF OBJECT_ID('dbo.MSrepl_queuedtraninfo') IS NOT NULL
			BEGIN
				--
				-- drop MSrepl_queuedtraninfo if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSrepl_queuedtraninfo
				)
				BEGIN
				    DROP TABLE dbo.MSrepl_queuedtraninfo
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END
		end
    END
    
    IF OBJECT_ID('MSsubscription_properties') IS NOT NULL
    BEGIN
        DELETE    dbo.MSsubscription_properties 
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                    publisher_db = @publisher_db AND
                    publication = @publication
                ) 
           OR    @reserved = 'drop_all'
 
        IF @@ERROR <> 0 
        BEGIN
            RETURN(1)
        END
    END

    -- These three tables must be dropped together (see sp_helppullsubscription)
    DECLARE @MSrs_exists bit,   -- MSreplication_subscriptions exists
            @MSsa_exists bit,   -- MSsubscription_agents exists
            @MSsp_exists bit,   -- MSsubscription_properties exists
            @MSrs_empty  bit,   -- MSreplication_subscriptions empty
            @MSsa_empty  bit,   -- MSsubscription_agents empty
            @MSsp_empty  bit    -- MSsubscription_properties empty

    SELECT  @MSrs_exists = ISNULL(OBJECT_ID('MSreplication_subscriptions'), 0),
            @MSsa_exists = ISNULL(OBJECT_ID('MSsubscription_agents'), 0),
            @MSsp_exists = ISNULL(OBJECT_ID('MSsubscription_properties'), 0),
            @MSrs_empty  = 0,
            @MSsa_empty  = 0,
            @MSsp_empty  = 0
    
    IF @MSrs_exists = 1
    BEGIN
        IF NOT EXISTS (SELECT * FROM MSreplication_subscriptions)
            SELECT @MSrs_empty = 1
    END
    
    IF @MSsa_exists = 1
    BEGIN
        IF NOT EXISTS (SELECT * FROM MSsubscription_agents)
            SELECT @MSsa_empty = 1
    END

    IF @MSsp_exists = 1
    BEGIN
        IF NOT EXISTS (SELECT * FROM MSsubscription_properties)
            SELECT @MSsp_empty = 1
    END

    -- only attempt a drop if all the tables either do not exist or are empty
    IF  (@MSrs_exists = 0 OR @MSrs_empty = 1) AND
        (@MSsa_exists = 0 OR @MSsa_empty = 1) AND
        (@MSsp_exists = 0 OR @MSsp_empty = 1)
    BEGIN
        IF @MSrs_exists = 1
        BEGIN
            DROP TABLE MSreplication_subscriptions
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END

        IF @MSsa_exists = 1
        BEGIN
            DROP TABLE MSsubscription_agents
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END

        IF @MSsp_exists = 1
        BEGIN
            EXEC @retcode = sys.sp_MSsub_cleanup_prop_table
            IF @@ERROR <> 0 OR @retcode <> 0
            BEGIN
                RETURN(1)
            END
        END
    END
        
    EXEC @retcode = sys.sp_resetsnapshotdeliveryprogress @drop_table = N'true'
    IF @retcode <> 0 or @@error <> 0
    BEGIN
        RETURN(1)
    END

	-- only attempt to drop the p2p tables if the db is not SQLDB
	-- 
	-- AND
	--  
	-- db is not published. if the db is published then the remaining 
	-- p2p tables will be removed when publish is turned off
	--
	-- OR
	--
	-- If we are in drop_all mode then get rid of the P2P 
	-- tables. This should only be true for sp_removedbreplication
	IF (serverproperty('EngineEdition') not in (5,12)) AND (sys.fn_MSrepl_istranpublished(DB_NAME(),0) != 1
		OR @reserved = 'drop_all')
	BEGIN
		EXEC @retcode = sys.sp_MSdrop_peertopeer_tables @from_backup = @from_backup
		IF @retcode <> 0 or @@error <> 0
	    BEGIN
			RETURN(1)
	    END	
	END

    -- Ignore errors.
    EXEC sys.sp_MSsub_cleanup_orphans

    EXEC sys.sp_dropreplsymmetrickey @check_replication = 1, @throw_error = 0
    
    RETURN (0)
END


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_subscription_cleanup
(
    @publisher      sysname,
    @publisher_db   sysname = NULL,
    @publication    sysname = NULL,
    @reserved       nvarchar(10) = NULL,
    @from_backup    bit = 0
)
AS
BEGIN
    DECLARE    @object_name sysname
    DECLARE    @fullname nvarchar(517)
    DECLARE    @object_type char(2)
    DECLARE    @independent_agent bit
    DECLARE    @retcode int
    DECLARE    @parent_obj int
    DECLARE    @object_id int
    DECLARE    @cmd nvarchar(4000)
    DECLARE    @publisherlinkusertodrop sysname

    SET NOCOUNT ON

    /*
    ** Security Check
    */

    EXEC @retcode = sys.sp_MSreplcheck_subscribe
    IF @@ERROR <> 0 or @retcode <> 0
        RETURN(1)
        
    IF @publication = '' OR @publication is NULL
    BEGIN
       SELECT @independent_agent = 0
    END
    ELSE
    BEGIN
        SELECT @independent_agent = 1
    END

    /*
    ** Parameter Check: @publisher_db
    */
    IF @publisher_db = 'all'
        AND @reserved != 'drop_all'
    BEGIN
        RAISERROR (14136, 16, -1)
        RETURN (1)
    END
    
    IF @publisher_db IS NULL
    BEGIN
		-- The parameter @publisher_db cannot be NULL.
        RAISERROR (14043, 16, -1, '@publisher_db', 'sp_subscription_cleanup')
        RETURN 1
    END

    EXEC @retcode = sys.sp_validname @publisher_db
    IF @@ERROR <> 0 OR @retcode <> 0
        RETURN 1
 
	-- We must cleanup the p2p meta-data prior to 
	-- dropping the MSsubscription_agents table
    EXEC @retcode = sys.sp_MScleanup_peer_metadata @type = 1,
													@publication = @publication,
                                                    @from_backup = @from_backup
	IF @retcode <> 0 or @@error <> 0
    BEGIN
		RETURN(1)
    END

    IF OBJECT_ID('MSsubscription_properties') IS NOT NULL
    BEGIN
        -- Unlink updating sub publisher information
        exec @retcode = sys.sp_unlink_publication_internal @publisher, @publisher_db, @publication, @reserved,@publisherlinkusertodrop = @publisherlinkusertodrop OUTPUT
        if (@retcode != 0 or @@error != 0)
            return (1)
    END

    IF OBJECT_ID('dbo.MSreplication_objects') IS NOT NULL
    BEGIN
        DECLARE object_cursor CURSOR LOCAL FAST_FORWARD FOR 
        SELECT DISTINCT object_name, object_type
        FROM    MSreplication_objects o, MSreplication_subscriptions s
        WHERE   (
        			-- Identify entries for a specific publication, or
					-- for all publications associated with a shared agent.
                    UPPER(o.publisher) = UPPER(@publisher) AND
                    UPPER(o.publisher) = UPPER(s.publisher) AND
                    o.publisher_db = @publisher_db AND
                    o.publisher_db = s.publisher_db AND
                    (
                      o.publication = @publication OR
                      (
                          @independent_agent = 0 AND
                          s.independent_agent = 0
                      ) 
                    )
                )
          OR    @reserved = 'drop_all' 

        OPEN    object_cursor
        FETCH    object_cursor INTO @object_name, @object_type

        WHILE (@@fetch_status <> -1)
        BEGIN
            IF @object_type in ('T' , 'P')
            BEGIN
                SELECT    @parent_obj = NULL
                
                SELECT    @parent_obj    = parent_object_id,
                        @object_id    = object_id
                FROM    sys.objects
                WHERE    name = @object_name
                
                IF @parent_obj IS NOT NULL
                BEGIN
                    -- Unmark synctran bit
                    EXEC sys.sp_MSget_qualified_name @parent_obj, @fullname output

                    IF @fullname IS NOT NULL
                    BEGIN
                        EXEC %%Object(MultiName = @fullname).LockMatchID(ID = @parent_obj, Exclusive = 1, BindInternal = 0)
                        --EXEC %%Object(MultiName = @fullname).LockExclusiveMatchID(ID = @parent_obj)

                        IF @@ERROR <> 0
                        BEGIN
                            RETURN(1)
                        END

                        EXEC %%Relation(ID = @parent_obj).SetSyncTranSubscribe(Value = 0)
                    end

                    EXEC @retcode = sys.sp_MSdrop_object @object_id = @object_id

                    IF @retcode <> 0 or @@error <> 0
                    BEGIN
                        RETURN(1)
                    END

                    -- Clean up identity range entry
                    -- Since we only support one trigger per subscriber table
                    -- we assume identity range row can not be reused by multiple
                    -- subscriptions.
                    IF OBJECT_ID('MSsub_identity_range') IS NOT NULL
                    BEGIN
                        IF EXISTS
                        (
                            SELECT    *
                            FROM    MSsub_identity_range
                            WHERE    objid = @parent_obj
                        )
                        BEGIN
                            -- Drop the identity range constraits.
                            EXEC @retcode = sys.sp_MSreseed    @objid            =  @parent_obj,
                                                            -- range or seed can be anything
                                                            @next_seed        = 10,
                                                            @range            = 10,
                                                            @is_publisher    = -1,
                                                            @check_only        = 1,
                                                            @drop_only        = 1

                            IF @retcode <> 0 or @@ERROR <> 0 
                            BEGIN
                                RETURN(1)
                            END

                            DELETE MSsub_identity_range
                            WHERE    objid = @parent_obj
                            
                            IF @@ERROR <> 0 
                            BEGIN
                                RETURN(1)
                            END
                        END

                        IF NOT EXISTS (SELECT * FROM MSsub_identity_range)
                        BEGIN
                            DROP TABLE MSsub_identity_range
                            IF @@ERROR <> 0 
                            BEGIN
                                RETURN(1)
                            END
                        END
                    END
                END
            END

            DELETE FROM MSreplication_objects
            WHERE object_name=@object_name

            FETCH object_cursor INTO @object_name, @object_type
        END
        
        CLOSE object_cursor
        DEALLOCATE object_cursor

        IF NOT EXISTS (SELECT * FROM MSreplication_objects) 
        BEGIN
            DROP TABLE MSreplication_objects

            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    --
    -- We must wait to drop the user until after the triggers are removed due
    --  to dependancy issues
    --
    if @publisherlinkusertodrop is not null
    begin
        exec @retcode = sys.sp_dropuser @name_in_db = @publisherlinkusertodrop
        if @@error <> 0 or @retcode <> 0
            return (1)        
    end

    IF OBJECT_ID('dbo.MSsavedforeignkeys', 'U') IS NOT NULL
    BEGIN
        IF @reserved = 'drop_all'
            TRUNCATE TABLE dbo.MSsavedforeignkeys
        -- 
        -- drop MSsavedforeignkeys if empty
        -- 
        IF NOT EXISTS
        (
            SELECT      *
            FROM     dbo.MSsavedforeignkeys
        )
        BEGIN
            DROP TABLE dbo.MSsavedforeignkeys
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    IF OBJECT_ID('dbo.MSsavedforeignkeycolumns', 'U') IS NOT NULL
    BEGIN
        IF @reserved = 'drop_all'
            TRUNCATE TABLE dbo.MSsavedforeignkeycolumns
        -- 
        -- drop MSsavedforeignkeycolumns if empty
        -- 
        IF NOT EXISTS
        (
            SELECT      *
            FROM     dbo.MSsavedforeignkeycolumns
        )
        BEGIN
            DROP TABLE dbo.MSsavedforeignkeycolumns
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    IF OBJECT_ID('dbo.MSsavedforeignkeyextendedproperties', 'U') IS NOT NULL
    BEGIN
        IF @reserved = 'drop_all'
            TRUNCATE TABLE dbo.MSsavedforeignkeyextendedproperties

        -- 
        -- drop MSsavedforeignkeyextendedproperties if empty
        -- 
        IF NOT EXISTS
        (
            SELECT      *
            FROM     dbo.MSsavedforeignkeyextendedproperties
        )
        BEGIN
            DROP TABLE dbo.MSsavedforeignkeyextendedproperties
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END
    END

    --
    -- cleanup queued conflict tables
    --
    IF OBJECT_ID('MSsubscription_agents') IS NOT NULL
    BEGIN
        DECLARE    @agent_id int
        DECLARE    @cft_table sysname
        DECLARE    @owner sysname

        --
        -- first get the agent(s) for this queued subscription(s) and 
        -- 
        DECLARE    #agent_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT    id
        FROM    dbo.MSsubscription_agents 
        WHERE    (
                    (
                        UPPER(publisher) = UPPER(@publisher) AND
                        publisher_db = @publisher_db AND 
                        publication = @publication
                    ) 
                    OR
                    (@reserved = 'drop_all')
                )
          AND    update_mode IN (0, 2,3,4,5)

        OPEN    #agent_cursor
        FETCH    #agent_cursor INTO @agent_id

        WHILE (@@fetch_status != -1)
        BEGIN
            --
            -- drop the conflict table for each article in this subscription
            --
            IF OBJECT_ID('MSsubscription_articles') IS NOT NULL
            BEGIN
                DECLARE #object_cursor CURSOR LOCAL FAST_FORWARD FOR 
                SELECT    owner,
                        cft_table
                FROM    dbo.MSsubscription_articles
                WHERE    agent_id = @agent_id

                OPEN #object_cursor
                FETCH #object_cursor INTO @owner, @cft_table

                WHILE (@@fetch_status != -1)
                BEGIN
                    --
                    -- drop the conflict table(s) for this article - ignore errors
                    --
                    SELECT    @cmd =    CASE WHEN (@owner IS NULL) THEN
                                        N'IF OBJECT_ID(N' + 
                                        QUOTENAME(@cft_table, N'''') collate database_default + ') IS NOT NULL drop table ' + 
                                        QUOTENAME(@cft_table) collate database_default
                                    ELSE
                                        N'IF OBJECT_ID(N' + 
                                        QUOTENAME(@cft_table, N'''') collate database_default + ') IS NOT NULL drop table ' + 
                                        QUOTENAME(@owner) collate database_default + N'.' +
                                        QUOTENAME(@cft_table) collate database_default
                                    END

                    EXECUTE(@cmd)

                    -- get next row
                    FETCH #object_cursor INTO @owner, @cft_table
                END
                
                CLOSE #object_cursor
                DEALLOCATE #object_cursor
                --
                -- delete entries from MSsubscription_articles for this agent id
                --
                DELETE    dbo.MSsubscription_articles
                WHERE    agent_id = @agent_id
            END
            --
            -- delete entries from MSsubscription_articlecolumns
            --
            IF OBJECT_ID('MSsubscription_articlecolumns') IS NOT NULL
            BEGIN
                DELETE dbo.MSsubscription_articlecolumns where agent_id = @agent_id
            END

            --
            -- get the next agent
            --
            FETCH #agent_cursor INTO @agent_id
        END
        
        CLOSE #agent_cursor
        DEALLOCATE #agent_cursor
    END

    --
    -- clean discarded queued transactions
    --
    IF OBJECT_ID('dbo.MSreplication_queue') IS NOT NULL
    BEGIN
        DELETE    dbo.MSreplication_queue
        WHERE    (
                    publisher = UPPER(@publisher) AND 
                    publisher_db = @publisher_db AND
                    publication = @publication
                )
           OR    @reserved = 'drop_all'
    end
    
    IF OBJECT_ID('dbo.MSrepl_queuedtraninfo') IS NOT NULL
    BEGIN
        DELETE    dbo.MSrepl_queuedtraninfo
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                    publisher_db = @publisher_db AND
                    publication = @publication
                )
           OR    @reserved = 'drop_all'
    end

    IF OBJECT_ID('dbo.MSreplication_subscriptions') IS NOT NULL
    BEGIN
        -- Drop the subscription as long as the publication name matches even if
        -- the publication is not independent agent
        -- This behaviour is expected by sp_droppullsubscription
        DELETE    dbo.MSreplication_subscriptions 
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                      publisher_db = @publisher_db AND
                      -- independent_agent = @independent_agent and 
                      (
                          publication = @publication OR
                          (
                              @independent_agent = 0 AND
                              independent_agent = 0
                          ) 
                      )
                  )
           OR    @reserved = 'drop_all'
    END

    IF OBJECT_ID('MSsubscription_agents') IS NOT NULL
    BEGIN
        -- Drop the subscription as long as the publication name matches even if
        -- the publication is not independent agent
        -- This behaviour is expected by sp_droppullsubscription
        DELETE    dbo.MSsubscription_agents
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                    publisher_db = @publisher_db AND
                    (
                        publication = @publication OR
                        (
                            @independent_agent = 0 AND
                            publication = N'ALL'
                        )
                    )
                )
           OR    @reserved = 'drop_all'

        -- Delete the agent entry if no corresponding rows found in
        -- MSreplication_subscription table. This is to cleanup share agent entry.
        -- This behaviour is expected by sp_droppullsubscription
        IF OBJECT_ID('MSreplication_subscriptions') IS NOT NULL
        BEGIN
            IF NOT EXISTS
            (
                SELECT    *
                FROM    dbo.MSreplication_subscriptions
                WHERE    UPPER(publisher) = UPPER(@publisher) 
                  AND    publisher_db = @publisher_db
                  AND    independent_agent = 0
            )
            BEGIN
                DELETE    dbo.MSsubscription_agents
                WHERE    UPPER(publisher) = UPPER(@publisher) 
                  AND    publisher_db = @publisher_db
                  AND    publication = N'ALL'
            END
        END
        ELSE
        BEGIN
            DELETE MSsubscription_agents
        END

		--cleanup queued tables only if there is no updateable subscriptions left
		if not exists(select * from MSsubscription_agents where update_mode > 0)
		begin
			IF OBJECT_ID('dbo.MSsubscription_articles') IS NOT NULL
			BEGIN
				--
				-- drop MSsubscription_articles if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSsubscription_articles
				)
				BEGIN
				    DROP TABLE dbo.MSsubscription_articles
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END

			IF OBJECT_ID('dbo.MSsubscription_articlecolumns') IS NOT NULL
			BEGIN
				--
				-- drop MSsubscription_articlecolumns if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSsubscription_articlecolumns
				)
				BEGIN
				    DROP TABLE dbo.MSsubscription_articlecolumns
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END
			IF OBJECT_ID('dbo.MSreplication_queue') IS NOT NULL
			BEGIN
				--
				-- drop MSreplication_queue if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSreplication_queue
				)
				BEGIN
				    DROP TABLE dbo.MSreplication_queue
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END
			IF OBJECT_ID('dbo.MSrepl_queuedtraninfo') IS NOT NULL
			BEGIN
				--
				-- drop MSrepl_queuedtraninfo if empty
				-- 
				IF NOT EXISTS
				(
				    SELECT    *
				    FROM    dbo.MSrepl_queuedtraninfo
				)
				BEGIN
				    DROP TABLE dbo.MSrepl_queuedtraninfo
				    IF @@ERROR != 0 
				    BEGIN
				        RETURN(1)
				    END
				END
			END
		end
    END
    
    IF OBJECT_ID('MSsubscription_properties') IS NOT NULL
    BEGIN
        DELETE    dbo.MSsubscription_properties 
        WHERE    (
                    UPPER(publisher) = UPPER(@publisher) AND
                    publisher_db = @publisher_db AND
                    publication = @publication
                ) 
           OR    @reserved = 'drop_all'
 
        IF @@ERROR <> 0 
        BEGIN
            RETURN(1)
        END
    END

    -- These three tables must be dropped together (see sp_helppullsubscription)
    DECLARE @MSrs_exists bit,   -- MSreplication_subscriptions exists
            @MSsa_exists bit,   -- MSsubscription_agents exists
            @MSsp_exists bit,   -- MSsubscription_properties exists
            @MSrs_empty  bit,   -- MSreplication_subscriptions empty
            @MSsa_empty  bit,   -- MSsubscription_agents empty
            @MSsp_empty  bit    -- MSsubscription_properties empty

    SELECT  @MSrs_exists = ISNULL(OBJECT_ID('MSreplication_subscriptions'), 0),
            @MSsa_exists = ISNULL(OBJECT_ID('MSsubscription_agents'), 0),
            @MSsp_exists = ISNULL(OBJECT_ID('MSsubscription_properties'), 0),
            @MSrs_empty  = 0,
            @MSsa_empty  = 0,
            @MSsp_empty  = 0
    
    IF @MSrs_exists = 1
    BEGIN
        IF NOT EXISTS (SELECT * FROM MSreplication_subscriptions)
            SELECT @MSrs_empty = 1
    END
    
    IF @MSsa_exists = 1
    BEGIN
        IF NOT EXISTS (SELECT * FROM MSsubscription_agents)
            SELECT @MSsa_empty = 1
    END

    IF @MSsp_exists = 1
    BEGIN
        IF NOT EXISTS (SELECT * FROM MSsubscription_properties)
            SELECT @MSsp_empty = 1
    END

    -- only attempt a drop if all the tables either do not exist or are empty
    IF  (@MSrs_exists = 0 OR @MSrs_empty = 1) AND
        (@MSsa_exists = 0 OR @MSsa_empty = 1) AND
        (@MSsp_exists = 0 OR @MSsp_empty = 1)
    BEGIN
        IF @MSrs_exists = 1
        BEGIN
            DROP TABLE MSreplication_subscriptions
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END

        IF @MSsa_exists = 1
        BEGIN
            DROP TABLE MSsubscription_agents
            IF @@ERROR <> 0 
            BEGIN
                RETURN(1)
            END
        END

        IF @MSsp_exists = 1
        BEGIN
            EXEC @retcode = sys.sp_MSsub_cleanup_prop_table
            IF @@ERROR <> 0 OR @retcode <> 0
            BEGIN
                RETURN(1)
            END
        END
    END
        
    EXEC @retcode = sys.sp_resetsnapshotdeliveryprogress @drop_table = N'true'
    IF @retcode <> 0 or @@error <> 0
    BEGIN
        RETURN(1)
    END

	-- only attempt to drop the p2p tables if the db is not SQLDB
	-- 
	-- AND
	--  
	-- db is not published. if the db is published then the remaining 
	-- p2p tables will be removed when publish is turned off
	--
	-- OR
	--
	-- If we are in drop_all mode then get rid of the P2P 
	-- tables. This should only be true for sp_removedbreplication
	IF (serverproperty('EngineEdition') <> 5) AND (sys.fn_MSrepl_istranpublished(DB_NAME(),0) != 1
		OR @reserved = 'drop_all')
	BEGIN
		EXEC @retcode = sys.sp_MSdrop_peertopeer_tables @from_backup = @from_backup
		IF @retcode <> 0 or @@error <> 0
	    BEGIN
			RETURN(1)
	    END	
	END

    -- Ignore errors.
    EXEC sys.sp_MSsub_cleanup_orphans

    EXEC sys.sp_dropreplsymmetrickey @check_replication = 1, @throw_error = 0
    
    RETURN (0)
END

