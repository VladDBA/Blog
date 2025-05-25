use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE PROCEDURE sys.sp_MSadd_subscription
(
	@publisher sysname,
	@publisher_db sysname,
	@subscriber sysname,       
	@article_id int = NULL,
	@subscriber_db sysname = NULL,
	@status tinyint,                    -- 0 = inactive, 1 = subscribed, 2 = active 
	@subscription_seqno varbinary(16),  -- publisher's database sequence number 

	-- Post 6.5 parameters
	@publication sysname = NULL,    -- 6.x publishers will not provide this
	@article sysname = NULL,
	@subscription_type tinyint = 0,     -- 0 = push, 1 = pull, 2 = anonymous 
	@sync_type tinyint = 0,             -- 0 = none  1 = automatic snaphot  2 = no intial snapshot
	@snapshot_seqno_flag bit = 0,       -- 1 = subscription seqno is the snapshot seqno

	@frequency_type int = NULL,
	@frequency_interval int = NULL,
	@frequency_relative_interval int = NULL,
	@frequency_recurrence_factor int = NULL,
	@frequency_subday int = NULL,
	@frequency_subday_interval int = NULL,
	@active_start_time_of_day int = NULL,
	@active_end_time_of_day int = NULL,
	@active_start_date int = NULL,
	@active_end_date int = NULL,
	@optional_command_line nvarchar(4000) = '',

	-- synctran
	@update_mode tinyint = 0, -- 0=read only,1=sync tran,2=queued tran,3=failover, 
											-- 4=sqlqueued tran,5=sqlqueued failover,6=sqlqueued qfailover,7=qfailover
	@loopback_detection bit = 0,
	@distribution_jobid binary(16) = NULL OUTPUT,

	-- agent offload
	@offloadagent  bit = 0,
	@offloadserver sysname = NULL,

	-- If agent is already created, the package name will be ignored.		 
	@dts_package_name sysname = NULL,
	@dts_package_password nvarchar(524) = NULL,
	@dts_package_location	int = 0,
	@distribution_job_name sysname = NULL,
	@internal sysname = N'PRE-YUKON'	, -- Can be: 'PRE-YUKON', 'YUKON ADD SUB', 'YUKON ADD AGENT'
	@publisher_engine_edition int = NULL,
	@nosync_type tinyint = 0 -- 0(none), 1(replication support only), 2(initialize with backup), 3(initialize from lsn) 
)
as
begin
	set nocount on
	declare @publisher_id smallint
			,@subscriber_id smallint
			,@command nvarchar (4000)
			,@type tinyint
			,@database sysname
			,@long_name nvarchar (255)
			,@retcode int
			,@login sysname
			,@password nvarchar(524)
			,@retryattempts int
			,@retrydelay int
			,@virtual smallint                       -- const: virtual subscriber id 
			,@virtual_anonymous smallint                 -- const: virtual anonymous subscriber id 
			,@publication_str nvarchar (32)
			,@agent_id int
			,@publication_id int
			,@publication_type int
			,@independent_agent bit
			,@allow_pull bit
			,@active tinyint

			,@flushfrequency int 
			,@frequencytype int
			,@frequencyinterval int 
			,@frequencyrelativeinterval int
			,@frequencyrecurrencefactor int 
			,@frequencysubday int 
			,@frequencysubdayinterval int
			,@activestarttimeofday int
			,@activeendtimeofday int
			,@activestartdate int 
			,@activeenddate int 
			,@dsn_subscriber tinyint
			,@jet_subscriber tinyint
			,@oledb_subscriber tinyint
			,@thirdparty_flag bit
			,@subscribersecuritymode smallint
			,@subscriberlogin sysname
			,@subscriberpassword nvarchar(524)

			,@distributor_security_mode      int  -- 0 standard, 1 integrated
			,@distributor_login              sysname 
			,@distributor_password           nvarchar(524)
			,@publisher_database_id int
			,@platform_nt binary
			,@anonymous_agent_id int
			,@agent_name nvarchar(100)
			,@publication_name sysname
			,@subscriber_provider sysname
			,@new_pubdat_id int

	--
	-- security check
	-- only db_owner can execute this
	--
	if (is_member ('db_owner') != 1) 
	begin
		raiserror(14260, 16, -1)
		return (1)
	end
	--
	-- security check
	-- Has to be executed from distribution database
	--
	if (sys.fn_MSrepl_isdistdb (db_name()) != 1)
	begin
		raiserror(21482, 16, -1, 'sp_MSadd_subscription', 'distribution')
		return (1)
	end

	IF @offloadagent IS NOT NULL
		AND @offloadagent != 0
	BEGIN
		-- "Parameter '@offloadagent' is no longer supported."
		RAISERROR(21698, 16, -1, '@offloadagent')
		RETURN 1
	END

	IF ISNULL(@offloadserver, N'') != N''
	BEGIN
		-- "Parameter '@offloadserver' is no longer supported."
		RAISERROR(21698, 16, -1, '@offloadserver')
		RETURN 1
	END
	
	-- Store off publication name for dummy monitor row
	select @publication_name = @publication
			-- Defined in sqlrepl.h
			-- Set null @optional_command_line to empty string to avoid string concat problem
			,@optional_command_line = ISNULL(LTRIM(RTRIM(@optional_command_line)), N'')
			,@dsn_subscriber = 1 -- Const: subscriber type 'dsn' 
			,@jet_subscriber = 2   
			,@oledb_subscriber = 3   
			,@virtual = -1
			,@virtual_anonymous = -2
			,@active = 2
			,@platform_nt = 0x1

    -- Check if publisher is a defined as a distribution publisher in the current database
    exec @retcode = sys.sp_MSvalidate_distpublisher @publisher, @publisher_id OUTPUT
    if @retcode <> 0 or @@error <> 0
    begin
        return(1)
    end

    -- Check if subscriber exists
    if @subscriber is null
    begin
      select @subscriber_id = @virtual
      -- The following 2 variables are hardcoded in sp_MSget_repl_cmds_anonymous 
              ,@subscriber_db = N'virtual'
              ,@subscription_type = 0
    end
    else
    begin
        select @subscriber_id = msssrs.msrs_srvid,
                @subscriber_provider = providername, -- providername is not present in Msreplservers, so we need to use view MSsysservers_replservers as it joins sysservers and MSreplservers
                @type = type
        from MSsysservers_replservers msssrs, MSsubscriber_info where 
            UPPER(msssrs.msrs_srvname) = UPPER(@subscriber) and
            UPPER(subscriber) = UPPER(@subscriber) and
            UPPER(publisher) = UPPER(@publisher) 

        -- If the publisher version is lower than SQL version 15 (SQL 2019)
        -- then subscriber entry may be in format <server>,<port>. We strip the port number
        -- and try to get subscriber id.
        if @subscriber_id is NULL
        begin
            declare @subscriber_without_port sysname
			declare @subscriber_with_port sysname
            exec sys.sp_MSget_server_portinfo 
                @name = @subscriber, 
                @srvname = @subscriber_without_port OUTPUT,
                @srvname_with_port = @subscriber_with_port OUTPUT

            select @subscriber_id = msssrs.msrs_srvid,
                    @subscriber_provider = providername, -- providername is not present in Msreplservers, so we need to use view MSsysservers_replservers as it joins sysservers and MSreplservers
                    @type = type
            from MSsysservers_replservers msssrs, MSsubscriber_info where 
                UPPER(msssrs.msrs_srvname) = UPPER(@subscriber_without_port) and
                UPPER(subscriber) = UPPER(@subscriber_without_port) and
                UPPER(publisher) = UPPER(@publisher)
            
            if @subscriber_id is NULL
            begin
				declare @trace_number	int,
						@trace_status	bit
				set @trace_number = 15005 -- Trace flag to enable Subscriber with non-default port
				exec sys.sp_check_trace_enabled_globally @trace_number, @trace_status OUTPUT, 1 /*nomsgs*/

				if @trace_status = 1 -- if Subscriber with non-default port is enabled
				begin
					select @subscriber_id = msssrs.msrs_srvid,
						@subscriber_provider = providername,
						@type = type
					from MSsysservers_replservers msssrs, MSsubscriber_info 
						where UPPER(msssrs.msrs_srvname) = UPPER(@subscriber_with_port) and
						UPPER(subscriber) = UPPER(@subscriber_with_port) and
						UPPER(publisher) = UPPER(@publisher)
				end
            end
        end
    end

    if @subscriber_id is NULL
    begin
        raiserror (20032, 16, -1, @subscriber, @publisher) 
        return (1)
    end
    
    -- Special logic for 6.5 publisher.
    -- If publisher_id, publisher_db pair is not in MSpublisher_databases then add it.  This will be used
    -- to store a publisher_database_id in the MSrepl_transactions and MSrepl_commands table.
    if @publication is null
    begin
        if not exists (select * from MSpublisher_databases where publisher_id = @publisher_id and
                            publisher_db = @publisher_db)
        begin
            insert into MSpublisher_databases (publisher_id, publisher_db, publisher_engine_edition) 
                    values (@publisher_id, @publisher_db, @publisher_engine_edition)
            if @@error <> 0
            goto UNDO

			set @new_pubdat_id = SCOPE_IDENTITY()
			exec sys.sp_repl_generate_metadata_event
				@event = N'repl_metadata_change',
				@metadata = N'MSpublisher_databases',
				@id = @new_pubdat_id,
				@action = N'insert',
				@context = N'sp_MSadd_subscription',
				@description = N'In case of null publication.'
        end
    end

	-- Get publisher_database_id
	select @publisher_database_id = id from MSpublisher_databases where publisher_id = @publisher_id and
		publisher_db = @publisher_db
	if @@error <> 0
		return 1

	-- If publication exists this is a post 6.x publisher
	if @publication is not NULL
	begin
		select @publication_id = publication_id, @publication_type = publication_type,
			@independent_agent = independent_agent, @allow_pull = allow_pull,
			@thirdparty_flag = thirdparty_flag from 
			dbo.MSpublications where 
			publisher_id = @publisher_id and
			publisher_db = @publisher_db and
			publication = @publication
		if @publication_id is NULL
		begin
			raiserror (20026, 11, -1, @publication)
			return (1)
		end

		-- Check if article_id exists
		if @article_id is not NULL 
		begin
			if not exists (select * from MSarticles where 
				publisher_id = @publisher_id and
				publisher_db = @publisher_db and
				article_id = @article_id)
			begin
				raiserror (20027, 11, -1, @article) 
				return (1)
			end
		end

		-- Check if article exists
		if @article is not NULL and @article_id is NULL
		begin
			select @article_id = article_id from MSarticles where 
				publisher_id = @publisher_id and
				publisher_db = @publisher_db and
				article = @article
			if @article_id is NULL
			begin
				raiserror (20027, 11, -1, @article) 
				return (1)
			end
		end
	end
	else
	begin   -- Set 6.x publishing values
		select @publication_id = 0
				,@independent_agent = 0
				,@allow_pull = 0
				,@thirdparty_flag = 0
				,@publication_type = NULL
	end

	-- If the subscriber is an Oracle subscriber and loopback detection has been enabled
	-- verify that the subscriber is also a publisher
	if (UPPER(@subscriber_provider) = UPPER('OraOLEDB.Oracle') OR
		UPPER(@subscriber_provider) = UPPER('MSDAORA'))        AND
		@type = @oledb_subscriber							   AND
		@loopback_detection = 1
	begin
		-- All of the following settings are required for Oracle bi-directional publishing
		if NOT @sync_type = 2
		begin
			raiserror (21744, 16, -1, 'sync_type', 'none') 
			return 1
		end
		
		if NOT @subscription_type = 0
		begin
			raiserror (21744, 16, -1, 'subscription_type', 'push') 
			return 1
		end
		
		if NOT @status = 2
		begin
			raiserror (21744, 16, -1, 'status', 'active') 
			return 1
		end

		if NOT @independent_agent = 1
		begin
			raiserror (21744, 16, -1, 'independent_agent', 'true') 
			return 1
		end
		
		--exec @retcode = sys.sp_ORACheckLoopbackSupport @subscriber
		--if @retcode <> 0 or @@error <> 0
		--	return 1
	end

	-- Make sure subscription does not already exist
	if exists (select * from dbo.MSsubscriptions where 
		publisher_id = @publisher_id and 
		publisher_db = @publisher_db and 
		publication_id = @publication_id and
		article_id = @article_id and
		subscriber_id = @subscriber_id and
		subscriber_db = @subscriber_db)
	begin
		if @thirdparty_flag = 1
		begin
			raiserror (14058, 16, -1)
			return(1)
		end
		else
		begin
			exec @retcode = sys.sp_MSdrop_subscription
				@publisher = @publisher,
				@publisher_db = @publisher_db,
				@subscriber = @subscriber,
				@article_id = @article_id,
				@subscriber_db = @subscriber_db,
				@publication = @publication,
				@article = @article
			if @retcode <> 0 or @@error <> 0
			begin
				return(1)
			end
		end
	end
	-- Check to see if we need to add a new distribution agent for the subscription.
	-- It is database wide for non independent agent publications, and publication wide otherwise.
	-- Check to see if the distribution agent for this subscription is already added.

	select @agent_id = NULL
	select @agent_id = agent_id from 
		dbo.MSsubscriptions where
		publisher_id = @publisher_id and
		publisher_db = @publisher_db and
		subscription_type = @subscription_type and
		(publication_id = @publication_id or @independent_agent = 0) and
		independent_agent = @independent_agent and 
		subscriber_id = @subscriber_id and
		subscriber_db = @subscriber_db

	if @subscriber_id = @virtual
	begin
		select @anonymous_agent_id = agent_id from 
			dbo.MSsubscriptions where
			publisher_id = @publisher_id and
			publisher_db = @publisher_db and
			subscription_type = @subscription_type and
			(publication_id = @publication_id or @independent_agent = 0) and
			independent_agent = @independent_agent and 
			subscriber_id = @virtual_anonymous and
			subscriber_db = @subscriber_db
	end
	
	begin tran
	save transaction MSadd_subscription

	--
	-- SKU based subscription count check
	-- This should be done just before creating distribution agent
	-- and adding entry in MSsubscriptions
	--
	-- Skip this check for snapshot publications
	--
	if (@publication_type != 1)
	begin
		--
		-- Since subscription addition is happening at an article level
		-- do the subscription count check once per publication
		--
		if not exists (select * from dbo.MSsubscriptions 
					where publisher_id = @publisher_id 
						and publisher_db = @publisher_db 
						and publication_id = @publication_id 
						and subscriber_id = @subscriber_id 
						and subscriber_db = @subscriber_db
						and subscription_type = @subscription_type)
		begin
			--
			-- we are adding the first article subscription to this publication
			--
			exec @retcode = sys.sp_MScheck_subscription_count_internal @mode=0 
											,@publisher = @publisher
											,@publisher_engine_edition = @publisher_engine_edition
											,@about_to_insert_new_subscription = 1
			if (@retcode != 0 or @@error != 0)
				goto UNDO
		end
	end
	--
	-- process the distribution agent
	--
	if @agent_id is NOT NULL
	begin
		select @distribution_jobid = job_id from MSdistribution_agents
			where id = @agent_id
	end
	else
	begin
		-- Create distribution agent
		-- Do not create local job if
		-- 1. virtual subscription 
		-- 2. no subscriber information, return (6.x legacy)
		-- 3. pull (this sp will not be called for anonymous subscription)
	
		declare @local_job bit

		-- Get subscriber without port number
		declare @sub_without_port sysname
		exec sys.sp_MSget_server_portinfo 
			@name = @subscriber, 
			@srvname = @sub_without_port OUTPUT

		if @subscriber_id = @virtual or 
			not exists (select * from MSsubscriber_info where
				UPPER(publisher) = UPPER(@publisher) and 
				UPPER(subscriber) in (UPPER(@subscriber), UPPER(@sub_without_port))) OR 
			@subscription_type = 1 

			select @local_job = 0
		else
			select @local_job = 1

		-- 'ALL' is reserved for indication all publications
		-- Hardcoded in sp_MSenum*... 
		-- Note! @publication is overwritten
		
		if @independent_agent = 0
			select @publication = 'ALL'

		if @local_job = 1
		begin
			select 
				@frequencytype = frequency_type,
				@frequencyinterval = frequency_interval,
				@frequencyrelativeinterval = frequency_relative_interval,
				@frequencyrecurrencefactor = frequency_recurrence_factor,
				@frequencysubday = frequency_subday,
				@frequencysubdayinterval = frequency_subday_interval,
				@activestarttimeofday = active_start_time_of_day,
				@activeendtimeofday = active_end_time_of_day,
				@activestartdate = active_start_date,
				@activeenddate = active_end_date
			from MSsubscriber_schedule 
			where UPPER(publisher) = UPPER(@publisher) and UPPER(subscriber) = UPPER(@subscriber) and agent_type = 0    
			
			if @frequency_type is null
				select @frequency_type = @frequencytype

			if @frequency_interval  is null
				select  @frequency_interval = @frequencyinterval

			if @frequency_relative_interval is null
				select  @frequency_relative_interval = @frequencyrelativeinterval

			if @frequency_recurrence_factor is null
				select  @frequency_recurrence_factor = @frequencyrecurrencefactor

			if @frequency_subday is null
				select  @frequency_subday = @frequencysubday

			if @frequency_subday_interval is null
				select  @frequency_subday_interval = @frequencysubdayinterval

			if @active_start_time_of_day is null
				select  @active_start_time_of_day = @activestarttimeofday

			if @active_end_time_of_day is null
				select  @active_end_time_of_day = @activeendtimeofday

			if @active_start_date is null
				select  @active_start_date = @activestartdate

			if @active_end_date is null
				select  @active_end_date = @activeenddate

			execute @retcode = sys.sp_MSadd_distribution_agent
				@publisher_id = @publisher_id,
				@publisher_db = @publisher_db,
				@publication = @publication,
				@subscriber_id = @subscriber_id,
				@subscriber_db = @subscriber_db,
				@subscription_type = @subscription_type,
				@local_job = @local_job,
				@frequency_type = @frequency_type,
				@frequency_interval = @frequency_interval,
				@frequency_subday = @frequency_subday,
				@frequency_subday_interval = @frequency_subday_interval,
				@frequency_relative_interval = @frequency_relative_interval,
				@frequency_recurrence_factor = @frequency_recurrence_factor,
				@active_start_date = @active_start_date,
				@active_end_date = @active_end_date,
				@active_start_time_of_day = @active_start_time_of_day,
				@active_end_time_of_day = @active_end_time_of_day,
				@command = @optional_command_line,
				@agent_id = @agent_id OUTPUT,
				@distribution_jobid = @distribution_jobid OUTPUT,
				@update_mode = @update_mode,
				@dts_package_name = @dts_package_name,
				@dts_package_password = @dts_package_password,
				@dts_package_location = @dts_package_location,
				@name = @distribution_job_name,
				@internal = @internal
			if @@error <> 0 or @retcode <> 0
				goto UNDO
		end
		else
		begin
			execute @retcode = sys.sp_MSadd_distribution_agent
				@publisher_id = @publisher_id,
				@publisher_db = @publisher_db,
				@publication = @publication,
				@subscriber_id = @subscriber_id,
				@subscriber_db = @subscriber_db,
				@subscription_type = @subscription_type,
				@local_job = @local_job,
				@agent_id = @agent_id OUTPUT,
				@distribution_jobid = @distribution_jobid OUTPUT,
				@update_mode = @update_mode
				-- Only push has distributor side package.

			if @@error <> 0 or @retcode <> 0
				goto UNDO
		end

		if @subscriber_id = @virtual
		begin
			execute @retcode = sys.sp_MSadd_distribution_agent
				@publisher_id = @publisher_id,
				@publisher_db = @publisher_db,
				@publication = @publication,
				@subscriber_id = @virtual_anonymous,
				@subscriber_db = @subscriber_db,
				@subscription_type = @subscription_type,
				@local_job = @local_job,
				@agent_id = @anonymous_agent_id OUTPUT,
				@distribution_jobid = @distribution_jobid OUTPUT,
				@update_mode = @update_mode
				-- No need to specify offload parameters for virtual agents
				-- No need to specify package name for virtual agents
		end
	end

	insert into dbo.MSsubscriptions values (@publisher_database_id, @publisher_id, @publisher_db, @publication_id,
		@article_id, @subscriber_id, @subscriber_db, @subscription_type, @sync_type, @status, 
		@subscription_seqno, @snapshot_seqno_flag, @independent_agent, getdate(), 
		@loopback_detection, @agent_id, @update_mode, @subscription_seqno, @subscription_seqno, @nosync_type)
	if @@error <> 0
		goto UNDO

	-- For shiloh, always add virtual anonymous entry for attach logic
	-- If anonymous publication, add "virtual anonymous" subscription
	-- when adding the virtual subscription
	if @subscriber_id = @virtual
	begin
		insert into dbo.MSsubscriptions values (@publisher_database_id, @publisher_id, @publisher_db, @publication_id,
			@article_id, @virtual_anonymous, @subscriber_db, @subscription_type, @sync_type, @status, 
			@subscription_seqno, @snapshot_seqno_flag, @independent_agent, getdate(), 
			@loopback_detection, @anonymous_agent_id, @update_mode, @subscription_seqno, @subscription_seqno, @nosync_type)
		if @@error <> 0
			goto UNDO
	end

	-- Check to see if we need to add a new qreader agent
	if (@update_mode in (2,3,4,5,6,7))
	begin
		--
		-- we can have only one agent for the distribution database
		--
		if not exists (select * from dbo.MSqreader_agents) 
		begin
			-- if at this point the @internal IS 'PRE-YUKON' we know that 
			-- the caller (publisher side) is yukonplus. In this case we  
			-- must let the user know how to create the qreader with a strict 
			-- security policy else we allow the creation without login/pwd
			if @internal != 'PRE-YUKON'
			begin
				-- The 'qreader' agent job must be added via 'sp_addqreader_agent' before continuing. Please see the documentation for 'sp_addqreader_agent'.
				RAISERROR(21798, 16, -1, 'qreader', 'sp_addqreader_agent', 'sp_addqreader_agent')			
				goto UNDO
			end

			execute @retcode = sys.sp_MSadd_qreader_agent
			if (@retcode != 0 or @@error != 0)
				goto UNDO
		end
	end

	commit transaction

	return(0)

UNDO:
	if @@TRANCOUNT > 0
	begin
		ROLLBACK TRAN MSadd_subscription
		COMMIT TRAN
	end
	return(1)
end


/*====  SQL Server 2022 version  ====*/
CREATE PROCEDURE sys.sp_MSadd_subscription
(
	@publisher sysname,
	@publisher_db sysname,
	@subscriber sysname,       
	@article_id int = NULL,
	@subscriber_db sysname = NULL,
	@status tinyint,                    -- 0 = inactive, 1 = subscribed, 2 = active 
	@subscription_seqno varbinary(16),  -- publisher's database sequence number 

	-- Post 6.5 parameters
	@publication sysname = NULL,    -- 6.x publishers will not provide this
	@article sysname = NULL,
	@subscription_type tinyint = 0,     -- 0 = push, 1 = pull, 2 = anonymous 
	@sync_type tinyint = 0,             -- 0 = none  1 = automatic snaphot  2 = no intial snapshot
	@snapshot_seqno_flag bit = 0,       -- 1 = subscription seqno is the snapshot seqno

	@frequency_type int = NULL,
	@frequency_interval int = NULL,
	@frequency_relative_interval int = NULL,
	@frequency_recurrence_factor int = NULL,
	@frequency_subday int = NULL,
	@frequency_subday_interval int = NULL,
	@active_start_time_of_day int = NULL,
	@active_end_time_of_day int = NULL,
	@active_start_date int = NULL,
	@active_end_date int = NULL,
	@optional_command_line nvarchar(4000) = '',

	-- synctran
	@update_mode tinyint = 0, -- 0=read only,1=sync tran,2=queued tran,3=failover, 
											-- 4=sqlqueued tran,5=sqlqueued failover,6=sqlqueued qfailover,7=qfailover
	@loopback_detection bit = 0,
	@distribution_jobid binary(16) = NULL OUTPUT,

	-- agent offload
	@offloadagent  bit = 0,
	@offloadserver sysname = NULL,

	-- If agent is already created, the package name will be ignored.		 
	@dts_package_name sysname = NULL,
	@dts_package_password nvarchar(524) = NULL,
	@dts_package_location	int = 0,
	@distribution_job_name sysname = NULL,
	@internal sysname = N'PRE-YUKON'	, -- Can be: 'PRE-YUKON', 'YUKON ADD SUB', 'YUKON ADD AGENT'
	@publisher_engine_edition int = NULL,
	@nosync_type tinyint = 0 -- 0(none), 1(replication support only), 2(initialize with backup), 3(initialize from lsn) 
)
as
begin
	set nocount on
	declare @publisher_id smallint
			,@subscriber_id smallint
			,@command nvarchar (4000)
			,@type tinyint
			,@database sysname
			,@long_name nvarchar (255)
			,@retcode int
			,@login sysname
			,@password nvarchar(524)
			,@retryattempts int
			,@retrydelay int
			,@virtual smallint                       -- const: virtual subscriber id 
			,@virtual_anonymous smallint                 -- const: virtual anonymous subscriber id 
			,@publication_str nvarchar (32)
			,@agent_id int
			,@publication_id int
			,@publication_type int
			,@independent_agent bit
			,@allow_pull bit
			,@active tinyint

			,@flushfrequency int 
			,@frequencytype int
			,@frequencyinterval int 
			,@frequencyrelativeinterval int
			,@frequencyrecurrencefactor int 
			,@frequencysubday int 
			,@frequencysubdayinterval int
			,@activestarttimeofday int
			,@activeendtimeofday int
			,@activestartdate int 
			,@activeenddate int 
			,@dsn_subscriber tinyint
			,@jet_subscriber tinyint
			,@oledb_subscriber tinyint
			,@thirdparty_flag bit
			,@subscribersecuritymode smallint
			,@subscriberlogin sysname
			,@subscriberpassword nvarchar(524)

			,@distributor_security_mode      int  -- 0 standard, 1 integrated
			,@distributor_login              sysname 
			,@distributor_password           nvarchar(524)
			,@publisher_database_id int
			,@platform_nt binary
			,@anonymous_agent_id int
			,@agent_name nvarchar(100)
			,@publication_name sysname
			,@subscriber_provider sysname
			,@new_pubdat_id int

	--
	-- security check
	-- only db_owner can execute this
	--
	if (is_member ('db_owner') != 1) 
	begin
		raiserror(14260, 16, -1)
		return (1)
	end
	--
	-- security check
	-- Has to be executed from distribution database
	--
	if (sys.fn_MSrepl_isdistdb (db_name()) != 1)
	begin
		raiserror(21482, 16, -1, 'sp_MSadd_subscription', 'distribution')
		return (1)
	end

	IF @offloadagent IS NOT NULL
		AND @offloadagent != 0
	BEGIN
		-- "Parameter '@offloadagent' is no longer supported."
		RAISERROR(21698, 16, -1, '@offloadagent')
		RETURN 1
	END

	IF ISNULL(@offloadserver, N'') != N''
	BEGIN
		-- "Parameter '@offloadserver' is no longer supported."
		RAISERROR(21698, 16, -1, '@offloadserver')
		RETURN 1
	END
	
	-- Store off publication name for dummy monitor row
	select @publication_name = @publication
			-- Defined in sqlrepl.h
			-- Set null @optional_command_line to empty string to avoid string concat problem
			,@optional_command_line = ISNULL(LTRIM(RTRIM(@optional_command_line)), N'')
			,@dsn_subscriber = 1 -- Const: subscriber type 'dsn' 
			,@jet_subscriber = 2   
			,@oledb_subscriber = 3   
			,@virtual = -1
			,@virtual_anonymous = -2
			,@active = 2
			,@platform_nt = 0x1

    -- Check if publisher is a defined as a distribution publisher in the current database
    exec @retcode = sys.sp_MSvalidate_distpublisher @publisher, @publisher_id OUTPUT
    if @retcode <> 0 or @@error <> 0
    begin
        return(1)
    end

    -- Check if subscriber exists
    if @subscriber is null
    begin
      select @subscriber_id = @virtual
      -- The following 2 variables are hardcoded in sp_MSget_repl_cmds_anonymous 
              ,@subscriber_db = N'virtual'
              ,@subscription_type = 0
    end
    else
    begin
        select @subscriber_id = msssrs.msrs_srvid,
                @subscriber_provider = providername, -- providername is not present in Msreplservers, so we need to use view MSsysservers_replservers as it joins sysservers and MSreplservers
                @type = type
        from MSsysservers_replservers msssrs, MSsubscriber_info where 
            UPPER(msssrs.msrs_srvname) = UPPER(@subscriber) and
            UPPER(subscriber) = UPPER(@subscriber) and
            UPPER(publisher) = UPPER(@publisher) 

        -- If the publisher version is lower than SQL version 15 (SQL 2019)
        -- then subscriber entry may be in format <server>,<port>. We strip the port number
        -- and try to get subscriber id.
        if @subscriber_id is NULL
        begin
            declare @subscriber_without_port sysname
			declare @subscriber_with_port sysname
            exec sys.sp_MSget_server_portinfo 
                @name = @subscriber, 
                @srvname = @subscriber_without_port OUTPUT,
                @srvname_with_port = @subscriber_with_port OUTPUT

            select @subscriber_id = msssrs.msrs_srvid,
                    @subscriber_provider = providername, -- providername is not present in Msreplservers, so we need to use view MSsysservers_replservers as it joins sysservers and MSreplservers
                    @type = type
            from MSsysservers_replservers msssrs, MSsubscriber_info where 
                UPPER(msssrs.msrs_srvname) = UPPER(@subscriber_without_port) and
                UPPER(subscriber) = UPPER(@subscriber_without_port) and
                UPPER(publisher) = UPPER(@publisher)
            
            if @subscriber_id is NULL
            begin
				declare @trace_number	int,
						@trace_status	bit
				set @trace_number = 15005 -- Trace flag to enable Subscriber with non-default port
				exec sys.sp_check_trace_enabled_globally @trace_number, @trace_status OUTPUT

				if @trace_status = 1 -- if Subscriber with non-default port is enabled
				begin
					select @subscriber_id = msssrs.msrs_srvid,
						@subscriber_provider = providername,
						@type = type
					from MSsysservers_replservers msssrs, MSsubscriber_info where 
						UPPER(msssrs.msrs_srvname) = UPPER(@subscriber_with_port) and
						UPPER(subscriber) = UPPER(@subscriber_with_port) and
						UPPER(publisher) = UPPER(@publisher)
				end
            end
        end
    end

    if @subscriber_id is NULL
    begin
        raiserror (20032, 16, -1, @subscriber, @publisher) 
        return (1)
    end
    
    -- Special logic for 6.5 publisher.
    -- If publisher_id, publisher_db pair is not in MSpublisher_databases then add it.  This will be used
    -- to store a publisher_database_id in the MSrepl_transactions and MSrepl_commands table.
    if @publication is null
    begin
        if not exists (select * from MSpublisher_databases where publisher_id = @publisher_id and
                            publisher_db = @publisher_db)
        begin
            insert into MSpublisher_databases (publisher_id, publisher_db, publisher_engine_edition) 
                    values (@publisher_id, @publisher_db, @publisher_engine_edition)
            if @@error <> 0
            goto UNDO

			set @new_pubdat_id = SCOPE_IDENTITY()
			exec sys.sp_repl_generate_metadata_event
				@event = N'repl_metadata_change',
				@metadata = N'MSpublisher_databases',
				@id = @new_pubdat_id,
				@action = N'insert',
				@context = N'sp_MSadd_subscription',
				@description = N'In case of null publication.'
        end
    end

	-- Get publisher_database_id
	select @publisher_database_id = id from MSpublisher_databases where publisher_id = @publisher_id and
		publisher_db = @publisher_db
	if @@error <> 0
		return 1

	-- If publication exists this is a post 6.x publisher
	if @publication is not NULL
	begin
		select @publication_id = publication_id, @publication_type = publication_type,
			@independent_agent = independent_agent, @allow_pull = allow_pull,
			@thirdparty_flag = thirdparty_flag from 
			dbo.MSpublications where 
			publisher_id = @publisher_id and
			publisher_db = @publisher_db and
			publication = @publication
		if @publication_id is NULL
		begin
			raiserror (20026, 11, -1, @publication)
			return (1)
		end

		-- Check if article_id exists
		if @article_id is not NULL 
		begin
			if not exists (select * from MSarticles where 
				publisher_id = @publisher_id and
				publisher_db = @publisher_db and
				article_id = @article_id)
			begin
				raiserror (20027, 11, -1, @article) 
				return (1)
			end
		end

		-- Check if article exists
		if @article is not NULL and @article_id is NULL
		begin
			select @article_id = article_id from MSarticles where 
				publisher_id = @publisher_id and
				publisher_db = @publisher_db and
				article = @article
			if @article_id is NULL
			begin
				raiserror (20027, 11, -1, @article) 
				return (1)
			end
		end
	end
	else
	begin   -- Set 6.x publishing values
		select @publication_id = 0
				,@independent_agent = 0
				,@allow_pull = 0
				,@thirdparty_flag = 0
				,@publication_type = NULL
	end

	-- If the subscriber is an Oracle subscriber and loopback detection has been enabled
	-- verify that the subscriber is also a publisher
	if (UPPER(@subscriber_provider) = UPPER('OraOLEDB.Oracle') OR
		UPPER(@subscriber_provider) = UPPER('MSDAORA'))        AND
		@type = @oledb_subscriber							   AND
		@loopback_detection = 1
	begin
		-- All of the following settings are required for Oracle bi-directional publishing
		if NOT @sync_type = 2
		begin
			raiserror (21744, 16, -1, 'sync_type', 'none') 
			return 1
		end
		
		if NOT @subscription_type = 0
		begin
			raiserror (21744, 16, -1, 'subscription_type', 'push') 
			return 1
		end
		
		if NOT @status = 2
		begin
			raiserror (21744, 16, -1, 'status', 'active') 
			return 1
		end

		if NOT @independent_agent = 1
		begin
			raiserror (21744, 16, -1, 'independent_agent', 'true') 
			return 1
		end
		
		--exec @retcode = sys.sp_ORACheckLoopbackSupport @subscriber
		--if @retcode <> 0 or @@error <> 0
		--	return 1
	end

	-- Make sure subscription does not already exist
	if exists (select * from dbo.MSsubscriptions where 
		publisher_id = @publisher_id and 
		publisher_db = @publisher_db and 
		publication_id = @publication_id and
		article_id = @article_id and
		subscriber_id = @subscriber_id and
		subscriber_db = @subscriber_db)
	begin
		if @thirdparty_flag = 1
		begin
			raiserror (14058, 16, -1)
			return(1)
		end
		else
		begin
			exec @retcode = sys.sp_MSdrop_subscription
				@publisher = @publisher,
				@publisher_db = @publisher_db,
				@subscriber = @subscriber,
				@article_id = @article_id,
				@subscriber_db = @subscriber_db,
				@publication = @publication,
				@article = @article
			if @retcode <> 0 or @@error <> 0
			begin
				return(1)
			end
		end
	end
	-- Check to see if we need to add a new distribution agent for the subscription.
	-- It is database wide for non independent agent publications, and publication wide otherwise.
	-- Check to see if the distribution agent for this subscription is already added.

	select @agent_id = NULL
	select @agent_id = agent_id from 
		dbo.MSsubscriptions where
		publisher_id = @publisher_id and
		publisher_db = @publisher_db and
		subscription_type = @subscription_type and
		(publication_id = @publication_id or @independent_agent = 0) and
		independent_agent = @independent_agent and 
		subscriber_id = @subscriber_id and
		subscriber_db = @subscriber_db

	if @subscriber_id = @virtual
	begin
		select @anonymous_agent_id = agent_id from 
			dbo.MSsubscriptions where
			publisher_id = @publisher_id and
			publisher_db = @publisher_db and
			subscription_type = @subscription_type and
			(publication_id = @publication_id or @independent_agent = 0) and
			independent_agent = @independent_agent and 
			subscriber_id = @virtual_anonymous and
			subscriber_db = @subscriber_db
	end
	
	begin tran
	save transaction MSadd_subscription

	--
	-- SKU based subscription count check
	-- This should be done just before creating distribution agent
	-- and adding entry in MSsubscriptions
	--
	-- Skip this check for snapshot publications
	--
	if (@publication_type != 1)
	begin
		--
		-- Since subscription addition is happening at an article level
		-- do the subscription count check once per publication
		--
		if not exists (select * from dbo.MSsubscriptions 
					where publisher_id = @publisher_id 
						and publisher_db = @publisher_db 
						and publication_id = @publication_id 
						and subscriber_id = @subscriber_id 
						and subscriber_db = @subscriber_db
						and subscription_type = @subscription_type)
		begin
			--
			-- we are adding the first article subscription to this publication
			--
			exec @retcode = sys.sp_MScheck_subscription_count_internal @mode=0 
											,@publisher = @publisher
											,@publisher_engine_edition = @publisher_engine_edition
											,@about_to_insert_new_subscription = 1
			if (@retcode != 0 or @@error != 0)
				goto UNDO
		end
	end
	--
	-- process the distribution agent
	--
	if @agent_id is NOT NULL
	begin
		select @distribution_jobid = job_id from MSdistribution_agents
			where id = @agent_id
	end
	else
	begin
		-- Create distribution agent
		-- Do not create local job if
		-- 1. virtual subscription 
		-- 2. no subscriber information, return (6.x legacy)
		-- 3. pull (this sp will not be called for anonymous subscription)
	
		declare @local_job bit

		-- Get subscriber without port number
		declare @sub_without_port sysname
		exec sys.sp_MSget_server_portinfo 
			@name = @subscriber, 
			@srvname = @sub_without_port OUTPUT

		if @subscriber_id = @virtual or 
			not exists (select * from MSsubscriber_info where
				UPPER(publisher) = UPPER(@publisher) and 
				UPPER(subscriber) in (UPPER(@subscriber), UPPER(@sub_without_port))) OR 
			@subscription_type = 1 

			select @local_job = 0
		else
			select @local_job = 1

		-- 'ALL' is reserved for indication all publications
		-- Hardcoded in sp_MSenum*... 
		-- Note! @publication is overwritten
		
		if @independent_agent = 0
			select @publication = 'ALL'

		if @local_job = 1
		begin
			select 
				@frequencytype = frequency_type,
				@frequencyinterval = frequency_interval,
				@frequencyrelativeinterval = frequency_relative_interval,
				@frequencyrecurrencefactor = frequency_recurrence_factor,
				@frequencysubday = frequency_subday,
				@frequencysubdayinterval = frequency_subday_interval,
				@activestarttimeofday = active_start_time_of_day,
				@activeendtimeofday = active_end_time_of_day,
				@activestartdate = active_start_date,
				@activeenddate = active_end_date
			from MSsubscriber_schedule 
			where UPPER(publisher) = UPPER(@publisher) and UPPER(subscriber) = UPPER(@subscriber) and agent_type = 0    
			
			if @frequency_type is null
				select @frequency_type = @frequencytype

			if @frequency_interval  is null
				select  @frequency_interval = @frequencyinterval

			if @frequency_relative_interval is null
				select  @frequency_relative_interval = @frequencyrelativeinterval

			if @frequency_recurrence_factor is null
				select  @frequency_recurrence_factor = @frequencyrecurrencefactor

			if @frequency_subday is null
				select  @frequency_subday = @frequencysubday

			if @frequency_subday_interval is null
				select  @frequency_subday_interval = @frequencysubdayinterval

			if @active_start_time_of_day is null
				select  @active_start_time_of_day = @activestarttimeofday

			if @active_end_time_of_day is null
				select  @active_end_time_of_day = @activeendtimeofday

			if @active_start_date is null
				select  @active_start_date = @activestartdate

			if @active_end_date is null
				select  @active_end_date = @activeenddate

			execute @retcode = sys.sp_MSadd_distribution_agent
				@publisher_id = @publisher_id,
				@publisher_db = @publisher_db,
				@publication = @publication,
				@subscriber_id = @subscriber_id,
				@subscriber_db = @subscriber_db,
				@subscription_type = @subscription_type,
				@local_job = @local_job,
				@frequency_type = @frequency_type,
				@frequency_interval = @frequency_interval,
				@frequency_subday = @frequency_subday,
				@frequency_subday_interval = @frequency_subday_interval,
				@frequency_relative_interval = @frequency_relative_interval,
				@frequency_recurrence_factor = @frequency_recurrence_factor,
				@active_start_date = @active_start_date,
				@active_end_date = @active_end_date,
				@active_start_time_of_day = @active_start_time_of_day,
				@active_end_time_of_day = @active_end_time_of_day,
				@command = @optional_command_line,
				@agent_id = @agent_id OUTPUT,
				@distribution_jobid = @distribution_jobid OUTPUT,
				@update_mode = @update_mode,
				@dts_package_name = @dts_package_name,
				@dts_package_password = @dts_package_password,
				@dts_package_location = @dts_package_location,
				@name = @distribution_job_name,
				@internal = @internal
			if @@error <> 0 or @retcode <> 0
				goto UNDO
		end
		else
		begin
			execute @retcode = sys.sp_MSadd_distribution_agent
				@publisher_id = @publisher_id,
				@publisher_db = @publisher_db,
				@publication = @publication,
				@subscriber_id = @subscriber_id,
				@subscriber_db = @subscriber_db,
				@subscription_type = @subscription_type,
				@local_job = @local_job,
				@agent_id = @agent_id OUTPUT,
				@distribution_jobid = @distribution_jobid OUTPUT,
				@update_mode = @update_mode
				-- Only push has distributor side package.

			if @@error <> 0 or @retcode <> 0
				goto UNDO
		end

		if @subscriber_id = @virtual
		begin
			execute @retcode = sys.sp_MSadd_distribution_agent
				@publisher_id = @publisher_id,
				@publisher_db = @publisher_db,
				@publication = @publication,
				@subscriber_id = @virtual_anonymous,
				@subscriber_db = @subscriber_db,
				@subscription_type = @subscription_type,
				@local_job = @local_job,
				@agent_id = @anonymous_agent_id OUTPUT,
				@distribution_jobid = @distribution_jobid OUTPUT,
				@update_mode = @update_mode
				-- No need to specify offload parameters for virtual agents
				-- No need to specify package name for virtual agents
		end
	end

	insert into dbo.MSsubscriptions values (@publisher_database_id, @publisher_id, @publisher_db, @publication_id,
		@article_id, @subscriber_id, @subscriber_db, @subscription_type, @sync_type, @status, 
		@subscription_seqno, @snapshot_seqno_flag, @independent_agent, getdate(), 
		@loopback_detection, @agent_id, @update_mode, @subscription_seqno, @subscription_seqno, @nosync_type)
	if @@error <> 0
		goto UNDO

	-- For shiloh, always add virtual anonymous entry for attach logic
	-- If anonymous publication, add "virtual anonymous" subscription
	-- when adding the virtual subscription
	if @subscriber_id = @virtual
	begin
		insert into dbo.MSsubscriptions values (@publisher_database_id, @publisher_id, @publisher_db, @publication_id,
			@article_id, @virtual_anonymous, @subscriber_db, @subscription_type, @sync_type, @status, 
			@subscription_seqno, @snapshot_seqno_flag, @independent_agent, getdate(), 
			@loopback_detection, @anonymous_agent_id, @update_mode, @subscription_seqno, @subscription_seqno, @nosync_type)
		if @@error <> 0
			goto UNDO
	end

	-- Check to see if we need to add a new qreader agent
	if (@update_mode in (2,3,4,5,6,7))
	begin
		--
		-- we can have only one agent for the distribution database
		--
		if not exists (select * from dbo.MSqreader_agents) 
		begin
			-- if at this point the @internal IS 'PRE-YUKON' we know that 
			-- the caller (publisher side) is yukonplus. In this case we  
			-- must let the user know how to create the qreader with a strict 
			-- security policy else we allow the creation without login/pwd
			if @internal != 'PRE-YUKON'
			begin
				-- The 'qreader' agent job must be added via 'sp_addqreader_agent' before continuing. Please see the documentation for 'sp_addqreader_agent'.
				RAISERROR(21798, 16, -1, 'qreader', 'sp_addqreader_agent', 'sp_addqreader_agent')			
				goto UNDO
			end

			execute @retcode = sys.sp_MSadd_qreader_agent
			if (@retcode != 0 or @@error != 0)
				goto UNDO
		end
	end

	commit transaction

	return(0)

UNDO:
	if @@TRANCOUNT > 0
	begin
		ROLLBACK TRAN MSadd_subscription
		COMMIT TRAN
	end
	return(1)
end

