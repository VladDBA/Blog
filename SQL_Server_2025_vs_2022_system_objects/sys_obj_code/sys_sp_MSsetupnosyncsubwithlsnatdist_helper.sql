use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
--
-- Name: sp_MSsetupnosyncsubwithlsnatdist_helper
-- 
-- Description: activate nosync subscription at distributor
-- and return the article id corresponding to the article;
-- when article = N'all', return the first retrieved article id in MSarticles;
-- create a temporary table (named 'MSnosyncsubsetup') in distribution db
-- if the table does not exist, and update the table with the parameters 
-- to be needed for setting up of the specified non-sync subscription.
--
-- Security: Procedural security check is performed inside this procedure to
--           ensure that the caller is a member of sysadmin. Execute 
--           permission of this procedure is granted to public. This procedure 
--           is invoked via RPC
--
create procedure sys.sp_MSsetupnosyncsubwithlsnatdist_helper
(
	-- Publication properties
	@publisher                              sysname,
	@publisher_db                         sysname,
	@publication                            sysname,
	@article                                   sysname = N'all',
	-- Subscription properties
	@subscriber                             sysname,
	@destination_db                       sysname,
	@subscriptionlsn                       binary(10),
	@lsnsource                               int,
	@pubid                                     int,
	@publisher_db_version              int,
	@script_txt                               nvarchar(max),
	@nosync_setup_script               nvarchar(max),
	@next_valid_lsn                        binary(10),
	-- Output article id
	@artid                                      int = NULL OUTPUT
)
as
begin 
	set nocount on
	declare @publisherid int,
			@subscriberid int,
			@publisher_database_id int,
			@publication_id int,
			@nosyncCommandStr nvarchar(max)

	-- Security check 
	if (isnull(is_srvrolemember('sysadmin'),0) = 0)
	begin
		raiserror(21089, 16, -1)
		return 1
	end
	
	exec sys.sp_MSget_server_portinfo 
			@name = @publisher, 
			@srvname = @publisher OUTPUT

	DECLARE @trace_number	int,
			@trace_status	bit
	SET @trace_number = 15005 -- Trace flag to enable Subscriber with non-default port in AG scenario
	EXEC sys.sp_check_trace_enabled_globally @trace_number, @trace_status OUTPUT, 1 /*nomsgs*/

	IF @trace_status = 0 -- if Subscriber with non-default port is disabled
	BEGIN
		exec sys.sp_MSget_server_portinfo 
			@name = @subscriber, 
			@srvname = @subscriber OUTPUT
	END

    -- Obtain Publisher's server id
    select @publisherid = srvid 
      from MSreplservers
     where upper(srvname) = upper(@publisher) collate database_default
    if @@error<>0 goto Failure

    -- Obtain Subscriber's server id
    select @subscriberid = srvid
      from MSreplservers
     where upper(srvname) = upper(@subscriber) collate database_default
    if @@error<>0 goto Failure

	-- Find out what the publisher database id is
	select @publisher_database_id = id
	  from dbo.MSpublisher_databases
	 where publisher_db = @publisher_db
	   and publisher_id = @publisherid
	if @@error<>0 goto Failure

	-- Obtain the publication id
	select @publication_id = publication_id
	  from dbo.MSpublications
	 where publisher_id = @publisherid
	   and publisher_db = @publisher_db
	   and publication = @publication
	if @@error<>0 goto Failure

	-- Obtain artid if this is for an incremental article
	-- or one of the artids if @article equals to N'all'.
	select @artid = article_id    
	from MSarticles
	where publisher_id = @publisherid
	   and publisher_db = @publisher_db 
	   and publication_id = @publication_id
	   and (@article = N'all' or article = @article)
	if @@error<>0 or @artid is NULL goto Failure

	begin tran nosyncSubSetupHelper
	save tran nosyncSubSetupHelper

	-- Activate subscriptions in dbo.MSsubscriptions
	update dbo.MSsubscriptions
	   set status = 2 --active status
	 where publisher_database_id = @publisher_database_id
	   and publisher_id = @publisherid
	   and publisher_db = @publisher_db -- Extra insurance
	   and publication_id = @publication_id
	   and subscriber_id = @subscriberid
	   and subscriber_db = @destination_db
	   and (@article = N'all' or article_id = @artid)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	-- The table MSnosyncsubsetup should already exist
	-- in distribution database.
	if object_id(N'dbo.MSnosyncsubsetup', 'U') is NULL 
		goto nosyncSubSetupHelperFailure

	-- Try to clean up the table before adding parameters (rows)
	-- into the table for the specified nonsync subscription.
	-- Note that, in MSnosyncsubsetup table, we need the publication 
	-- id (i.e., @publid) from publisher db instead of the publication
	-- id (i.e., MSpublications.publication_id) from distribution db, 
	-- which might be different.
	delete dbo.MSnosyncsubsetup 
	where publisher_database_id = @publisher_database_id
	  and publication_id = @pubid
	  and artid = @artid
	  and next_valid_lsn = @next_valid_lsn
	if @@error<>0 goto nosyncSubSetupHelperFailure

	-- Update table MSnosyncsubsetup with the parameters
	-- to be needed by stored proc sp_MSsetupnosyncsubwithlsnatdist
	declare @parameterValueStr nvarchar(max)
	select @parameterValueStr = @publisher
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'publisher',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = @publisher_db
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'publisher_db',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = @publication
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'publication',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = @article
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'article',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@subscriber, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'subscriber',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@destination_db, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'destination_db',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@subscriptionlsn AS nvarchar(max)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'subscriptionlsn',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@lsnsource AS nvarchar(20)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'lsnsource',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@pubid AS nvarchar(20)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'originator_publication_id',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@publisher_db_version AS nvarchar(20)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'originator_db_version',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@script_txt, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'originator_meta_data',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@nosync_setup_script, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'nosync_setup_script',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@next_valid_lsn AS nvarchar(max)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'next_valid_lsn',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	commit tran nosyncSubSetupHelper
	return 0

nosyncSubSetupHelperFailure:
	rollback tran nosyncSubSetupHelper
	commit tran
	return 1

Failure:
	return 1
end


/*====  SQL Server 2022 version  ====*/
--
-- Name: sp_MSsetupnosyncsubwithlsnatdist_helper
-- 
-- Description: activate nosync subscription at distributor
-- and return the article id corresponding to the article;
-- when article = N'all', return the first retrieved article id in MSarticles;
-- create a temporary table (named 'MSnosyncsubsetup') in distribution db
-- if the table does not exist, and update the table with the parameters 
-- to be needed for setting up of the specified non-sync subscription.
--
-- Security: Procedural security check is performed inside this procedure to
--           ensure that the caller is a member of sysadmin. Execute 
--           permission of this procedure is granted to public. This procedure 
--           is invoked via RPC
--
create procedure sys.sp_MSsetupnosyncsubwithlsnatdist_helper
(
	-- Publication properties
	@publisher                              sysname,
	@publisher_db                         sysname,
	@publication                            sysname,
	@article                                   sysname = N'all',
	-- Subscription properties
	@subscriber                             sysname,
	@destination_db                       sysname,
	@subscriptionlsn                       binary(10),
	@lsnsource                               int,
	@pubid                                     int,
	@publisher_db_version              int,
	@script_txt                               nvarchar(max),
	@nosync_setup_script               nvarchar(max),
	@next_valid_lsn                        binary(10),
	-- Output article id
	@artid                                      int = NULL OUTPUT
)
as
begin 
	set nocount on
	declare @publisherid int,
			@subscriberid int,
			@publisher_database_id int,
			@publication_id int,
			@nosyncCommandStr nvarchar(max)

	-- Security check 
	if (isnull(is_srvrolemember('sysadmin'),0) = 0)
	begin
		raiserror(21089, 16, -1)
		return 1
	end
	
	exec sys.sp_MSget_server_portinfo 
			@name = @publisher, 
			@srvname = @publisher OUTPUT

	DECLARE @trace_number	int,
			@trace_status	bit
	SET @trace_number = 15005 -- Trace flag to enable Subscriber with non-default port in AG scenario
	EXEC sys.sp_check_trace_enabled_globally @trace_number, @trace_status OUTPUT

	IF @trace_status = 0 -- if Subscriber with non-default port is disabled
	BEGIN
		exec sys.sp_MSget_server_portinfo 
			@name = @subscriber, 
			@srvname = @subscriber OUTPUT
	END

    -- Obtain Publisher's server id
    select @publisherid = srvid 
      from MSreplservers
     where upper(srvname) = upper(@publisher) collate database_default
    if @@error<>0 goto Failure

    -- Obtain Subscriber's server id
    select @subscriberid = srvid
      from MSreplservers
     where upper(srvname) = upper(@subscriber) collate database_default
    if @@error<>0 goto Failure

	-- Find out what the publisher database id is
	select @publisher_database_id = id
	  from dbo.MSpublisher_databases
	 where publisher_db = @publisher_db
	   and publisher_id = @publisherid
	if @@error<>0 goto Failure

	-- Obtain the publication id
	select @publication_id = publication_id
	  from dbo.MSpublications
	 where publisher_id = @publisherid
	   and publisher_db = @publisher_db
	   and publication = @publication
	if @@error<>0 goto Failure

	-- Obtain artid if this is for an incremental article
	-- or one of the artids if @article equals to N'all'.
	select @artid = article_id    
	from MSarticles
	where publisher_id = @publisherid
	   and publisher_db = @publisher_db 
	   and publication_id = @publication_id
	   and (@article = N'all' or article = @article)
	if @@error<>0 or @artid is NULL goto Failure

	begin tran nosyncSubSetupHelper
	save tran nosyncSubSetupHelper

	-- Activate subscriptions in dbo.MSsubscriptions
	update dbo.MSsubscriptions
	   set status = 2 --active status
	 where publisher_database_id = @publisher_database_id
	   and publisher_id = @publisherid
	   and publisher_db = @publisher_db -- Extra insurance
	   and publication_id = @publication_id
	   and subscriber_id = @subscriberid
	   and subscriber_db = @destination_db
	   and (@article = N'all' or article_id = @artid)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	-- The table MSnosyncsubsetup should already exist
	-- in distribution database.
	if object_id(N'dbo.MSnosyncsubsetup', 'U') is NULL 
		goto nosyncSubSetupHelperFailure

	-- Try to clean up the table before adding parameters (rows)
	-- into the table for the specified nonsync subscription.
	-- Note that, in MSnosyncsubsetup table, we need the publication 
	-- id (i.e., @publid) from publisher db instead of the publication
	-- id (i.e., MSpublications.publication_id) from distribution db, 
	-- which might be different.
	delete dbo.MSnosyncsubsetup 
	where publisher_database_id = @publisher_database_id
	  and publication_id = @pubid
	  and artid = @artid
	  and next_valid_lsn = @next_valid_lsn
	if @@error<>0 goto nosyncSubSetupHelperFailure

	-- Update table MSnosyncsubsetup with the parameters
	-- to be needed by stored proc sp_MSsetupnosyncsubwithlsnatdist
	declare @parameterValueStr nvarchar(max)
	select @parameterValueStr = @publisher
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'publisher',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = @publisher_db
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'publisher_db',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = @publication
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'publication',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = @article
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'article',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@subscriber, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'subscriber',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@destination_db, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'destination_db',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@subscriptionlsn AS nvarchar(max)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'subscriptionlsn',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@lsnsource AS nvarchar(20)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'lsnsource',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@pubid AS nvarchar(20)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'originator_publication_id',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@publisher_db_version AS nvarchar(20)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'originator_db_version',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@script_txt, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'originator_meta_data',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(@nosync_setup_script, N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'nosync_setup_script',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	select @parameterValueStr = ISNULL(CAST(@next_valid_lsn AS nvarchar(max)), N'')
	insert into dbo.MSnosyncsubsetup ( 
		publisher_database_id,
		publication_id,
		artid,
		next_valid_lsn,
		parameterName,
		parameterValue)
	values (
		@publisher_database_id,
		@pubid,
		@artid,
		@next_valid_lsn,
		N'next_valid_lsn',
		@parameterValueStr)
	if @@error<>0 goto nosyncSubSetupHelperFailure

	commit tran nosyncSubSetupHelper
	return 0

nosyncSubSetupHelperFailure:
	rollback tran nosyncSubSetupHelper
	commit tran
	return 1

Failure:
	return 1
end

