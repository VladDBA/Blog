use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_dbcmptlevel			-- 1997/04/15
	@dbname sysname = NULL,					-- database name to change
	@new_cmptlevel tinyint = NULL OUTPUT	-- the new compatibility level to change to
as
	set nocount    on

	declare @exec_stmt nvarchar(max)
	declare @returncode	int
	declare @comptlevel	float(8)
	declare @dbid int					-- dbid of the database
	declare @dbsid varbinary(85)		-- id of the owner of the database
	declare @orig_cmptlevel tinyint		-- original compatibility level
	declare @input_cmptlevel tinyint	-- compatibility level passed in by user
		,@cmptlvl100 tinyint				-- compatibility to SQL Server Version 10.0
		,@cmptlvl110 tinyint				-- compatibility to SQL Server Version 11.0
		,@cmptlvl120 tinyint				-- compatibility to SQL Server Version 12.0
		,@cmptlvl130 tinyint				-- compatibility to SQL Server Version 13.0
		,@cmptlvl140 tinyint				-- compatibility to SQL Server Version 14.0
		,@cmptlvl150 tinyint				-- compatibility to SQL Server Version 15.0
		,@cmptlvl160 tinyint				-- compatibility to SQL Server Version 16.0
		,@cmptlvl170 tinyint				-- compatibility to SQL Server Version 17.0
        
	select  @cmptlvl100 = 100,
			@cmptlvl110 = 110,
			@cmptlvl120 = 120,
			@cmptlvl130 = 130,
			@cmptlvl140 = 140,
			@cmptlvl150 = 150,
			@cmptlvl160 = 160,
			@cmptlvl170 = 170

	-- SP MUST BE CALLED AT ADHOC LEVEL --
	if (@@nestlevel > 1)
	begin
		raiserror(15432,-1,-1,'sys.sp_dbcmptlevel')
		return (1)
	end

	-- If no @dbname given, just list the valid compatibility level values.
	if @dbname is null
	begin
		raiserror (15048, -1, -1, @cmptlvl100, @cmptlvl110, @cmptlvl120, @cmptlvl130, @cmptlvl140, @cmptlvl150, @cmptlvl160, @cmptlvl170)
		return (0)
	end
	
	--  Verify the database name and get info
	select @dbid = dbid, @dbsid = sid ,@orig_cmptlevel = cmptlevel
		from master.dbo.sysdatabases
		where name = @dbname

	--  If @dbname not found, say so and list the databases.
	if @dbid is null
	begin
		raiserror(15010,-1,-1,@dbname)
		print ' '
		select name as 'Available databases:'
			from master.dbo.sysdatabases
		return (1)
	end

	-- Now save the input compatibility level and initialize the return clevel
	-- to be the current clevel
	select @input_cmptlevel = @new_cmptlevel
	select @new_cmptlevel = @orig_cmptlevel

	-- If no clevel was supplied, display and output current level.
	if @input_cmptlevel is null
	begin
		raiserror(15054, -1, -1, @orig_cmptlevel)
		return(0)
	end

	-- If invalid clevel given, print usage and return error code
	-- 'usage: sp_dbcmptlevel [dbname [, compatibilitylevel]]'
	if @input_cmptlevel not in (@cmptlvl100, @cmptlvl110, @cmptlvl120, @cmptlvl130, @cmptlvl140, @cmptlvl150, @cmptlvl160, @cmptlvl170)
	begin
		raiserror(15416, -1, -1)
		print ' '
		raiserror (15048, -1, -1, @cmptlvl100, @cmptlvl110, @cmptlvl120, @cmptlvl130, @cmptlvl140, @cmptlvl150, @cmptlvl160, @cmptlvl170)
		return (1)
	end

	--  Only the SA or the dbo of @dbname can execute the update part
	--  of this procedure sys.so check.
	if (not (is_srvrolemember('sysadmin') = 1)) and suser_sid() <> @dbsid
		-- ALSO ALLOW db_owner ONLY IF DB REQUESTED IS CURRENT DB
		and (@dbid <> db_id() or is_member('db_owner') <> 1)
	begin
		raiserror(15418,-1,-1)
		return (1)
	end

	-- If we're in a transaction, disallow this since it might make recovery impossible.
	set implicit_transactions off
	if @@trancount > 0
	begin
		raiserror(15002,-1,-1,'sys.sp_dbcmptlevel')
		return (1)
	end

	set @exec_stmt = 'ALTER DATABASE ' + quotename(@dbname, '[') + ' SET COMPATIBILITY_LEVEL = ' + cast(@input_cmptlevel as nvarchar(128))

	-- Note: database @dbname may not exist anymore
	exec(@exec_stmt)

	select @new_cmptlevel = @input_cmptlevel

	return (0) -- sp_dbcmptlevel


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_dbcmptlevel			-- 1997/04/15
	@dbname sysname = NULL,					-- database name to change
	@new_cmptlevel tinyint = NULL OUTPUT	-- the new compatibility level to change to
as
	set nocount    on

	declare @exec_stmt nvarchar(max)
	declare @returncode	int
	declare @comptlevel	float(8)
	declare @dbid int					-- dbid of the database
	declare @dbsid varbinary(85)		-- id of the owner of the database
	declare @orig_cmptlevel tinyint		-- original compatibility level
	declare @input_cmptlevel tinyint	-- compatibility level passed in by user
		,@cmptlvl100 tinyint				-- compatibility to SQL Server Version 10.0
		,@cmptlvl110 tinyint				-- compatibility to SQL Server Version 11.0
		,@cmptlvl120 tinyint				-- compatibility to SQL Server Version 12.0
		,@cmptlvl130 tinyint				-- compatibility to SQL Server Version 13.0
		,@cmptlvl140 tinyint				-- compatibility to SQL Server Version 14.0
		,@cmptlvl150 tinyint				-- compatibility to SQL Server Version 15.0
		,@cmptlvl160 tinyint				-- compatibility to SQL Server Version 16.0
        
	select  @cmptlvl100 = 100,
			@cmptlvl110 = 110,
			@cmptlvl120 = 120,
			@cmptlvl130 = 130,
			@cmptlvl140 = 140,
			@cmptlvl150 = 150,
			@cmptlvl160 = 160

	-- SP MUST BE CALLED AT ADHOC LEVEL --
	if (@@nestlevel > 1)
	begin
		raiserror(15432,-1,-1,'sys.sp_dbcmptlevel')
		return (1)
	end

	-- If no @dbname given, just list the valid compatibility level values.
	if @dbname is null
	begin
		raiserror (15048, -1, -1, @cmptlvl100, @cmptlvl110, @cmptlvl120, @cmptlvl130, @cmptlvl140, @cmptlvl150, @cmptlvl160)
		return (0)
	end
	
	--  Verify the database name and get info
	select @dbid = dbid, @dbsid = sid ,@orig_cmptlevel = cmptlevel
		from master.dbo.sysdatabases
		where name = @dbname

	--  If @dbname not found, say so and list the databases.
	if @dbid is null
	begin
		raiserror(15010,-1,-1,@dbname)
		print ' '
		select name as 'Available databases:'
			from master.dbo.sysdatabases
		return (1)
	end

	-- Now save the input compatibility level and initialize the return clevel
	-- to be the current clevel
	select @input_cmptlevel = @new_cmptlevel
	select @new_cmptlevel = @orig_cmptlevel

	-- If no clevel was supplied, display and output current level.
	if @input_cmptlevel is null
	begin
		raiserror(15054, -1, -1, @orig_cmptlevel)
		return(0)
	end

	-- If invalid clevel given, print usage and return error code
	-- 'usage: sp_dbcmptlevel [dbname [, compatibilitylevel]]'
	if @input_cmptlevel not in (@cmptlvl100, @cmptlvl110, @cmptlvl120, @cmptlvl130, @cmptlvl140, @cmptlvl150, @cmptlvl160)
	begin
		raiserror(15416, -1, -1)
		print ' '
		raiserror (15048, -1, -1, @cmptlvl100, @cmptlvl110, @cmptlvl120, @cmptlvl130, @cmptlvl140, @cmptlvl150, @cmptlvl160)
		return (1)
	end

	--  Only the SA or the dbo of @dbname can execute the update part
	--  of this procedure sys.so check.
	if (not (is_srvrolemember('sysadmin') = 1)) and suser_sid() <> @dbsid
		-- ALSO ALLOW db_owner ONLY IF DB REQUESTED IS CURRENT DB
		and (@dbid <> db_id() or is_member('db_owner') <> 1)
	begin
		raiserror(15418,-1,-1)
		return (1)
	end

	-- If we're in a transaction, disallow this since it might make recovery impossible.
	set implicit_transactions off
	if @@trancount > 0
	begin
		raiserror(15002,-1,-1,'sys.sp_dbcmptlevel')
		return (1)
	end

	set @exec_stmt = 'ALTER DATABASE ' + quotename(@dbname, '[') + ' SET COMPATIBILITY_LEVEL = ' + cast(@input_cmptlevel as nvarchar(128))

	-- Note: database @dbname may not exist anymore
	exec(@exec_stmt)

	select @new_cmptlevel = @input_cmptlevel

	return (0) -- sp_dbcmptlevel

