use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_adddistpublisher
(
	@publisher				sysname,
	@distribution_db		sysname,
	@security_mode			int = 1,
	@login					sysname = NULL,
	@password				sysname = NULL,
	@working_directory		nvarchar(255) = NULL,
	@trusted				nvarchar(5) = 'false',
	@encrypted_password		bit = 0,
	@thirdparty_flag		bit = 0,
	@publisher_type			sysname = N'MSSQLSERVER',
	@storage_connection_string nvarchar(255) = NULL
)
AS
BEGIN
	DECLARE @cmd								nvarchar(4000)
	DECLARE @retcode							int
	DECLARE @distdb_in_secondary 				bit
	DECLARE @trace_number						int
	DECLARE @trace_status						bit
	DECLARE @use_case_sensitive_publisher_name	bit = 0

	SET @retcode = 0
	SET @cmd = N''

	-- DEPRECATED PARAMETER: @trusted
	-- For security reasons, @trusted is no longer supported.
	-- Implicitly, it must always be @trusted == false.  If
	-- anything other than false is supplied, an error is thrown.
	IF LOWER(@trusted collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('false')
	BEGIN
		RAISERROR(21698, 16, -1, '@trusted')
		RETURN (1)
	END
	
	-- Check if HREPL
	IF NOT @publisher_type = N'MSSQLSERVER'
	BEGIN
		SET @cmd = @cmd + QUOTENAME(@distribution_db) + N'.'
	END

	-- Check if dist db is secondary in AG and if it is in readable state
	select @distdb_in_secondary = sys.fn_MSrepl_isdistdbsecondary(@distribution_db)
	
	if (@distdb_in_secondary = 1)
	BEGIN 
		if not exists (select * from sys.availability_replicas where replica_id = 
					(
						select hdr.replica_id from sys.dm_hadr_database_replica_states as hdr where hdr.database_id = db_id(@distribution_db collate database_default)
					)
					and secondary_role_allow_connections = 2
                   )
				    BEGIN
						RAISERROR (25036, 16, -1)
						RETURN (1)
					END
	END 	

	-- We always use uppercase for Publisher, unless Publisher and Distributor are different Box servers.
	-- When Publisher and Distributor are different Box servers using uppercase for Publisher can cause
	-- replication setup to fail, so we should use case sensitive Publisher name instead.
	IF SERVERPROPERTY('EngineEdition') IN (2, 3, 4) AND (UPPER(@@SERVERNAME) != UPPER(@publisher))
	BEGIN
		SET @trace_number = 15004 -- Trace flag to disable using case sensitive publisher name
		EXEC sys.sp_check_trace_enabled_globally @trace_number, @trace_status OUTPUT

		IF @trace_status = 0
		BEGIN
			SET @use_case_sensitive_publisher_name = 1
		END
	END

	IF @use_case_sensitive_publisher_name = 0
	BEGIN
		SET @publisher = UPPER(@publisher) COLLATE DATABASE_DEFAULT
	END

	-- Add sp
	set @cmd = @cmd + N'sys.sp_MSrepl_adddistpublisher'
	
	EXEC @retcode = @cmd
					@publisher,
					@distribution_db,
					@security_mode,
					@login,
					@password,
					@working_directory,
					@trusted,
					@encrypted_password,
					@thirdparty_flag,
					@publisher_type,
					@storage_connection_string,
					@use_case_sensitive_publisher_name

	RETURN (@retcode)
END


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_adddistpublisher
(
	@publisher				sysname,
	@distribution_db		sysname,
	@security_mode			int = 1,
	@login					sysname = NULL,
	@password				sysname = NULL,
	@working_directory		nvarchar(255) = NULL,
	@trusted				nvarchar(5) = 'false',
	@encrypted_password		bit = 0,
	@thirdparty_flag		bit = 0,
	@publisher_type			sysname = N'MSSQLSERVER',
	@storage_connection_string nvarchar(255) = NULL
)
AS
BEGIN
	DECLARE @cmd								nvarchar(4000)
	DECLARE @retcode							int
	DECLARE @distdb_in_secondary				bit
	DECLARE @trace_number						int
	DECLARE @trace_status						bit
	DECLARE @use_case_sensitive_publisher_name	bit = 0

	SET @retcode = 0
	SET @cmd = N''

	-- DEPRECATED PARAMETER: @trusted
	-- For security reasons, @trusted is no longer supported.
	-- Implicitly, it must always be @trusted == false.  If
	-- anything other than false is supplied, an error is thrown.
	IF LOWER(@trusted collate SQL_Latin1_General_CP1_CS_AS) NOT IN ('false')
	BEGIN
		RAISERROR(21698, 16, -1, '@trusted')
		RETURN (1)
	END
	
	-- Check if HREPL
	IF NOT @publisher_type = N'MSSQLSERVER'
	BEGIN
		SET @cmd = @cmd + QUOTENAME(@distribution_db) + N'.'
	END

	-- Check if dist db is secondary in AG and if it is in readable state
	select @distdb_in_secondary = sys.fn_MSrepl_isdistdbsecondary(@distribution_db)
	
	if (@distdb_in_secondary = 1)
	BEGIN 
		if not exists (select * from sys.availability_replicas where replica_id = 
					(
						select hdr.replica_id from sys.dm_hadr_database_replica_states as hdr where hdr.database_id = db_id(@distribution_db collate database_default)
					)
					and secondary_role_allow_connections = 2
                   )
				    BEGIN
						RAISERROR (25036, 16, -1)
						RETURN (1)
					END
	END 	

	-- We always use uppercase for Publisher, unless Publisher and Distributor are different Box servers.
	-- When Publisher and Distributor are different Box servers using uppercase for Publisher can cause
	-- replication setup to fail, so we should use case sensitive Publisher name instead.
	IF SERVERPROPERTY('EngineEdition') IN (2, 3, 4) AND (UPPER(@@SERVERNAME) != UPPER(@publisher))
	BEGIN
		SET @trace_number = 15004 -- Trace flag to disable using case sensitive publisher name
		EXEC sys.sp_check_trace_enabled_globally @trace_number, @trace_status OUTPUT

		IF @trace_status = 0
		BEGIN
			SET @use_case_sensitive_publisher_name = 1
		END
	END

	IF @use_case_sensitive_publisher_name = 0
	BEGIN
		SET @publisher = UPPER(@publisher) COLLATE DATABASE_DEFAULT
	END

	-- Add sp
	set @cmd = @cmd + N'sys.sp_MSrepl_adddistpublisher'
	
	EXEC @retcode = @cmd
					@publisher,
					@distribution_db,
					@security_mode,
					@login,
					@password,
					@working_directory,
					@trusted,
					@encrypted_password,
					@thirdparty_flag,
					@publisher_type,
					@storage_connection_string,
					@use_case_sensitive_publisher_name

	RETURN (@retcode)
END

