SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE PROC sys.sp_flush_commit_table_on_demand (@numrows as BIGINT = NULL, @deleted_rows BIGINT = NULL OUTPUT, @date_cleanedup DATETIME = NULL OUTPUT, @cleanup_ts BIGINT = NULL OUTPUT) AS
BEGIN

	IF (is_member('db_owner') = 0)
	BEGIN
		raiserror(21050,0,1)
		return 1
	END


	IF (@numrows<=0)
	BEGIN
		raiserror(23100,0,1)
		return 1
	END

	-- Get the original value of IMPLICIT_TRANSACTIONS before setting it to OFF
	declare @implicit_transaction    int
	SELECT @implicit_transaction = 0
	SELECT @implicit_transaction = @@options & 2
	SET IMPLICIT_TRANSACTIONS OFF

	SET DEADLOCK_PRIORITY LOW

	IF  EXISTS (SELECT * FROM sys.change_tracking_databases where database_id = DB_ID())
	BEGIN
		DECLARE @trcflg_skip_safe_cleanup_version INT = 0, @trace_number INT = 8239
		EXEC sys.sp_check_trace_enabled_globally @trace_number, @trcflg_skip_safe_cleanup_version OUTPUT

		DECLARE @batch_size INT
		IF (@numrows IS NULL or @numrows >= 4999)
			SET @batch_size = 4999
		ELSE
			SET @batch_size = @numrows

		--check if column cleanup_version datatype in MSchange_tracking_history is INT then Alter to BIGINT
		EXEC sp_changetracking_alter_history_table

		-- checking for change tracking side table watermark
		DECLARE @deleted_rowcount INT
		SET		@cleanup_ts = change_tracking_hardened_cleanup_version ()
		RAISERROR(22866,0,1,35502,@cleanup_ts)
		
		--snapshot isolation status
		DECLARE @snapshot_isolation_status INT
		SELECT @snapshot_isolation_status = S.snapshot_isolation_state 
		FROM sys.databases as S 
		WHERE database_id = DB_ID()
		
		
		--need to update the @cleanup_ts if snapshot isolation is on and trcflg_skip_safe_cleanup_version is off
		if (@snapshot_isolation_status <> 0 AND @trcflg_skip_safe_cleanup_version = 0)
		BEGIN
			-- checking for safe cleanup watermark
			DECLARE @cleanup_version BIGINT = 0
			set @cleanup_version = safe_cleanup_version ()
			RAISERROR(22866,0,1,35503,@cleanup_version)

			IF (@cleanup_ts > @cleanup_version) 
			BEGIN
				SET @cleanup_ts = @cleanup_version
			END
		END

		DECLARE @start_time DATETIME,
				@objid INT
		SELECT @start_time = GETDATE()
		SELECT @objid = object_id('sys.syscommittab')

		SELECT @deleted_rowcount = 0, @deleted_rows = 0

		WHILE (1=1)
		BEGIN
			DELETE TOP(@batch_size) sys.syscommittab WHERE commit_ts < @cleanup_ts
			SELECT @deleted_rowcount = @@ROWCOUNT
			SELECT @deleted_rows += @deleted_rowcount
			SELECT @numrows -= @batch_size
			IF ((@numrows IS NOT NULL AND @numrows <= 0 ) OR @deleted_rowcount < @batch_size)
				BREAK;
		END

		EXEC sp_add_ct_history @start_time = @start_time, @internal_table_id = @objid, @rows_cleaned_up = @deleted_rows,
					@cleanup_version = @cleanup_ts, @comments = 'syscommittab cleanup succeeded'

		select @date_cleanedup = min(commit_time) from sys.syscommittab where commit_ts = (select min(commit_ts) from sys.syscommittab)
		IF (@date_cleanedup is null)
			SET @date_cleanedup = GETUTCDATE()

	END
	ELSE IF EXISTS (SELECT * FROM sys.databases WHERE DB_NAME() not in ('master', 'tempdb', 'model', 'msdb', 'mssqlsystemresource')) and EXISTS (select 1 from sys.syscommittab having count(*) > 0)
	BEGIN
		TRUNCATE TABLE sys.syscommittab
		IF OBJECT_ID('MSchange_tracking_history') IS NOT NULL
			DROP TABLE MSchange_tracking_history
	END

	/*
	** Set back original settings
	*/
	IF @implicit_transaction <> 0
		SET IMPLICIT_TRANSACTIONS ON

END


/*====  SQL Server 2022 version  ====*/
CREATE PROC sys.sp_flush_commit_table_on_demand (@numrows as BIGINT = NULL, @deleted_rows BIGINT = NULL OUTPUT, @date_cleanedup DATETIME = NULL OUTPUT, @cleanup_ts BIGINT = NULL OUTPUT) AS
BEGIN

	IF (is_member('db_owner') = 0)
	BEGIN
		raiserror(21050,0,1)
		return 1
	END


	IF (@numrows<=0)
	BEGIN
		raiserror(23100,0,1)
		return 1
	END

	-- Get the original value of IMPLICIT_TRANSACTIONS before setting it to OFF
	declare @implicit_transaction    int
	SELECT @implicit_transaction = 0
	SELECT @implicit_transaction = @@options & 2
	SET IMPLICIT_TRANSACTIONS OFF

	SET DEADLOCK_PRIORITY LOW

	IF  EXISTS (SELECT * FROM sys.change_tracking_databases where database_id = DB_ID())
	BEGIN
		DECLARE @trcflg_skip_safe_cleanup_version INT = 0, @trace_number INT = 8239
		EXEC sys.sp_check_trace_enabled_globally @trace_number, @trcflg_skip_safe_cleanup_version OUTPUT

		DECLARE @batch_size INT
		IF (@numrows IS NULL or @numrows >= 4999)
			SET @batch_size = 4999
		ELSE
			SET @batch_size = @numrows

		--check if column cleanup_version datatype in MSchange_tracking_history is INT then Alter to BIGINT
		EXEC sp_changetracking_alter_history_table

		-- checking for change tracking side table watermark
		DECLARE @deleted_rowcount INT
		SET		@cleanup_ts = change_tracking_hardened_cleanup_version ()
		RAISERROR(22866,0,1,35502,@cleanup_ts)

		-- checking for safe cleanup watermark
		DECLARE @cleanup_version BIGINT
		set @cleanup_version = safe_cleanup_version ()
		RAISERROR(22866,0,1,35503,@cleanup_version)

		IF (@trcflg_skip_safe_cleanup_version = 0 AND @cleanup_ts > @cleanup_version)
			SET @cleanup_ts = @cleanup_version

		DECLARE @start_time DATETIME,
				@objid INT
		SELECT @start_time = GETDATE()
		SELECT @objid = object_id('sys.syscommittab')

		SELECT @deleted_rowcount = 0, @deleted_rows = 0

		WHILE (1=1)
		BEGIN
			DELETE TOP(@batch_size) sys.syscommittab WHERE commit_ts < @cleanup_ts
			SELECT @deleted_rowcount = @@ROWCOUNT
			SELECT @deleted_rows += @deleted_rowcount
			SELECT @numrows -= @batch_size
			IF ((@numrows IS NOT NULL AND @numrows <= 0 ) OR @deleted_rowcount < @batch_size)
				BREAK;
		END

		EXEC sp_add_ct_history @start_time = @start_time, @internal_table_id = @objid, @rows_cleaned_up = @deleted_rows,
					@cleanup_version = @cleanup_ts, @comments = 'syscommittab cleanup succeeded'

		select @date_cleanedup = min(commit_time) from sys.syscommittab where commit_ts = (select min(commit_ts) from sys.syscommittab)
		IF (@date_cleanedup is null)
			SET @date_cleanedup = GETUTCDATE()

	END
	ELSE IF EXISTS (SELECT * FROM sys.databases WHERE DB_NAME() not in ('master', 'tempdb', 'model', 'msdb', 'mssqlsystemresource')) and EXISTS (select 1 from sys.syscommittab having count(*) > 0)
	BEGIN
		TRUNCATE TABLE sys.syscommittab
		IF OBJECT_ID('MSchange_tracking_history') IS NOT NULL
			DROP TABLE MSchange_tracking_history
	END

	/*
	** Set back original settings
	*/
	IF @implicit_transaction <> 0
		SET IMPLICIT_TRANSACTIONS ON

END

