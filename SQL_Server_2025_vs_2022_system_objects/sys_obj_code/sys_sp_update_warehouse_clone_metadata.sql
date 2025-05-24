SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_update_warehouse_clone_metadata
	@target_database_guid UNIQUEIDENTIFIER,
	@source_database_guid UNIQUEIDENTIFIER = NULL,
	@source_workspace_guid UNIQUEIDENTIFIER = NULL
AS
BEGIN
	EXEC sys.sp_ensure_trident_frontend;
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL SNAPSHOT;

	-- Internal (non-system) tables should not be cloned
	DECLARE @previous_internal_table_name NVARCHAR(776);
	DECLARE @current_internal_table_name NVARCHAR(776);
	EXEC sys.sp_get_top_internal_table @current_internal_table_name OUTPUT;

	WHILE (@current_internal_table_name <> ISNULL(@previous_internal_table_name, ''))
	BEGIN
		DECLARE @drop_sql NVARCHAR(788) = 'DROP TABLE ' + @current_internal_table_name + ';';
		EXEC sp_executesql @drop_sql;

		SET @previous_internal_table_name = @current_internal_table_name;
		EXEC sys.sp_get_top_internal_table @current_internal_table_name OUTPUT;
	END

	-- The logical filenames should update from the source artifact ID to the target artifact ID
	DECLARE @source_database_guid_str NVARCHAR(100) = UPPER(CONVERT(NVARCHAR(100), @source_database_guid));
	DECLARE @target_database_guid_str NVARCHAR(100) = UPPER(CONVERT(NVARCHAR(100), @target_database_guid));

	DECLARE @file_mappings TABLE (Id INT IDENTITY(1,1), logical_name_old NVARCHAR(100), logical_name_new NVARCHAR(100));
	INSERT INTO @file_mappings VALUES
		(@source_database_guid_str, @target_database_guid_str),
		(@source_database_guid_str + '_log', @target_database_guid_str + '_log');
	
	DECLARE @logical_name_old NVARCHAR(100);
	DECLARE @logical_name_new NVARCHAR(100);
	DECLARE @count_files_to_rename INT = (SELECT MAX(Id) FROM @file_mappings);
	DECLARE @id INT = 1;

	WHILE @id <= @count_files_to_rename
	BEGIN
		SELECT
			@logical_name_old = logical_name_old,
			@logical_name_new = logical_name_new
		FROM @file_mappings
		WHERE Id = @id;

		DECLARE @rename_file_sql NVARCHAR(4000) =
			'ALTER DATABASE ' + QUOTENAME(DB_NAME()) +
			' MODIFY FILE ' + 
			'(NAME = ''' + @logical_name_old + ''', NEWNAME = ''' + @logical_name_new + ''');';

		EXEC sp_executesql @rename_file_sql;

		SET @id += 1;
	END

	-- Remove dropped and non-user tables from each internal system table
	DECLARE @default_drop_commit_time DATETIME = '1900-01-01T00:00:00';
	DECLARE @user_table_classification INT = 1;

	DECLARE @deleted_tables TABLE (table_id BIGINT NOT NULL);
	DELETE FROM sys.manageddeltatables
	OUTPUT DELETED.table_id INTO @deleted_tables
	WHERE drop_commit_time > @default_drop_commit_time;

	DELETE FROM sys.manageddeltatablelogfiles
	WHERE table_id IN (SELECT table_id FROM @deleted_tables);

	DELETE FROM sys.manageddeltatableforks
	WHERE table_id IN (SELECT table_id FROM @deleted_tables);

	DELETE FROM sys.manageddeltatablecheckpoints
	WHERE table_id IN (SELECT table_id FROM @deleted_tables);

	IF OBJECT_ID('sys.statistics_table_metadata') IS NOT NULL
	BEGIN
		DELETE FROM sys.statistics_table_metadata
		WHERE table_id IN (SELECT table_id FROM @deleted_tables);
	END

	DELETE FROM sys.statisticstablemetadata
	WHERE table_id IN (SELECT table_id FROM @deleted_tables);

	DELETE FROM sys.statistics_histogram_metadata
	WHERE table_id IN (SELECT table_id FROM @deleted_tables);

	IF OBJECT_ID('sys.discoveredtableproperties') IS NOT NULL
	BEGIN
		DELETE FROM sys.discoveredtableproperties
		WHERE table_id IN (SELECT table_id FROM @deleted_tables);
	END

	-- Update each table in sys.manageddeltatables
	UPDATE mdt
	SET
		mdt.table_guid = source.new_table_guid,
		mdt.fork_guid = source.new_table_guid,
		mdt.clone_island_guid = source.original_clone_island_guid,
		mdt.clone_parent_guid = source.original_table_guid
	FROM
		(SELECT
			table_id,
			NEWID() AS new_table_guid,
			table_guid AS original_table_guid,
			clone_island_guid AS original_clone_island_guid
		FROM sys.manageddeltatables) source 
		JOIN sys.manageddeltatables mdt ON source.table_id = mdt.table_id;

	BEGIN TRAN;

	DECLARE @current_txn_xdes_ts BIGINT =
		(SELECT transaction_id FROM sys.dm_tran_current_transaction);

	DECLARE @current_txn_commit_time DATETIME = GETDATE();

	-- Create a fork for each table
	INSERT INTO sys.manageddeltatableforks
		SELECT
			table_id,
			fork_guid,
			table_guid,
			@target_database_guid,
			@current_txn_xdes_ts,
			@current_txn_commit_time
		FROM sys.manageddeltatables;

	COMMIT TRAN;

	IF @source_database_guid IS NOT NULL AND @source_workspace_guid IS NOT NULL
	BEGIN
		INSERT INTO sys.sourcedatabases VALUES (@source_database_guid, @source_workspace_guid);
	END
END

