SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE PROCEDURE [sys].[sp_help_fabric_mirroring_table]
(
	@table_group_id uniqueidentifier = null,
	@table_id uniqueidentifier = null,
	@source_schema sysname = 'dbo',
	@source_name sysname = null
)
AS
BEGIN
	DECLARE @stmt nvarchar(max),
			@id_params bit = 0,
			@source_object_id int,
			@source_table nvarchar(1000),
			@metadata_schema_name nvarchar(10),
			@db_name sysname

	SET @db_name = db_name()

	-- Return if Fabric Link is not enabled.
	IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = @db_name AND is_change_feed_enabled = 1 AND is_data_lake_replication_enabled = 1)
	BEGIN
		RAISERROR(22706, 16, 6, @db_name)
		RETURN 1
	END

	-- Get the metadata table schema name (sys or changefeed) based on the TridentOneLakeLinkUseInternalTablesForMetadata feature switch value.
	EXEC sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT

	IF (@table_group_id IS NOT NULL AND @table_id IS NOT NULL)
	BEGIN
		SET @id_params = 1
		DECLARE @result_count int = 0;

		SET @stmt = 
			N'
			SELECT @result_count = count(*)
			FROM ' + @metadata_schema_name + '.change_feed_tables INNER JOIN ' + @metadata_schema_name + '.change_feed_table_groups 
			ON ' + @metadata_schema_name + '.change_feed_table_groups.table_group_id = ' + @metadata_schema_name + '.change_feed_tables.table_group_id
			WHERE ' + @metadata_schema_name +  '.change_feed_tables.table_id = ''' + cast(@table_id as nvarchar(max)) + '''
				and ' + @metadata_schema_name + '.change_feed_tables.table_group_id = ''' + cast(@table_group_id as nvarchar(max)) + ''''
			
		EXEC sp_executesql @stmt, N'@result_count int output', @result_count = @result_count output;

		IF @result_count = 0
		BEGIN
			RAISERROR(22769, 16, -1)
			RETURN 1
		END
	END
	ELSE IF (@source_name IS NOT NULL)
	BEGIN
		SELECT @source_schema = ISNULL(@source_schema, 'dbo')
		SELECT @source_table = QUOTENAME(@source_schema) + N'.' + QUOTENAME(@source_name)
		SELECT @source_object_id = OBJECT_ID(@source_table)

		IF (@source_object_id IS NULL)
		OR NOT EXISTS (SELECT *
					   FROM sys.tables
					   WHERE object_id = @source_object_id AND is_ms_shipped = 0)
		BEGIN
			RAISERROR(22769, 16, -1)
			RETURN 1
		END
	END
	ELSE
	BEGIN
		RAISERROR(22770, 16, -1)
		RETURN 1
	END

	DECLARE @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled int
	EXEC @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkUseHelperProcsToExposeReseedId'
	DECLARE @exposed_reseed_id sysname = IIF(sys.fn_trident_link_is_enabled_for_current_db() <> 1 OR @is_trident_publishing_use_helper_procs_to_expose_reseed_id_enabled <> 1, N'', N', sl_table.reseed_id AS reseed_id')

	set @stmt =
		N'
		SELECT
			table_group.table_group_id AS table_group_id,
			table_group.table_group_name AS table_group_name,
			object_schema_name(sl_table.object_id) AS schema_name,
			object_name(sl_table.object_id) AS table_name,
			sl_table.table_id AS table_id,
			table_group.destination_location AS destination_location,
			table_group.workspace_id AS workspace_id,
			sl_table.state AS [state],
			sl_table.object_id AS table_object_id,
			sl_table.version AS version,
			sl_table.enable_lsn AS enable_lsn,
			sl_table.disable_lsn AS disable_lsn,
			sl_table.snapshot_phase AS snapshot_phase,
			sl_table.snapshot_current_phase_time AS snapshot_current_phase_time,
			sl_table.snapshot_retry_count AS snapshot_retry_count,
			sl_table.snapshot_start_time AS snapshot_start_time,
			sl_table.snapshot_end_time AS snapshot_end_time,
			sl_table.snapshot_row_count AS snapshot_row_count' + @exposed_reseed_id + N'
		FROM ' + @metadata_schema_name + '.change_feed_table_groups table_group JOIN ' + @metadata_schema_name + '.change_feed_tables sl_table
		ON table_group.table_group_id = sl_table.table_group_id AND '

	IF (@id_params = 1)
	BEGIN
		SET @stmt += 'sl_table.table_id = @t_id AND sl_table.table_group_id = @tg_id'
		EXEC sp_executesql @stmt, N'@t_id uniqueidentifier, @tg_id uniqueidentifier', @t_id = @table_id, @tg_id = @table_group_id;
	END
	ELSE IF (@id_params <> 1)
	BEGIN
		SET @stmt += 'sl_table.object_id = @object_id'
		EXEC sp_executesql @stmt, N'@object_id int', @object_id = @source_object_id;
	END

	RETURN 0
END

