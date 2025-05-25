SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_cleanup_dropped_table_metadata
	@table_guid uniqueidentifier
AS
BEGIN

	-- Set noncount to avoid sending extra messages to the client for every statement with "rows affected".
	SET NOCOUNT ON

	-- Get the table_id and drop_commit_time for the given table.
	DECLARE @drop_commit_time DATETIME
	DECLARE @table_id BIGINT
	SELECT @table_id = table_id, @drop_commit_time = drop_commit_time
	from sys.manageddeltatables where table_guid = @table_guid;

	-- Delete the entries for the table from the internal sys tables.
	IF (@table_id IS NOT NULL)
	BEGIN
		-- If the table has not been dropped, we cannot attempt to clean its internal metadata so throw an ex:DW_INTERNAL_SPROC_CLEANUP_ERROR_MANAGED_DELTA_TABLE_IS_NOT_DROPPED
		IF (@drop_commit_time IS NULL OR @drop_commit_time <= '1900-01-01 00:00:00.000')
		BEGIN
			RAISERROR (
				24758, -- ERROR_NUMBER
				16, -- ERROR_SEVERITY
				1, -- ERROR_STATE
				N'Table must be dropped before calling sys.sp_cleanup_dropped_table_metadata' -- ERROR_MESSAGE
				);
			
			RETURN(1) --Failure
		END

		-- Start a user transaction. We want to make sure we either delete from all tables or none if a failure happens.
		DECLARE @ORIGINAL_XACT_ABORT VARCHAR(3) = 'OFF';
		IF ( (16384 & @@OPTIONS) = 16384 ) SET @ORIGINAL_XACT_ABORT = 'ON';
		SET XACT_ABORT ON;

		BEGIN TRY
			BEGIN TRAN T1;

			DELETE FROM sys.manageddeltatables WHERE table_id = @table_id;
			DECLARE @deletedRowCountManagedDeltaTables INT = @@ROWCOUNT;

			DELETE FROM sys.manageddeltatablelogfiles WHERE table_id = @table_id;
			DECLARE @deletedRowCountManagedDeltaTableLogFiles INT = @@ROWCOUNT;

			DELETE FROM sys.manageddeltatableforks WHERE table_id = @table_id;
			DECLARE @deletedRowCountManagedDeltaTableForks INT = @@ROWCOUNT;

			DELETE FROM sys.manageddeltatablecheckpoints WHERE table_id = @table_id;
			DECLARE @deletedRowCountManagedDeltaTableCheckpoints INT = @@ROWCOUNT;

			DECLARE @deletedRowCountDiscoveredTableProperties INT = 0;
			IF OBJECT_ID('sys.discoveredtableproperties') IS NOT NULL
			BEGIN
				DELETE FROM sys.discoveredtableproperties WHERE table_id = @table_id;
				SET @deletedRowCountDiscoveredTableProperties = @@ROWCOUNT;
			END

			DECLARE @deletedRowCountStatisticsTableMetadataOld INT = 0;
			IF OBJECT_ID('sys.statistics_table_metadata') IS NOT NULL
			BEGIN
				DELETE FROM sys.statistics_table_metadata WHERE table_id = @table_id;
				SET @deletedRowCountStatisticsTableMetadataOld = @@ROWCOUNT;
			END

			DELETE FROM sys.statisticstablemetadata WHERE table_id = @table_id;
			DECLARE @deletedRowCountStatisticsTableMetadata INT = @@ROWCOUNT;

			DELETE FROM sys.statistics_histogram_metadata WHERE table_id = @table_id;
			DECLARE @deletedRowCountStatisticsHistogramMetadata INT = @@ROWCOUNT;

			SELECT @deletedRowCountManagedDeltaTables AS 'deleted_row_count_managed_delta_tables',
			@deletedRowCountManagedDeltaTableLogFiles AS 'deleted_row_count_managed_delta_log_files',
			@deletedRowCountManagedDeltaTableForks AS 'deleted_row_count_managed_delta_table_forks',
			@deletedRowCountManagedDeltaTableCheckpoints AS 'deleted_row_count_managed_delta_table_checkpoints',
			@deletedRowCountDiscoveredTableProperties AS 'deleted_row_count_discovered_table_properties',
			@deletedRowCountStatisticsTableMetadataOld AS 'deleted_row_count_statistics_table_metadata_old',
			@deletedRowCountStatisticsTableMetadata AS 'deleted_row_count_statistics_table_metadata',
			@deletedRowCountStatisticsHistogramMetadata AS 'deleted_row_count_statistics_histogram_metadata'

			COMMIT TRAN T1;
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0
				ROLLBACK TRAN T1;
		END CATCH;

		IF (@ORIGINAL_XACT_ABORT = 'OFF') SET XACT_ABORT OFF ELSE SET XACT_ABORT ON;
	END
END

