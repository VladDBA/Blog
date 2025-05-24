SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_get_expired_checkpoint_file_metadata
	@table_guid uniqueidentifier,
	@min_delta_log_commit_sequence_id_exclusive BIGINT,
	@part_exclusive INT,
	@max_delta_log_commit_sequence_id_exclusive BIGINT,
	@batch_size INT
AS
BEGIN
	-- Get the table_id for the given table.
	DECLARE @table_id BIGINT;

	SELECT @table_id = table_id
	FROM sys.manageddeltatables
	WHERE table_guid = @table_guid;
	
	-- Set nocount to avoid sending extra messages to the client
	-- for every statement with "rows affected".
	SET NOCOUNT ON;
	
	SELECT TOP (@batch_size) source_database_guid AS database_guid,
	UPPER(CAST(source_table_guid AS VARCHAR(36))) AS path_relative_to_log_directory,
	UPPER(CAST(file_guid AS VARCHAR(36))) AS file_name,
	delta_log_commit_sequence_id,
	part
	FROM sys.manageddeltatablecheckpoints
	WHERE table_id = @table_id
	AND 
		(delta_log_commit_sequence_id = @min_delta_log_commit_sequence_id_exclusive AND part > @part_exclusive OR
		delta_log_commit_sequence_id > @min_delta_log_commit_sequence_id_exclusive)
	AND delta_log_commit_sequence_id < @max_delta_log_commit_sequence_id_exclusive
	ORDER BY delta_log_commit_sequence_id ASC, part ASC;
END

