use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_get_expired_log_file_metadata
	@table_guid uniqueidentifier,
	@min_commit_sequence_id_exclusive BIGINT,
	@max_commit_sequence_id_inclusive BIGINT,
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
	commit_sequence_id
	FROM sys.manageddeltatablelogfiles
	WHERE table_id = @table_id
	AND commit_sequence_id > @min_commit_sequence_id_exclusive
	AND commit_sequence_id <= @max_commit_sequence_id_inclusive
	ORDER BY commit_sequence_id ASC;
END

