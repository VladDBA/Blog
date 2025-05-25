use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_cleanup_expired_log_file_metadata
	@table_guid uniqueidentifier,
	@max_commit_sequence_id_inclusive BIGINT,
	@batch_size INT,
	@deleted_row_count INT OUTPUT
AS
BEGIN
	-- Get the table_id for the given table.
	DECLARE @table_id BIGINT
	SELECT @table_id = table_id
	FROM sys.manageddeltatables WHERE table_guid = @table_guid;
	
	-- set nocount to avoid sending extra messages to the client for every statement with "rows affected".
	SET NOCOUNT ON
	
	-- delete from sys.manageddeltatablelogfiles for given batch size.
	DELETE TOP (@batch_size) FROM sys.manageddeltatablelogfiles 
	WHERE table_id = @table_id AND commit_sequence_id <= @max_commit_sequence_id_inclusive; 
	
	SET @deleted_row_count = @@ROWCOUNT;
END

