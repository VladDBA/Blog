use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_get_fork_guids
	@batch_size INT,
	@min_commit_sequence_id_exclusive BIGINT
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP (@batch_size) fork_guid, commit_sequence_id
	FROM sys.manageddeltatableforks
	WHERE commit_sequence_id > @min_commit_sequence_id_exclusive
	ORDER BY commit_sequence_id;
END

