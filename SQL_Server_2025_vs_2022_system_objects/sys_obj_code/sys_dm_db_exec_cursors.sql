use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE FUNCTION sys.dm_db_exec_cursors (@spid int)
RETURNS table
AS
	RETURN SELECT *
	FROM OpenRowSet(TABLE DM_DB_EXEC_CURSORS, @spid)

