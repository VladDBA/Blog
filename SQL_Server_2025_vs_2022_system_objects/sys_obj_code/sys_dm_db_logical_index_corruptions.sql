use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_db_logical_index_corruptions AS
	SELECT * from OpenRowSet(TABLE DM_DB_LOGICAL_INDEX_CORRUPTIONS)
