use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_os_memory_nodes_processor_groups AS
	SELECT *
	FROM OpenRowSet(TABLE DM_OS_MEMORY_NODES_PROCESSOR_GROUPS)

