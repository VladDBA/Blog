SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_os_memory_health_history
AS
	SELECT 
	    snapshot_time,
		allocation_potential_memory_mb,
		reclaimable_cache_memory_mb,
		top_memory_clerks,
		severity_level
	FROM OpenRowSet(TABLE DM_OS_MEMORY_HEALTH_HISTORY)

