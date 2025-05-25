SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_os_memory_allocations_filtered AS 
	SELECT memory_object_address = MAX(memory_object_address),sum(size_in_bytes) as sum_bytes, line_num, source_file from sys.dm_os_memory_allocations 
	GROUP BY
		source_file, line_num

