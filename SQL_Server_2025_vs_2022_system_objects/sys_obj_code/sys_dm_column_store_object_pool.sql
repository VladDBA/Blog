use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_column_store_object_pool AS
	SELECT op.database_id AS database_id,
	op.object_id AS object_id,
	op.index_id AS index_id,
	op.partition_number AS partition_number,
	op.column_id AS column_id,
	op.row_group_id AS row_group_id,
	sp.value AS object_type,
	sp.name AS object_type_desc,
	op.access_count AS access_count,
	op.memory_used_in_bytes AS memory_used_in_bytes,
	op.object_load_time AS object_load_time
	FROM OpenRowset(TABLE DM_COLUMNSTORE_OBJECT_POOL_STATS) as op
	INNER JOIN sys.syspalvalues sp
		ON sp.class = 'COT'
			AND ((op.object_type = 0 AND sp.value = 0)
			OR (op.object_type = 1 AND sp.value = 1)
			OR (op.object_type = 2 AND sp.value = 2)
			OR (op.object_type = 4 AND sp.value = 3)
			OR (op.object_type = 5 AND sp.value = 4)
			OR (op.object_type = 6 AND sp.value = 5)
			OR (op.object_type = 8 AND sp.value = 6)
			OR (op.object_type = 9 AND sp.value = 7)
			OR (op.object_type = 10 AND sp.value = 8))



/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_column_store_object_pool AS
	SELECT op.database_id AS database_id,
	op.object_id AS object_id,
	op.index_id AS index_id,
	op.partition_number AS partition_number,
	op.column_id AS column_id,
	op.row_group_id AS row_group_id,
	sp.value AS object_type,
	sp.name AS object_type_desc,
	op.access_count AS access_count,
	op.memory_used_in_bytes AS memory_used_in_bytes,
	op.object_load_time AS object_load_time
	FROM OpenRowset(TABLE DM_COLUMNSTORE_OBJECT_POOL_STATS) as op
	INNER JOIN sys.syspalvalues sp
		ON sp.class = 'COT'
			AND ((op.object_type = 0 AND sp.value = 0)
			OR (op.object_type = 1 AND sp.value = 1)
			OR (op.object_type = 2 AND sp.value = 2)
			OR (op.object_type = 4 AND sp.value = 3)
			OR (op.object_type = 5 AND sp.value = 4)
			OR (op.object_type = 6 AND sp.value = 5)
			OR (op.object_type = 8 AND sp.value = 6))


