SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.internal_partitions AS
	WITH Group1(partition_id, object_id, index_id, partition_number, 
			hobt_id, internal_object_type, row_group_id, rows, data_compression, xml_compression)
	AS
	(
	SELECT
		rs.rowsetid AS partition_id,
		-- The next 4 columns form a unique Composite ID for internal partition.
		rs.idmajor AS object_id, -- This is the first column
		rs.idminor AS index_id,  -- This is the second column
		rs.numpart AS partition_number, -- This is the third column
		rs.rowsetid AS hobt_id, -- This is the fourth column
		MAX(rs.ownertype) as internal_object_type,
		NULL AS row_group_id, -- No rowgroup id for delete bitmaps, delete buffer or mapping index.
		SUM(isnull(ct.rows, rs.rcrows))	AS rows,
		MAX(rs.cmprlevel)	AS data_compression,
		sysconv(bit, rs.status & 0x04000000) AS xml_compression
	FROM sys.sysrowsets rs OUTER APPLY OpenRowset(TABLE ALUCOUNT, rs.rowsetid, 0, 0, 0) ct 
	WHERE rs.ownertype IN (2,4,5,6,7) -- delete bitmaps, delete buffer, mapping index, sort, and lob redirect table.
	GROUP BY rs.rowsetid, rs.idmajor, rs.idminor, rs.numpart, rs.status
	),
	Group2(partition_id, object_id, index_id, partition_number, 
			hobt_id, internal_object_type, row_group_id, rows, data_compression, xml_compression )
	AS
	(
		SELECT
			rs.rowsetid AS partition_id, 
		-- The next 4 columns form a unique Composite ID for internal partition.
		rs.idmajor AS object_id, -- This is the first column
			rs.idminor AS index_id,  -- This is the second column
			rs.numpart AS partition_number, -- This is the third column
			rs.rowsetid AS hobt_id, -- This is the fourth column
			MAX(rs.ownertype) as internal_object_type,
		MAX(rg.row_group_id) AS row_group_id,
			SUM(isnull(ct.rows, rs.rcrows))	AS rows,
		MAX(rs.cmprlevel)	AS data_compression,
		sysconv(bit, rs.status & 0x04000000) AS xml_compression
		FROM sys.sysrowsets rs OUTER APPLY OpenRowset(TABLE ALUCOUNT, rs.rowsetid, 0, 0, 0) ct
	LEFT JOIN OpenRowset(TABLE COLUMNSTORE_ROW_GROUPS, 0, 0) rg ON 
		rs.idmajor = rg.parent_object_id AND rs.idminor = rg.parent_index_id AND rs.rowsetid = rg.delta_store_hobt_id
	WHERE rs.ownertype IN (3) -- deltastores
		GROUP BY rs.rowsetid, rs.idmajor, rs.idminor, rs.numpart, rs.status
	)
	SELECT 
		P.partition_id,
		P.object_id,
		P.index_id,
		P.partition_number,
		P.hobt_id,
		P.internal_object_type,
		rst.name as internal_object_type_desc,
		P.row_group_id, 
		P.rows, 
		P.data_compression,
		cl.name AS data_compression_desc,
		P.xml_compression,
		case when (P.xml_compression = 1) then 'ON'
		when (P.xml_compression = 0) then 'OFF' end as xml_compression_desc
	FROM (SELECT * from Group1
		UNION ALL
		SELECT * from Group2) AS P
	LEFT JOIN sys.syspalvalues cl ON cl.class = 'CMPL' AND cl.value = P.data_compression
	LEFT JOIN sys.syspalvalues rst ON rst.class = 'ROTY' AND rst.value = P.internal_object_type


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.internal_partitions AS
	WITH Group1(partition_id, object_id, index_id, partition_number, 
			hobt_id, internal_object_type, row_group_id, rows, data_compression, xml_compression)
	AS
	(
	SELECT
		rs.rowsetid AS partition_id,
		-- The next 4 columns form a unique Composite ID for internal partition.
		rs.idmajor AS object_id, -- This is the first column
		rs.idminor AS index_id,  -- This is the second column
		rs.numpart AS partition_number, -- This is the third column
		rs.rowsetid AS hobt_id, -- This is the fourth column
		MAX(rs.ownertype) as internal_object_type,
		NULL AS row_group_id, -- No rowgroup id for delete bitmaps, delete buffer or mapping index.
		SUM(isnull(ct.rows, rs.rcrows))	AS rows,
		MAX(rs.cmprlevel)	AS data_compression,
		sysconv(bit, rs.status & 0x04000000) AS xml_compression
	FROM sys.sysrowsets rs OUTER APPLY OpenRowset(TABLE ALUCOUNT, rs.rowsetid, 0, 0, 0) ct 
	WHERE rs.ownertype IN (2,4,5) -- delete bitmaps, delete buffer and mapping index.
	GROUP BY rs.rowsetid, rs.idmajor, rs.idminor, rs.numpart, rs.status
	),
	Group2(partition_id, object_id, index_id, partition_number, 
			hobt_id, internal_object_type, row_group_id, rows, data_compression, xml_compression )
	AS
	(
		SELECT
			rs.rowsetid AS partition_id, 
		-- The next 4 columns form a unique Composite ID for internal partition.
		rs.idmajor AS object_id, -- This is the first column
			rs.idminor AS index_id,  -- This is the second column
			rs.numpart AS partition_number, -- This is the third column
			rs.rowsetid AS hobt_id, -- This is the fourth column
			MAX(rs.ownertype) as internal_object_type,
		MAX(rg.row_group_id) AS row_group_id,
			SUM(isnull(ct.rows, rs.rcrows))	AS rows,
		MAX(rs.cmprlevel)	AS data_compression,
		sysconv(bit, rs.status & 0x04000000) AS xml_compression
		FROM sys.sysrowsets rs OUTER APPLY OpenRowset(TABLE ALUCOUNT, rs.rowsetid, 0, 0, 0) ct
	LEFT JOIN OpenRowset(TABLE COLUMNSTORE_ROW_GROUPS, 0, 0) rg ON 
		rs.idmajor = rg.parent_object_id AND rs.idminor = rg.parent_index_id AND rs.rowsetid = rg.delta_store_hobt_id
	WHERE rs.ownertype IN (3) -- deltastores
		GROUP BY rs.rowsetid, rs.idmajor, rs.idminor, rs.numpart, rs.status
	)
	SELECT 
		P.partition_id,
		P.object_id,
		P.index_id,
		P.partition_number,
		P.hobt_id,
		P.internal_object_type,
		rst.name as internal_object_type_desc,
		P.row_group_id, 
		P.rows, 
		P.data_compression,
		cl.name AS data_compression_desc,
		P.xml_compression,
		case when (P.xml_compression = 1) then 'ON'
		when (P.xml_compression = 0) then 'OFF' end as xml_compression_desc
	FROM (SELECT * from Group1
		UNION ALL
		SELECT * from Group2) AS P
	LEFT JOIN sys.syspalvalues cl ON cl.class = 'CMPL' AND cl.value = P.data_compression
	LEFT JOIN sys.syspalvalues rst ON rst.class = 'ROTY' AND rst.value = P.internal_object_type

