SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.index_columns AS
	SELECT isc.idmajor AS object_id,
		isc.idminor AS index_id,
		isc.subid AS index_column_id,
		isc.intprop AS column_id,
	  	isc.tinyprop1 AS key_ordinal,
	  	isc.tinyprop2 AS partition_ordinal,
	  	convert (bit, isc.status & 0x4) AS is_descending_key,	-- ISC_IC_DESC_KEY
		convert (bit, isc.status & 0x10) AS is_included_column,	-- ISC_IC_INCLUDED
		iif (serverproperty('EngineEdition') = 11 AND cast(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS sysname) = 'DataWarehouse', cast(0 as tinyint), isc.tinyprop4) AS column_store_order_ordinal,
		iif (serverproperty('EngineEdition') = 11 AND cast(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS sysname) = 'DataWarehouse', isc.tinyprop4, cast(0 as tinyint)) AS data_clustering_ordinal
	FROM sys.sysiscols isc
	INNER JOIN sys.sysschobjs$ obj ON obj.id = isc.idmajor
	INNER JOIN sys.sysidxstats idxstats ON obj.id = idxstats.id AND isc.idminor = idxstats.indid
	WHERE (isc.status & 2) <> 0
		AND has_access('CO', isc.idmajor) = 1	-- ISC_IND_COL
		AND (isc.status & 0x10 = 0 OR obj.status2 & 0x00000008 = 0 OR isc.idminor = 1)	-- ISC_IC_INCLUDED, OBJTAB2_HEKATON, ISC_IC_IDMINOR
		AND (idxstats.status & 0x04000000) = 0 -- !IS_IND_RESUMABLE


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.index_columns AS
	SELECT isc.idmajor AS object_id,
		isc.idminor AS index_id,
		isc.subid AS index_column_id,
		isc.intprop AS column_id,
	  	isc.tinyprop1 AS key_ordinal,
	  	isc.tinyprop2 AS partition_ordinal,
	  	convert (bit, isc.status & 0x4) AS is_descending_key,	-- ISC_IC_DESC_KEY
		convert (bit, isc.status & 0x10) AS is_included_column,	-- ISC_IC_INCLUDED
		isc.tinyprop4 as column_store_order_ordinal
	FROM sys.sysiscols isc
	INNER JOIN sys.sysschobjs$ obj ON obj.id = isc.idmajor
	INNER JOIN sys.sysidxstats idxstats ON obj.id = idxstats.id AND isc.idminor = idxstats.indid
	WHERE (isc.status & 2) <> 0
		AND has_access('CO', isc.idmajor) = 1	-- ISC_IND_COL
		AND (isc.status & 0x10 = 0 OR obj.status2 & 0x00000008 = 0 OR isc.idminor = 1)	-- ISC_IC_INCLUDED, OBJTAB2_HEKATON, ISC_IC_IDMINOR
		AND (idxstats.status & 0x04000000) = 0 -- !IS_IND_RESUMABLE

