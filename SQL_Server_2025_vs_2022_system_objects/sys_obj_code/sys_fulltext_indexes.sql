SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.fulltext_indexes AS
	SELECT fti.id AS object_id,
		fti.indid AS unique_index_id,
		convert(int, sov.value) AS index_version,
		ftc.indepid AS fulltext_catalog_id,
		sysconv(bit, fti.status & 8) AS is_enabled,		--FTIND_ACTIVE
		convert(char(1), substring('OMxA',1+(fti.status&3),1)) AS change_tracking_state,	--FTIND_CHANGETRACKING | FTIND_AUTOUPDATE
		ts.name AS change_tracking_state_desc,
		sysconv(bit, fti.status & 16) AS has_crawl_completed,	--FTIND_CRAWLCOMPLETED
		fti.crtype AS crawl_type,
		ct.name AS crawl_type_desc,
		fti.crstart AS crawl_start_date,
		fti.crend AS crawl_end_date,
		fti.crtsnext AS incremental_timestamp,
		case ISNULL(fts.indepid,0)  -- NULL means using system default, change it to 0.
			WHEN -2 then NULL -- -2 means disabled, change it to null
			ELSE ISNULL(fts.indepid, 0)
		end AS stoplist_id,
		ftp.indepid AS property_list_id,
		fti.fgid AS data_space_id
	FROM sys.sysftinds fti
	LEFT JOIN sys.syssingleobjrefs ftc ON ftc.depid = fti.id AND ftc.class = 39 AND ftc.depsubid = 0 -- SRC_FTITABTOCAT
	LEFT JOIN sys.syssingleobjrefs fts ON fts.depid = fti.id AND fts.class = 41 AND fts.depsubid = 0 -- SRC_FTITABTOSTOP
	LEFT JOIN sys.syssingleobjrefs ftp ON ftp.depid = fti.id AND ftp.class = 103 AND ftp.depsubid = 0 
	LEFT JOIN sys.sysobjvalues     sov ON sov.objid = fti.id AND sov.valclass = 176 -- SVC_FULLTEXT_WB_VERSION
	LEFT JOIN sys.syspalvalues ts ON ts.class = 'FITS' AND ts.value = fti.status & 3
	LEFT JOIN sys.syspalnames ct ON ct.class = 'FTCT' AND ct.value = fti.crtype
	WHERE has_access('CO', fti.id) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.fulltext_indexes AS
	SELECT fti.id AS object_id,
		fti.indid AS unique_index_id,
		ftc.indepid AS fulltext_catalog_id,
		sysconv(bit, fti.status & 8) AS is_enabled,		--FTIND_ACTIVE
		convert(char(1), substring('OMxA',1+(fti.status&3),1)) AS change_tracking_state,	--FTIND_CHANGETRACKING | FTIND_AUTOUPDATE
		ts.name AS change_tracking_state_desc,
		sysconv(bit, fti.status & 16) AS has_crawl_completed,	--FTIND_CRAWLCOMPLETED
		fti.crtype AS crawl_type,
		ct.name AS crawl_type_desc,
		fti.crstart AS crawl_start_date,
		fti.crend AS crawl_end_date,
		fti.crtsnext AS incremental_timestamp,
		case ISNULL(fts.indepid,0)  -- NULL means using system default, change it to 0.
			WHEN -2 then NULL -- -2 means disabled, change it to null
			ELSE ISNULL(fts.indepid, 0)
		end AS stoplist_id,
		ftp.indepid AS property_list_id,
		fti.fgid AS data_space_id
	FROM sys.sysftinds fti
	LEFT JOIN sys.syssingleobjrefs ftc ON ftc.depid = fti.id AND ftc.class = 39 AND ftc.depsubid = 0 -- SRC_FTITABTOCAT
	LEFT JOIN sys.syssingleobjrefs fts ON fts.depid = fti.id AND fts.class = 41 AND fts.depsubid = 0 -- SRC_FTITABTOSTOP
	LEFT JOIN sys.syssingleobjrefs ftp ON ftp.depid = fti.id AND ftp.class = 103 AND ftp.depsubid = 0 
	LEFT JOIN sys.syspalvalues ts ON ts.class = 'FITS' AND ts.value = fti.status & 3
	LEFT JOIN sys.syspalnames ct ON ct.class = 'FTCT' AND ct.value = fti.crtype
	WHERE has_access('CO', fti.id) = 1

