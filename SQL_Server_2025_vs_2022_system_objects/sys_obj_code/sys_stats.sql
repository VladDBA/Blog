use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.stats AS
	SELECT id AS object_id, name,
		indid AS stats_id,
		sysconv(bit, status & 0x2000) AS auto_created,	-- IS_STATS_AUTO_CRT
		sysconv(bit, CASE WHEN (status & 0x2001) = 0 THEN 1 ELSE 0 END) AS user_created,
		sysconv(bit, status & 0x4000) AS no_recompute,	-- IS_STATS_NORECOMP
		sysconv(bit, status & 0x20000) AS has_filter,	-- IS_HAS_FILTER
		case when (status & 0x20000) != 0 then object_definition(id, indid, 9) else NULL end AS filter_definition, -- x_euncStats
		convert(bit, 0) as is_temporary,
		sysconv(bit, status & 0x800000) AS is_incremental,
		sysconv(bit, status & 0x10000000) AS has_persisted_sample,
		CASE ISNULL(o.value, 0)
			WHEN 1 THEN 1
			WHEN 2 THEN 1 -- Basic statistics is also streaming stats.
			ELSE 0 -- to prevent confusion
		END as stats_generation_method,
		CASE ISNULL(o.value, 0)
			WHEN 1 THEN 'Streaming statistics computed by CREATE or UPDATE statistics and auto statistics'
			WHEN 2 THEN 'Streaming statistics computed during load' -- not yet created
			ELSE 'Sort based statistics'
		END collate Latin1_General_CI_AS_KS_WS AS stats_generation_method_desc, -- These _DESC columns should always be the fixed latin collation. Reason is that unlike other columns in system tables that can change depending on the collation setting of the database/instance, these are always fixed on all installations of SQL Server.
		CASE
			WHEN sysconv(bit, status & 0x2000) =  1 THEN convert(bit, 1) -- Applies to auto stats, they are also auto drop.
			ELSE sysconv(bit, status & 0x1000)
		END AS auto_drop, -- IS_STATS_AUTO_DROP
		sysconv(tinyint, ISNULL(o2.value, 1)) AS replica_role_id,
		CASE o2.value
			WHEN 2 THEN CAST('SECONDARY' AS nvarchar(60))
			WHEN 3 THEN CAST('GEO REPLICATION FORWARDER' AS nvarchar(60))
			WHEN 4 THEN CAST('GEO HA SECONDARY' AS nvarchar(60))
			WHEN 5 THEN CAST('NAMED REPLICA' AS nvarchar(60))
			ELSE CAST('PRIMARY' AS nvarchar(60))
		END collate Latin1_General_CI_AS_KS_WS AS replica_role_desc,
		convert(sysname, o3.value) AS replica_name
	FROM sys.sysidxstats s 
	LEFT JOIN sys.sysobjvalues o ON o.valclass = 143 AND o.valnum >= 1 AND id = o.objid AND indid = o.subobjid	-- SVC_STREAMING_STATISTICS
	LEFT JOIN sys.sysobjvalues o2 ON o2.valclass = 177 AND o2.valnum = 0 AND id = o2.objid AND indid = o2.subobjid	-- SVC_STATS_ON_SECONDARY_REPLICA_INFO, replica role id
	LEFT JOIN sys.sysobjvalues o3 ON o3.valclass = 177 AND o3.valnum = 1 AND id = o3.objid AND indid = o3.subobjid	-- SVC_STATS_ON_SECONDARY_REPLICA_INFO, replica name
	WHERE (status & 2)  = 2			-- IS_STATS
		AND has_access('CO', id) = 1
		AND indid NOT IN 
		(
			SELECT t.valnum as stats_id
			FROM tempstatvals t
			WHERE t.valnum < 0x40000
				AND t.subobjid = s.id
		)
		AND (s.status & 0x04000000) = 0 -- !IS_IND_RESUMABLE
	UNION ALL
	SELECT	t.subobjid as object_id,
			v.name COLLATE catalog_default AS name,
			t.valnum as stats_id,
			v.auto_created, 
			v.user_created,
			v.no_recompute,
			convert(bit, 0) as has_filter,
			NULL as filter_definition,
			convert(bit, 1) as is_temporary,
			convert(bit, 0) as is_incremental,
			v.has_persisted_sample,
			sysconv(int, 0) AS stats_generation_method,
			'Sort based statistics' collate Latin1_General_CI_AS_KS_WS AS stats_generation_method_desc,
			v.auto_drop,
			sysconv(tinyint, NULL) as replica_role_id,
			CAST(NULL AS nvarchar(60)) as replica_role_desc,
			convert(sysname, NULL) as replica_name
	FROM	tempstatvals t
			OUTER APPLY 
			OPENROWSET(TABLE TEMPSTATS, t.objid, t.subobjid, t.valnum, convert(bit, 0)) v 
	WHERE 	has_access('CO', t.subobjid) = 1
	UNION ALL
	SELECT	fidoMd.object_id, 
			fidoMd.name COLLATE catalog_default AS name,
			fidoMd.stats_id,
			fidoMd.auto_created,
			fidoMd.user_created,
			fidoMd.no_recompute,
			fidoMd.has_filter,
			fidoMd.filter_definition COLLATE catalog_default AS filter_definition,
			fidoMd.is_temporary,
			fidoMd.is_incremental,
			sysconv(bit, 0) AS has_persisted_sample,
			sysconv(int, 1) AS stats_generation_method,
			'Streaming statistics computed by CREATE or UPDATE statistics and auto statistics' collate Latin1_General_CI_AS_KS_WS AS stats_generation_method_desc,
			sysconv(bit, 1) AS auto_drop,
			sysconv(tinyint, NULL) as replica_role_id,
			CAST(NULL AS nvarchar(60)) as replica_role_desc,
			convert(sysname, NULL) as replica_name
	FROM 	OPENROWSET(TABLE DM_FIDO_AUTOSTATS_METADATA) fidoMd -- Gets SQL DW Fido Autostats Info, returns 0 rows for non-Fido databases
	WHERE 	has_access('CO', fidoMd.object_id) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.stats AS
	SELECT id AS object_id, name,
		indid AS stats_id,
		sysconv(bit, status & 0x2000) AS auto_created,	-- IS_STATS_AUTO_CRT
		sysconv(bit, CASE WHEN (status & 0x2001) = 0 THEN 1 ELSE 0 END) AS user_created,
		sysconv(bit, status & 0x4000) AS no_recompute,	-- IS_STATS_NORECOMP
		sysconv(bit, status & 0x20000) AS has_filter,	-- IS_HAS_FILTER
		case when (status & 0x20000) != 0 then object_definition(id, indid, 9) else NULL end AS filter_definition, -- x_euncStats
		convert(bit, 0) as is_temporary,
		sysconv(bit, status & 0x800000) AS is_incremental,
		sysconv(bit, status & 0x10000000) AS has_persisted_sample,
		CASE ISNULL(o.value, 0)
			WHEN 1 THEN 1
			WHEN 2 THEN 1 -- Basic statistics is also streaming stats.
			ELSE 0 -- to prevent confusion
		END as stats_generation_method,
		CASE ISNULL(o.value, 0)
			WHEN 1 THEN 'Streaming statistics computed by CREATE or UPDATE statistics and auto statistics'
			WHEN 2 THEN 'Streaming statistics computed during load' -- not yet created
			ELSE 'Sort based statistics'
		END collate Latin1_General_CI_AS_KS_WS AS stats_generation_method_desc, -- These _DESC columns should always be the fixed latin collation. Reason is that unlike other columns in system tables that can change depending on the collation setting of the database/instance, these are always fixed on all installations of SQL Server.
		CASE
			WHEN sysconv(bit, status & 0x2000) =  1 THEN convert(bit, 1) -- Applies to auto stats, they are also auto drop.
			ELSE sysconv(bit, status & 0x1000)
		END AS auto_drop -- IS_STATS_AUTO_DROP
	FROM sys.sysidxstats s 
	LEFT JOIN sys.sysobjvalues o ON o.valclass = 143 AND o.valnum >= 1 AND id = o.objid AND indid = o.subobjid	-- SVC_STREAMING_STATISTICS
	WHERE (status & 2)  = 2			-- IS_STATS
		AND has_access('CO', id) = 1
		AND indid NOT IN 
		(
			SELECT t.valnum as stats_id
			FROM tempstatvals t
			WHERE t.valnum < 0x40000
				AND t.subobjid = s.id
		)
		AND (s.status & 0x04000000) = 0 -- !IS_IND_RESUMABLE
	UNION ALL
	SELECT	t.subobjid as object_id,
			v.name COLLATE catalog_default AS name,
			t.valnum as stats_id,
			v.auto_created, 
			v.user_created,
			v.no_recompute,
			convert(bit, 0) as has_filter,
			'' as filter_definition,
			convert(bit, 1) as is_temporary,
			convert(bit, 0) as is_incremental,
			v.has_persisted_sample,
			sysconv(int, 0) AS stats_generation_method,
			'Sorted statistics (default)' collate Latin1_General_CI_AS_KS_WS AS stats_generation_method_desc,
			v.auto_drop
	FROM	tempstatvals t
			OUTER APPLY 
			OPENROWSET(TABLE TEMPSTATS, t.objid, t.subobjid, t.valnum, convert(bit, 0)) v 
	WHERE 	has_access('CO', t.subobjid) = 1
	UNION ALL
	SELECT	fidoMd.object_id, 
			fidoMd.name COLLATE catalog_default AS name,
			fidoMd.stats_id,
			fidoMd.auto_created,
			fidoMd.user_created,
			fidoMd.no_recompute,
			fidoMd.has_filter,
			fidoMd.filter_definition COLLATE catalog_default AS filter_definition,
			fidoMd.is_temporary,
			fidoMd.is_incremental,
			sysconv(bit, 0) AS has_persisted_sample,
			sysconv(int, 1) AS stats_generation_method,
			'Streaming statistics computed by CREATE or UPDATE statistics and auto statistics' collate Latin1_General_CI_AS_KS_WS AS stats_generation_method_desc,
			sysconv(bit, 1) AS auto_drop
	FROM 	OPENROWSET(TABLE DM_FIDO_AUTOSTATS_METADATA) fidoMd -- Gets SQL DW Fido Autostats Info, returns 0 rows for non-Fido databases
	WHERE 	has_access('CO', fidoMd.object_id) = 1

