SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.sysdatabases AS
	SELECT d.database_logical_name as name,
		convert(smallint, d.id_nonrepl) AS dbid,
		sid,
		mode = convert(smallint, 0),
		status = convert(int, dbprop((case when serverproperty('EngineEdition') in (5,12) then DB_ID() else d.id end), 'status80')),
		status2 = convert(int,status2 & 0x77937C00),
		crdate,
		reserved = convert(datetime, 0),
		category = convert(int,category & 0x37),
		cmptlevel,
		filename = convert(nvarchar(260), case when serverproperty('EngineEdition') in (5,12) then NULL else dbprop(d.id, 'primaryfilename') end),
		version = convert(smallint, dbprop((case when serverproperty('EngineEdition') in (5,12) then DB_ID() else d.id end), 'Version'))
	FROM sys.sysdbreg$ d OUTER APPLY OpenRowset(TABLE DBPROP, (case when serverproperty('EngineEdition') in (5,12) then DB_ID() else d.id end)) p
	WHERE id < 0x7fff AND repl_sys_db_visible(id) = 1
	AND has_access('DB', (case when serverproperty('EngineEdition') in (5,12) then DB_ID() else id end)) = 1 


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.sysdatabases AS
	SELECT d.database_logical_name as name,
		convert(smallint, d.id_nonrepl) AS dbid,
		sid,
		mode = convert(smallint, 0),
		status = convert(int, dbprop((case when serverproperty('EngineEdition') = 5 then DB_ID() else d.id end), 'status80')),
		status2 = convert(int,status2 & 0x77937C00),
		crdate,
		reserved = convert(datetime, 0),
		category = convert(int,category & 0x37),
		cmptlevel,
		filename = convert(nvarchar(260), case when serverproperty('EngineEdition') = 5 then NULL else dbprop(d.id, 'primaryfilename') end),
		version = convert(smallint, dbprop((case when serverproperty('EngineEdition') = 5 then DB_ID() else d.id end), 'Version'))
	FROM sys.sysdbreg$ d OUTER APPLY OpenRowset(TABLE DBPROP, (case when serverproperty('EngineEdition') = 5 then DB_ID() else d.id end)) p
	WHERE id < 0x7fff AND repl_sys_db_visible(id) = 1
	AND has_access('DB', (case when serverproperty('EngineEdition') = 5 then DB_ID() else id end)) = 1 

