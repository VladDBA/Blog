SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.change_tracking_databases AS
	SELECT 
		id_nonrepl AS database_id,
		-- UNDONE: reconcile this for Sterling if needed.
		-- case when d.name = db_name() then COALESCE(convert(int, dbprop(db_id(), 'logicaldbid')), d.id) else d.id end AS database_id,
		convert(tinyint, DatabasePropertyEx(name, 'IsChangeTrackingAutoCleanup')) as is_auto_cleanup_on,
		convert(int, DatabasePropertyEx(name, 'ChangeTrackingRetentionPeriod')) as retention_period,
		convert(tinyint, DatabasePropertyEx(name, 'ChangeTrackingRetentionUnits')) as retention_period_units,
		convert(nvarchar(60), DatabasePropertyEx(name, 'ChangeTrackingRetentionUnitsDesc')) as retention_period_units_desc,
		change_tracking_max_cleanup_version((case when serverproperty('EngineEdition') in (5,12) then DB_ID() else id end)) as max_cleanup_version
	FROM sys.sysdbreg$
	WHERE id < 0x7fff AND repl_sys_db_visible(id) = 1
		AND (engineedition() <> 11 OR sys_db_visible_polaris(id) = 1)
		AND has_access('DB', (case when serverproperty('EngineEdition') in (5,12) then DB_ID() else id end)) = 1
		AND (category & 128) = 128


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.change_tracking_databases AS
	SELECT 
		id_nonrepl AS database_id,
		-- UNDONE: reconcile this for Sterling if needed.
		-- case when d.name = db_name() then COALESCE(convert(int, dbprop(db_id(), 'logicaldbid')), d.id) else d.id end AS database_id,
		convert(tinyint, DatabasePropertyEx(name, 'IsChangeTrackingAutoCleanup')) as is_auto_cleanup_on,
		convert(int, DatabasePropertyEx(name, 'ChangeTrackingRetentionPeriod')) as retention_period,
		convert(tinyint, DatabasePropertyEx(name, 'ChangeTrackingRetentionUnits')) as retention_period_units,
		convert(nvarchar(60), DatabasePropertyEx(name, 'ChangeTrackingRetentionUnitsDesc')) as retention_period_units_desc,
		change_tracking_max_cleanup_version((case when serverproperty('EngineEdition') = 5 then DB_ID() else id end)) as max_cleanup_version
	FROM sys.sysdbreg$
	WHERE id < 0x7fff AND repl_sys_db_visible(id) = 1
		AND (engineedition() <> 11 OR sys_db_visible_polaris(id) = 1)
		AND has_access('DB', (case when serverproperty('EngineEdition') = 5 then DB_ID() else id end)) = 1
		AND (category & 128) = 128

