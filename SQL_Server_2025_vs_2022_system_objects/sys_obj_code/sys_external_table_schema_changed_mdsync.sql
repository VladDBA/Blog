use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.external_table_schema_changed_mdsync
AS
SELECT 
	ords.objid AS database_id,										-- database id
	ords.subobjid AS table_id,										-- external table id
	schema_changed = IIF(value IS NULL, 0, value) 
FROM sys.sysobjvalues AS ords 
WHERE ords.valclass = 167             								-- SVC_DELTA_EXTERNAL_TABLE_SCHEMA_CHANGES_MDSYNC
	AND ords.valnum = 0    											-- VALNUM_DELTA_EXTERNAL_TABLE_SCHEMA_CHANGES_MDSYNC_SCHEMA_CHANGED


