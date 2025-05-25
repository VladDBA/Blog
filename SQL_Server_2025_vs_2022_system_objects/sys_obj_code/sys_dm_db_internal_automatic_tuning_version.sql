SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

-- in contained db the db schema collation will be different from
-- db default collation, so we forcefully convert the collation of p_name
-- to db default
CREATE VIEW sys.dm_db_internal_automatic_tuning_version
AS
SELECT [bigint_value] AS [version_code]
FROM OpenRowSet(TABLE DM_DB_INTERNAL_ATS_PROPERTIES)
WHERE [p_name] COLLATE database_default =N'version'

