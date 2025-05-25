use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.json_index_paths AS
	SELECT i.id AS object_id,
		i.indid AS index_id,
		p.path
	FROM sys.sysidxstats i
	CROSS APPLY OPENROWSET(TABLE JSON_INDEX_PATHS, i.id, i.indid) p
	WHERE (i.indid >= 1216000 and i.indid < 1216256)
		AND has_access('CO', i.id) = 1

