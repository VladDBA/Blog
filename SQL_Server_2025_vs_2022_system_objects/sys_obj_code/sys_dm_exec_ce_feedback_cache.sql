use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_exec_ce_feedback_cache AS 
	SELECT
		database_id,
		fingerprint,
		feedback,
		observed_count,
		state
	FROM OpenRowset(TABLE DM_EXEC_CE_FEEDBACK_CACHE)

