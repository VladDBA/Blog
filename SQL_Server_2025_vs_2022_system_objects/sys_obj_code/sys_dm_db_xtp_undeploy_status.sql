use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_db_xtp_undeploy_status
AS
	SELECT *
	FROM OpenRowset(TABLE XTP_UNDEPLOY_STATUS)

