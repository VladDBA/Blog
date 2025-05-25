use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_server_managed_identities AS
	SELECT *
	FROM OpenRowset(TABLE SERVER_MANAGED_IDENTITIES)

