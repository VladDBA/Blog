use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_external_policy_excluded_role_members AS
	SELECT *
	FROM OpenRowset(TABLE EXTERNAL_POLICY_EXCLUDED_ROLE_MEMBERS)

