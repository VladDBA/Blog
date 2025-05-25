use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_drop_discoveredtableproperties
AS
	-- Allow only on Trident frontend instance
	EXEC sys.sp_ensure_trident_frontend

	-- Don't return impacted row count, as we don't process the output of this query.
	SET NOCOUNT ON

	-- Drop the table.
	DROP TABLE IF EXISTS sys.discoveredtableproperties;


