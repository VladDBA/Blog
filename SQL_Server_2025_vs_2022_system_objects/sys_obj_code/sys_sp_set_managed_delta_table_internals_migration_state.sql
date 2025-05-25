use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_set_managed_delta_table_internals_migration_state
@state INT
AS
	-- Allow only on Trident frontend instance
	EXEC sys.sp_ensure_trident_frontend

	SET NOCOUNT ON

	-- Migration state will be 0 when both phase1 and phase2 are turned off. In such case drop any v2 tables.
	IF @state = 0
		BEGIN
			DROP TABLE IF EXISTS sys.manageddeltatables;
			DROP TABLE IF EXISTS sys.manageddeltatablelogfiles;
			DROP TABLE IF EXISTS sys.manageddeltatableforks;
			DROP TABLE IF EXISTS sys.manageddeltatablecheckpoints;
		END

