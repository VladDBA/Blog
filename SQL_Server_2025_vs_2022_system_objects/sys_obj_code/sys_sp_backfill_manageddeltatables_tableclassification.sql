use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_backfill_manageddeltatables_tableclassification
	@default_classification_for_dropped_tables INT
AS
BEGIN
	BEGIN TRAN

	-- Allow only on Trident frontend instance.
	EXEC sys.sp_ensure_trident_frontend

	-- Don't return impacted row count.
	SET NOCOUNT ON

	-- Handle dropped tables first. If a table is dropped, we default to the passed in parameter since the logical metadata has been dropped, which makes a TSQL-based
	-- approach impossible.
	UPDATE sys.manageddeltatables SET table_classification = @default_classification_for_dropped_tables WHERE table_classification = 0 AND drop_commit_time > '1900-01-01 00:00:00.000'

	-- Handle internal tables, by checking to see if the table is in a reserved internal schema.
	UPDATE sys.manageddeltatables SET table_classification = 3 WHERE table_classification = 0 AND drop_commit_time <= '1900-01-01 00:00:00.000' AND sql_object_id in
	(
		SELECT
			object_id as sql_object_id
		FROM
			sys.tables
		WHERE
			 SCHEMA_NAME(schema_id) = '_rsc' OR SCHEMA_NAME(schema_id) = 'queryinsights'
	)

	-- Handle discovered tables, which are expected to have a hardcoded extended property called 'format_type' whose value is 'DELTA' (in various casing) or '1'.
	UPDATE sys.manageddeltatables SET table_classification = 2 WHERE drop_commit_time <= '1900-01-01 00:00:00.000' AND sql_object_id in
	(
		SELECT
			major_id as sql_object_id
		FROM
			sys.extended_properties
		WHERE
			class_desc = 'OBJECT_OR_COLUMN'
			AND name = 'format_type'
			AND (UPPER(TRY_CONVERT(NVARCHAR(100), VALUE)) = 'DELTA' OR value = '1')
	)

	-- Assume the rest of the unclassified tables are managed.
	UPDATE sys.manageddeltatables SET table_classification = 1 WHERE table_classification = 0 AND drop_commit_time <= '1900-01-01 00:00:00.000'

	COMMIT TRAN
END

