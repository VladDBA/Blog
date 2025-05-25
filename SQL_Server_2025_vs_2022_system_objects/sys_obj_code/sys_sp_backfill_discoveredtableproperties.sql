use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;


CREATE PROCEDURE sys.sp_backfill_discoveredtableproperties
AS
BEGIN
	BEGIN TRAN

	-- Allow only on Trident frontend instance.
	EXEC sys.sp_ensure_trident_frontend

	-- Don't return impacted row count, as we don't process the output of this query.
	SET NOCOUNT ON

	-- Insert the discovered table properties modeled as extended properties into sys.discoveredtableproperties if no entry for that
	-- table already exists.
	INSERT INTO sys.discoveredtableproperties (table_id, discovered_object_id, discovered_object_version, format_type, last_applied_commit_id, partition_columns, use_schema_in_storage_path)
	SELECT 
		-- Use default values via COALESCE if conversion fails. This indicates that an invalid value was persisted for the property,
		-- and will be handled via query failure if the property is scanned.
		table_id AS table_id,
		COALESCE(TRY_CONVERT(UNIQUEIDENTIFIER, object_id), '00000000-0000-0000-0000-000000000000') AS discovered_object_id,
		COALESCE(TRY_CONVERT(BIGINT, object_version), -1) AS discovered_object_version,
		CONVERT(TINYINT, 1) AS format_type,
		COALESCE(TRY_CONVERT(BIGINT, last_applied_commit_id), -1) AS last_applied_commit_id,
		COALESCE(TRY_CONVERT(NVARCHAR(4000), partition_columns), '') AS partition_columns,
		COALESCE(TRY_CONVERT(BIT, use_schema_in_storage_path), 'false') AS use_schema_in_storage_path
	FROM 
		(SELECT mdt.table_id, ep.name, ep.value
			FROM 
				sys.manageddeltatables AS mdt
			INNER JOIN 
				sys.extended_properties AS ep
					ON ep.major_id = mdt.sql_object_id
					AND ep.class = 1 -- Filter down to object or column.
					AND ep.minor_id = 0 -- Filter down to object only.
			INNER JOIN
				sys.tables AS obj
					ON obj.object_id = ep.major_id
		WHERE 
			NOT EXISTS (SELECT 1 from sys.discoveredtableproperties dtp WHERE dtp.table_id = mdt.table_id)
			AND SCHEMA_NAME(obj.schema_id) NOT IN ('_rsc', 'queryinsights') -- Skip internal schemas.
		GROUP BY  mdt.table_id, ep.name, ep.value) AS JoinedExtendedPropertiesTable
	PIVOT
		(MAX(value) FOR name IN (object_id, object_version, last_applied_commit_id, partition_columns, use_schema_in_storage_path, format_type)) AS ExtendedPropertiesPivotTable
	WHERE UPPER(TRY_CONVERT(NVARCHAR(100), format_type)) = 'DELTA' or format_type = '1' -- Filter out tables with extended properties that are not discovered tables by using the flag and casing we use in engine code to identify discovered tables.
	ORDER BY 
		table_id

	-- If the discovered table already exists in sys.discoveredtableproperties, update the last applied commit ID if the extended property is larger
	-- than the internal table value. This indicates a rollback occurred, and we need to catch up to a previous backfill. In these cases, last applied
	-- commit ID is the only property that would have been potentially updated. All others are provided only on table creation.
	UPDATE sys.discoveredtableproperties
	SET sys.discoveredtableproperties.last_applied_commit_id = TRY_CONVERT(BIGINT, sys.extended_properties.value)
	FROM sys.discoveredtableproperties
		INNER JOIN sys.manageddeltatables ON sys.discoveredtableproperties.table_id = sys.manageddeltatables.table_id
		INNER JOIN sys.extended_properties ON sys.manageddeltatables.sql_object_id = sys.extended_properties.major_id AND sys.extended_properties.name = 'last_applied_commit_id'
	WHERE sys.discoveredtableproperties.last_applied_commit_id < TRY_CONVERT(BIGINT, sys.extended_properties.value);

	COMMIT TRAN
END

