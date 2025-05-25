use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.external_governance_sensitivity_labels_mapping AS
		SELECT
		class = 1,
		class_desc = 'OBJECT_OR_COLUMN',
		major_id = c.object_id,
		minor_id = c.column_id,
		convert(uniqueidentifier, sov3.value) AS label_id
	FROM sys.columns c
	LEFT JOIN sys.sysobjvalues sov1 ON c.object_id = sov1.objid AND c.column_id = sov1.subobjid and sov1.valclass = 170  -- SVC_EXTGOV_COL_SENSITIVTY_LABEL
	LEFT JOIN sys.sysobjvalues sov3 ON c.object_id = sov3.objid AND c.column_id = sov3.subobjid and sov3.valclass = 172  -- SVC_EXTGOV_COL_SENSITIVTY_LABEL_ID
	WHERE
		exists (select 1 from sys.dm_feature_switches where name = 'ExternalGovernanceAttributes' and is_enabled = 1)
		AND (sov1.value IS NOT NULL OR sov3.value IS NOT NULL)
        AND has_perms_by_name(NULL, 'DATABASE', 'VIEW ANY SENSITIVITY CLASSIFICATION') = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.external_governance_sensitivity_labels_mapping AS
		SELECT
		class = 1,
		class_desc = 'OBJECT_OR_COLUMN',
		major_id = c.object_id,
		minor_id = c.column_id,
		convert(uniqueidentifier, sov3.value) AS label_id
	FROM sys.columns c
	LEFT JOIN sys.sysobjvalues sov1 ON c.object_id = sov1.objid AND c.column_id = sov1.subobjid and sov1.valclass = 170  -- SVC_EXTGOV_COL_SENSITIVTY_LABEL
	LEFT JOIN sys.sysobjvalues sov3 ON c.object_id = sov3.objid AND c.column_id = sov3.subobjid and sov3.valclass = 172  -- SVC_EXTGOV_COL_SENSITIVTY_LABEL_ID
	WHERE
		((SERVERPROPERTY('IsExternalGovernanceEnabled') IS NOT NULL)
			AND (SERVERPROPERTY('IsExternalGovernanceEnabled') = 1))
		AND (sov1.value IS NOT NULL OR sov3.value IS NOT NULL)
        AND has_perms_by_name(NULL, 'DATABASE', 'VIEW ANY SENSITIVITY CLASSIFICATION') = 1

