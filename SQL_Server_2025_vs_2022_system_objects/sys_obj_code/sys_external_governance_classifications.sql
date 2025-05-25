SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.external_governance_classifications AS
		SELECT
		convert(nvarchar(4000), sov2.value) COLLATE database_default AS classification,
		convert(uniqueidentifier, sov4.value) AS classification_id
	FROM sys.columns c
	LEFT JOIN sys.sysobjvalues sov2 ON c.object_id = sov2.objid AND c.column_id = sov2.subobjid and sov2.valclass = 171  -- SVC_EXTGOV_COL_SENSITIVTY_INFORMATION_TYPE
	LEFT JOIN sys.sysobjvalues sov4 ON c.object_id = sov4.objid AND c.column_id = sov4.subobjid and sov4.valclass = 173  -- SVC_EXTGOV_COL_SENSITIVTY_INFORMATION_TYPE_ID
	WHERE
		exists (select 1 from sys.dm_feature_switches where name = 'ExternalGovernanceAttributes' and is_enabled = 1)
		AND (sov2.value IS NOT NULL  OR sov4.value IS NOT NULL)
		AND (sov2.valnum = sov4.valnum)
        AND has_perms_by_name(NULL, 'DATABASE', 'VIEW ANY SENSITIVITY CLASSIFICATION') = 1
	GROUP BY sov2.value, sov4.value


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.external_governance_classifications AS
		SELECT
		convert(nvarchar(4000), sov2.value) COLLATE database_default AS classification,
		convert(uniqueidentifier, sov4.value) AS classification_id
	FROM sys.columns c
	LEFT JOIN sys.sysobjvalues sov2 ON c.object_id = sov2.objid AND c.column_id = sov2.subobjid and sov2.valclass = 171  -- SVC_EXTGOV_COL_SENSITIVTY_INFORMATION_TYPE
	LEFT JOIN sys.sysobjvalues sov4 ON c.object_id = sov4.objid AND c.column_id = sov4.subobjid and sov4.valclass = 173  -- SVC_EXTGOV_COL_SENSITIVTY_INFORMATION_TYPE_ID
	WHERE
		((SERVERPROPERTY('IsExternalGovernanceEnabled') IS NOT NULL)
			AND (SERVERPROPERTY('IsExternalGovernanceEnabled') = 1))
		AND (sov2.value IS NOT NULL  OR sov4.value IS NOT NULL)
        AND has_perms_by_name(NULL, 'DATABASE', 'VIEW ANY SENSITIVITY CLASSIFICATION') = 1
	GROUP BY sov2.value, sov4.value

