use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.external_governance_classification_attributes AS
	SELECT
		object_id = o.object_id,
		type = o.type,
		type_desc = o.type_desc,
		convert(nvarchar(MAX), sov.imageval) AS object_attributes
	FROM sys.objects o
	LEFT JOIN sys.sysobjvalues sov ON o.object_id = sov.objid AND sov.valclass = 163 -- SVC_EXTGOV_ATTRSYNC
	WHERE
		exists (select 1 from sys.dm_feature_switches where name = 'ExternalGovernanceAttributes' and is_enabled = 1)
		AND o.type in ('U', 'V')
		AND has_perms_by_name(NULL, NULL, 'VIEW SERVER SECURITY STATE') = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.external_governance_classification_attributes AS
	SELECT
		object_id = o.object_id,
		type = o.type,
		type_desc = o.type_desc,
		convert(nvarchar(MAX), sov.imageval) AS object_attributes
	FROM sys.objects o
	LEFT JOIN sys.sysobjvalues sov ON o.object_id = sov.objid AND sov.valclass = 163 -- SVC_EXTGOV_ATTRSYNC
	WHERE
		((SERVERPROPERTY('IsExternalGovernanceEnabled') IS NOT NULL)
			AND (SERVERPROPERTY('IsExternalGovernanceEnabled') = 1))
		AND o.type in ('U', 'V')
		AND has_perms_by_name(NULL, NULL, 'VIEW SERVER SECURITY STATE') = 1

