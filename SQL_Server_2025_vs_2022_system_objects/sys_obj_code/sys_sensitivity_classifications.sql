use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.sensitivity_classifications AS
	SELECT 
		class = 1,
		class_desc = 'OBJECT_OR_COLUMN',
		major_id = c.object_id,
		minor_id = c.column_id,
		convert(sysname, sov1.value) AS label,
		convert(sysname, sov3.value) AS label_id,
		convert(sysname, sov2.value) AS information_type,
		convert(sysname, sov4.value) AS information_type_id,
		CASE 
			WHEN sov5.value = -1 THEN NULL 
			ELSE convert(int, sov5.value)
		END AS rank,
		CASE 
			WHEN sov5.value =  0 THEN 'NONE'
			WHEN sov5.value = 10 THEN 'LOW'
			WHEN sov5.value = 20 THEN 'MEDIUM'
			WHEN sov5.value = 30 THEN 'HIGH'
			WHEN sov5.value = 40 THEN 'CRITICAL'
			ELSE NULL
		END AS rank_desc
	FROM sys.columns c
	LEFT JOIN sys.sysobjvalues sov1 ON c.object_id = sov1.objid AND c.column_id = sov1.subobjid and sov1.valclass = 138
	LEFT JOIN sys.sysobjvalues sov2 ON c.object_id = sov2.objid AND c.column_id = sov2.subobjid and sov2.valclass = 139
	LEFT JOIN sys.sysobjvalues sov3 ON c.object_id = sov3.objid AND c.column_id = sov3.subobjid and sov3.valclass = 140
	LEFT JOIN sys.sysobjvalues sov4 ON c.object_id = sov4.objid AND c.column_id = sov4.subobjid and sov4.valclass = 141
	LEFT JOIN sys.sysobjvalues sov5 ON c.object_id = sov5.objid AND c.column_id = sov5.subobjid and sov5.valclass = 146
	WHERE 
		(sov1.value IS NOT NULL OR sov2.value IS NOT NULL OR sov3.value IS NOT NULL OR sov4.value IS NOT NULL OR sov5.value IS NOT NULL)
        AND has_perms_by_name(NULL, 'DATABASE', 'VIEW ANY SENSITIVITY CLASSIFICATION') = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.sensitivity_classifications AS
	SELECT 
		class = 1,
		class_desc = 'OBJECT_OR_COLUMN',
		major_id = c.object_id,
		minor_id = c.column_id,
		convert(sysname, sov1.value) AS label,
		convert(sysname, sov3.value) AS label_id,
		convert(sysname, sov2.value) AS information_type,
		convert(sysname, sov4.value) AS information_type_id,
		CASE 
			WHEN sov5.value = -1 THEN NULL 
			ELSE convert(int, sov5.value)
		END AS rank,
		CASE 
			WHEN sov5.value =  0 THEN 'NONE'
			WHEN sov5.value = 10 THEN 'LOW'
			WHEN sov5.value = 20 THEN 'MEDIUM'
			WHEN sov5.value = 30 THEN 'HIGH'
			WHEN sov5.value = 40 THEN 'CRITICAL'
			ELSE NULL
		END AS rank_desc
	FROM sys.columns c
	LEFT JOIN sys.sysobjvalues sov1 ON c.object_id = sov1.objid AND c.column_id = sov1.subobjid and sov1.valclass = 138
	LEFT JOIN sys.sysobjvalues sov2 ON c.object_id = sov2.objid AND c.column_id = sov2.subobjid and sov2.valclass = 139
	LEFT JOIN sys.sysobjvalues sov3 ON c.object_id = sov3.objid AND c.column_id = sov3.subobjid and sov3.valclass = 140
	LEFT JOIN sys.sysobjvalues sov4 ON c.object_id = sov4.objid AND c.column_id = sov4.subobjid and sov4.valclass = 141
	LEFT JOIN sys.sysobjvalues sov5 ON c.object_id = sov5.objid AND c.column_id = sov5.subobjid and sov5.valclass = 146
	WHERE 
		((SERVERPROPERTY('IsExternalGovernanceEnabled') IS NULL)
			OR (SERVERPROPERTY('IsExternalGovernanceEnabled') <> 1))
		AND (sov1.value IS NOT NULL OR sov2.value IS NOT NULL OR sov3.value IS NOT NULL OR sov4.value IS NOT NULL OR sov5.value IS NOT NULL)
        AND has_perms_by_name(NULL, 'DATABASE', 'VIEW ANY SENSITIVITY CLASSIFICATION') = 1

