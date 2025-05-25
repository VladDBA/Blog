use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.information_protection_label_mapping
as
SELECT
	class = 1, 
	class_desc = 'COLUMN', 
	major_id = c.object_id, 
	minor_id = c.column_id, 
	convert(uniqueidentifier, sov1.value) AS label_id 
FROM sys.columns c 
LEFT JOIN sys.sysobjvalues sov1 ON c.object_id = sov1.objid AND c.column_id = sov1.subobjid and sov1.valclass = 185     --SVC_COLUMN_INFORMATION_PROTECTION_LABEL
WHERE sov1.value IS NOT NULL

