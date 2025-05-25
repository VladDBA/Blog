use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.external_models AS
	SELECT 
		co.id AS external_model_id,
		convert(sysname, ov5.value) collate catalog_default AS name,
		sor.indepid AS principal_id,
		convert(nvarchar(4000), ov1.value) collate catalog_default AS location,
		convert(nvarchar(100), ov2.value) collate catalog_default AS api_format,
		convert(int, pv.value) AS model_type_id,
		convert(nvarchar(65), ov3.value) collate catalog_default AS model_type_desc,
		convert(nvarchar(100), ov4.value) collate catalog_default AS model,
		sor1.indepid as credential_id,
		convert(json, convert(nvarchar(max), ov6.value)) AS parameters,
		convert(datetime2, co.created) AS create_time,
		convert(datetime2, co.modified) as modify_time
	FROM
		sys.sysclsobjs co LEFT JOIN
		sys.sysobjvalues ov1 ON
		(
			co.id = ov1.objid AND
			ov1.valclass = 182 AND -- SVC_EXTERNAL_MODEL
			ov1.valnum = 2 -- MODEL LOCATION
		) LEFT JOIN
		sys.sysobjvalues ov2 ON
		(
			co.id = ov2.objid AND
			ov2.valclass = 182 AND -- SVC_EXTERNAL_MODEL
			ov2.valnum = 3 AND -- API FORMAT
			ov2.subobjid = ov1.subobjid
		) LEFT JOIN
		sys.sysobjvalues ov3 ON
		(
			co.id = ov3.objid AND
			ov3.valclass = 182 AND -- SVC_EXTERNAL_MODEL
			ov3.valnum = 4 -- MODEL TYPE
		) LEFT JOIN
		sys.sysobjvalues ov4 ON
		(
			co.id = ov4.objid AND
			ov4.valclass = 182 AND -- SVC_EXTERNAL_MODEL
			ov4.valnum = 5 AND -- MODEL NAME
			ov4.subobjid = ov1.subobjid
		)
		LEFT JOIN
		sys.sysobjvalues ov5 ON
		(
			co.id = ov5.objid AND
			ov5.valclass = 182 AND -- SVC_EXTERNAL_MODEL
			ov5.valnum = 1 AND -- MODEL
			ov5.subobjid = ov1.subobjid
		)
		LEFT JOIN
		sys.sysobjvalues ov6 ON
		(
			co.id = ov6.objid AND
			ov6.valclass = 182 AND -- SVC_EXTERNAL_MODEL
			ov6.valnum = 7 AND -- MODEL PARAMETER
			ov6.subobjid = ov1.subobjid
		)
		LEFT JOIN
		sys.syssingleobjrefs sor ON
		(
			sor.depid = co.id AND sor.class = 137 -- SRC_EXTERNAL_MODEL_OWNER
		)
		LEFT JOIN
		sys.syssingleobjrefs sor1 ON
		(
			sor1.depid = co.id AND sor1.class = 138 -- SRC_EXTERNAL_MODEL_CREDENTIAL
		)
		LEFT JOIN
		sys.syspalvalues pv on
		(
			pv.class = 'EMT' AND
			pv.value = ov3.subobjid
		)
	WHERE
		co.class = 109 AND
		has_access('EM', co.id) = 1

