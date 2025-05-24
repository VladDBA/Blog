SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.sysdatabase_principals_2021 AS
	SELECT u.name,
		u.id AS principal_id,
		u.type,
		n.name AS type_desc,
		u.dfltsch AS default_schema_name,
		u.created AS create_date,
		u.modified AS modify_date,
		r.indepid AS owning_principal_id,
		u.sid,
		sysconv(bit, case when u.id >= 16384 and u.id < 16400 then 1 else 0 end) AS is_fixed_role,
		u.authentication_type,
		aty.name AS authentication_type_desc,
		u.deflanguage as default_language_name,
		l.lcid as default_language_lcid,
		sysconv(bit, IIF(u.status & 0x20 = 0x20, 1, 0)) as allow_encrypted_value_modifications
	FROM (
		SELECT *,
			CASE 
				-- Guest is always authentication type none
				WHEN type = 'S' AND id = 2 THEN 0
				-- Users without password and without SQL SID prefix are instance auth
				WHEN type = 'S' AND password IS NULL AND cast(sid as binary(9)) <> 0x010500000000000903 THEN 1
				-- Users with passwords are database auth
				WHEN type = 'S' AND password IS NOT NULL THEN 2
				-- Windows users are always windows auth
				WHEN type = 'U' OR type = 'G' THEN 3
				-- External Users or External Groups
				WHEN type = 'E' OR type = 'X' THEN 4
				-- Users without passwords, certificate, key users, etc. have no authentication type
				ELSE 0
			END AS authentication_type
		FROM sys.sysowners) u
	LEFT JOIN sys.syssingleobjrefs r ON r.depid = u.id AND r.class = 51 AND r.depsubid = 0	-- SRC_ROLEOWNER
	LEFT JOIN sys.syspalnames n ON n.class = 'USTY' AND n.value = u.type
	LEFT JOIN sys.syspalnames aty ON aty.class = 'DBAT' AND aty.value = u.authentication_type
	LEFT JOIN sys.syslanguages l ON u.deflanguage COLLATE catalog_default = l.name COLLATE catalog_default
	WHERE u.type <> 'L'
		AND has_access('US', u.id) = 1	-- PR_ALIAS

