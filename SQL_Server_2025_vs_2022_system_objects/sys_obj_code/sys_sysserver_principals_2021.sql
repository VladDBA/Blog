SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.sysserver_principals_2021 AS
	SELECT p.name,
		p.id AS principal_id,
		p.sid, p.type,
		n.name AS type_desc,
		is_disabled = sysconv(bit, p.status & 0x80),
		p.crdate AS create_date,
		p.modate AS modify_date,
		p.dbname AS default_database_name,
		p.lang AS default_language_name,
		r.indepid AS credential_id,
		(case when p.id >= 2 and p.id < 21 then 1 else ro.indepid end) AS owning_principal_id,
		sysconv(bit, case when p.id >= 3 and p.id < 21 then 1 else 0 end) AS is_fixed_role
	FROM master.sys.sysxlgns p
	LEFT JOIN sys.syspalnames n ON n.class = 'LGTY' AND n.value = p.type
	LEFT JOIN sys.syssingleobjrefs r ON r.depid = p.id AND r.class = 63 AND r.depsubid = 0	-- SRC_LOGIN_CREDENTIAL
	LEFT JOIN sys.syssingleobjrefs ro ON ro.depid = p.id AND ro.class = 61 AND ro.depsubid = 0	-- SRC_SRVROLELOGINOWNER
	WHERE has_access('LG', p.id) = 1
		AND p.type <> 'M' -- exclude component logins

