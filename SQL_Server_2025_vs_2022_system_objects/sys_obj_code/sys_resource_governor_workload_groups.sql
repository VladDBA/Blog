SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.resource_governor_workload_groups
AS
    -- ISSUE - VSTS 83011
    -- Use isnull() below because Algebrizer doesn't properly
    -- determine these columns are non-nullable.
    SELECT
        group_id                    = co.id, -- group id
        name                        = co.name,
        importance                  = isnull(convert(sysname,case
                                                     when ((co.status / 0x100) % 0x100) = 1 then 'Low'    -- x_ergi_Low
                                                     when ((co.status / 0x100) % 0x100) = 3 then 'Medium' -- x_ergi_Medium
                                                     when ((co.status / 0x100) % 0x100) = 9 then 'High'   -- x_ergi_High
                                                     end),
                                              0),
        request_max_memory_grant_percent = isnull(co.status % 0x100, 0),
        request_max_cpu_time_sec         = co.intprop,
        request_memory_grant_timeout_sec = isnull(convert(int, convert(bigint, ov.value) % 0x100000000), 0),
        max_dop                          = isnull(convert(int, convert(bigint, ov_maxdop.value) % 0x100000000), 0),
        group_max_requests               = isnull(convert(int, convert(bigint, ov.value) / 0x100000000), 0),
        pool_id                          = sor.indepid,
        external_pool_id                 = isnull(sorex.indepid, 2),
        request_max_memory_grant_percent_numeric   = isnull(convert(float, isnull(ov_memgrant.value, co.status % 0x100)), 0),
        -- In tempdb RG a value of -1 means that the configuration has not been set.
        group_max_tempdb_data_percent    = case when (convert(float, group_max_tempdb_data_percent) = -1) then null else convert(float, group_max_tempdb_data_percent) end,
        group_max_tempdb_data_mb         = case when (convert(float, group_max_tempdb_data_mb) = -1) then null else convert(float, group_max_tempdb_data_mb) end
    FROM
        master.sys.sysclsobjs co INNER JOIN
        master.sys.syssingleobjrefs sor ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = sor.depid AND
            sor.class = 13 AND  -- SRC_RG_GROUP_TO_POOL
            sor.depsubid = 0 AND
            sor.indepsubid = 0
        ) LEFT JOIN
        master.sys.syssingleobjrefs sorex ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = sorex.depid AND
            sorex.class = 120 AND  -- SRC_RG_GROUP_TO_EXTERNAL_POOL
            sorex.depsubid = 0 AND
            sorex.indepsubid = 0
        ) INNER JOIN
        master.sys.sysobjvalues ov ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = ov.objid AND
            ov.valclass = 64 AND -- SVC_RG_GROUP
            ov.subobjid = 0 AND
            ov.valnum = 0
        ) LEFT JOIN
        master.sys.sysobjvalues ov_maxdop ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = ov_maxdop.objid AND
            ov_maxdop.valclass = 64 AND -- SVC_RG_GROUP
            ov_maxdop.subobjid = 0 AND
            ov_maxdop.valnum = 1
        ) LEFT JOIN
        master.sys.sysobjvalues ov_memgrant ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = ov_memgrant.objid AND
            ov_memgrant.valclass = 64 AND -- SVC_RG_GROUP
            ov_memgrant.subobjid = 0 AND
            ov_memgrant.valnum = 3 -- Floating value of REQUEST_MAX_MEMORY_GRANT_PERCENT
        ) LEFT JOIN
        (
            SELECT
                objid,
                MAX(CASE WHEN valnum = 4 THEN value ELSE NULL END) AS group_max_tempdb_data_percent,
                MAX(CASE WHEN valnum = 5 THEN value ELSE NULL END) AS group_max_tempdb_data_mb
            FROM
                master.sys.sysobjvalues
            WHERE
                valclass = 64 AND -- SVC_RG_GROUP
                subobjid = 0 AND
                valnum IN (4, 5)
            GROUP BY
                objid
        ) ov_agg ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = ov_agg.objid
        )
    WHERE
        has_access('RG', 0) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.resource_governor_workload_groups
AS
    -- ISSUE - VSTS 83011
    -- Use isnull() below because Algebrizer doesn't properly
    -- determine these columns are non-nullable.
    SELECT
        group_id                    = co.id, -- group id
        name                        = co.name,
        importance                  = isnull(convert(sysname,case
                                                     when ((co.status / 0x100) % 0x100) = 1 then 'Low'    -- x_ergi_Low
                                                     when ((co.status / 0x100) % 0x100) = 3 then 'Medium' -- x_ergi_Medium
                                                     when ((co.status / 0x100) % 0x100) = 9 then 'High'   -- x_ergi_High
                                                     end),
                                              0),
        request_max_memory_grant_percent = isnull(co.status % 0x100, 0),
        request_max_cpu_time_sec         = co.intprop,
        request_memory_grant_timeout_sec = isnull(convert(int, convert(bigint, ov.value) % 0x100000000), 0),
        max_dop                          = isnull(convert(int, convert(bigint, ov_maxdop.value) % 0x100000000), 0),
        group_max_requests               = isnull(convert(int, convert(bigint, ov.value) / 0x100000000), 0),
        pool_id                          = sor.indepid,
        external_pool_id                 = isnull(sorex.indepid, 2),
        request_max_memory_grant_percent_numeric   = isnull(convert(float, isnull(ov_memgrant.value, co.status % 0x100)), 0)
    FROM
        master.sys.sysclsobjs co INNER JOIN
        master.sys.syssingleobjrefs sor ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = sor.depid AND
            sor.class = 13 AND  -- SRC_RG_GROUP_TO_POOL
            sor.depsubid = 0 AND
            sor.indepsubid = 0
        ) LEFT JOIN
        master.sys.syssingleobjrefs sorex ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = sorex.depid AND
            sorex.class = 120 AND  -- SRC_RG_GROUP_TO_EXTERNAL_POOL
            sorex.depsubid = 0 AND
            sorex.indepsubid = 0
        ) INNER JOIN
        master.sys.sysobjvalues ov ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = ov.objid AND
            ov.valclass = 64 AND -- SVC_RG_GROUP
            ov.subobjid = 0 AND
            ov.valnum = 0
        ) LEFT JOIN
        master.sys.sysobjvalues ov_maxdop ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = ov_maxdop.objid AND
            ov_maxdop.valclass = 64 AND -- SVC_RG_GROUP
            ov_maxdop.subobjid = 0 AND
            ov_maxdop.valnum = 1
        ) LEFT JOIN
        master.sys.sysobjvalues ov_memgrant ON
        (
            co.class = 53 AND   -- SOC_RG_GROUP
            co.id = ov_memgrant.objid AND
            ov_memgrant.valclass = 64 AND -- SVC_RG_GROUP
            ov_memgrant.subobjid = 0 AND
            ov_memgrant.valnum = 3 -- Floating value of REQUEST_MAX_MEMORY_GRANT_PERCENT
        )
    WHERE
        has_access('RG', 0) = 1

