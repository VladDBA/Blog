use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

CREATE VIEW sys.database_automatic_tuning_configurations AS
SELECT
    convert(nvarchar(60), o.[name]) AS [option],
    convert(nvarchar(60), v.[name]) AS [option_value],
    convert(nvarchar(60), t.[name]) AS [type],
    convert(sql_variant, query_id) AS [type_value],
    convert(nvarchar(4000), 'Query with query_id : ' + convert(nvarchar(50), query_id) + ' will be ignored by FORCE_LAST_GOOD_PLAN automatic tuning option')
      AS [details],
    1 AS [state]
    FROM sys.plan_persist_query
    LEFT JOIN sys.syspalnames o ON o.class = 'ATOP' AND o.[value] = 0
    LEFT JOIN sys.syspalnames t ON t.class = 'ATCT' AND t.[value] = 0
    LEFT JOIN sys.syspalvalues v ON v.class = 'ATCV' AND v.[value] = 0
WHERE query_flags & 0x01 <> 0

UNION ALL

SELECT
    convert(nvarchar(60), o.[name]) AS [option],
    convert(nvarchar(60), v.[name]) AS [option_value],
    convert(nvarchar(60), t.[name]) AS [type],
    convert(sql_variant, query_id) AS [type_value],
    convert(nvarchar(4000), 'Query with query_id : ' + convert(nvarchar(50), query_id) + ' will be sent to the extended check by FORCE_LAST_GOOD_PLAN automatic tuning option if it is enabled')
      AS [details],
    1 AS [state]
    FROM sys.plan_persist_query
    LEFT JOIN sys.syspalnames o ON o.class = 'ATOP' AND o.[value] = 1
    LEFT JOIN sys.syspalnames t ON t.class = 'ATCT' AND t.[value] = 0
    LEFT JOIN sys.syspalvalues v ON v.class = 'ATCV' AND v.[value] = 1
WHERE query_flags & 0x04 <> 0


/*====  SQL Server 2022 version  ====*/

CREATE VIEW sys.database_automatic_tuning_configurations AS
SELECT 
    convert(nvarchar(60), o.[name]) AS [option],
    convert(nvarchar(60), v.[name]) AS [option_value],
    convert(nvarchar(60), t.[name]) AS [type],
    convert(sql_variant, query_id) AS [type_value],
    convert(nvarchar(4000), 'Query with query_id : ' + convert(nvarchar(50), query_id) + ' will be ignored by FORCE_LAST_GOOD_PLAN automatic tuning option')
      AS [details],
    1 AS [state]
    FROM sys.plan_persist_query
    LEFT JOIN sys.syspalnames o ON o.class = 'ATOP' AND o.[value] = 0
    LEFT JOIN sys.syspalnames t ON t.class = 'ATCT' AND t.[value] = 0
    LEFT JOIN sys.syspalvalues v ON v.class = 'ATCV' AND v.[value] = 0
WHERE query_flags & 0x01 <> 0

UNION ALL

SELECT 
    convert(nvarchar(60), o.[name]) AS [option],
    convert(nvarchar(60), v.[name]) AS [option_value],
    convert(nvarchar(60), t.[name]) AS [type],
    convert(sql_variant, query_id) AS [type_value],
    convert(nvarchar(4000), 'Query with query_id : ' + convert(nvarchar(50), query_id) + ' will be sent to the extended check by FORCE_LAST_GOOD_PLAN automatic tuning option if it is enabled')
      AS [details],
    1 AS [state]
    FROM sys.plan_persist_query
    LEFT JOIN sys.syspalnames o ON o.class = 'ATOP' AND o.[value] = 1
    LEFT JOIN sys.syspalnames t ON t.class = 'ATCT' AND t.[value] = 0
    LEFT JOIN sys.syspalvalues v ON v.class = 'ATCV' AND v.[value] = 1
WHERE query_flags & 0x04 <> 0

