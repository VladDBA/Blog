use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.query_store_plan_forcing_locations AS 
	SELECT
	plan_forcing_location_id,
	query_id,
	plan_id,
	replica_group_id,
	timestamp,
	plan_forcing_type,
	p.name as plan_forcing_type_desc
	FROM (
    SELECT
        plan_forcing_location_id,
		query_id,
		plan_id,
		replica_group_id,
		timestamp,
        CASE WHEN convert(bit, plan_forcing_flags & 0x02) = 0 THEN 1
             WHEN convert(bit, plan_forcing_flags & 0x02) = 1 THEN 2
             ELSE 0
        END as plan_forcing_type
	FROM sys.plan_persist_plan_forcing_locations
	) AS subquery
LEFT JOIN sys.syspalvalues p ON p.class = 'PFT' AND p.value = subquery.plan_forcing_type;


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.query_store_plan_forcing_locations AS 
	SELECT
		plan_forcing_location_id,
		query_id,
		plan_id,
		replica_group_id
	FROM sys.plan_persist_plan_forcing_locations

