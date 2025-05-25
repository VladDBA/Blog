use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure sys.sp_xtp_slo_can_downgrade
(
	@database_name sysname,
	@xtp_can_downgrade bit OUTPUT,
	@allow_table_types bit = 0
)
as
	exec sys.sp_xtp_slo_downgrade_helper_internal @database_name, 0, 1, @xtp_can_downgrade OUTPUT, @allow_table_types


/*====  SQL Server 2022 version  ====*/

create procedure sys.sp_xtp_slo_can_downgrade
(
	@database_name sysname,
	@xtp_can_downgrade bit OUTPUT
)
as
	exec sys.sp_xtp_slo_downgrade_helper_internal @database_name, 0, 1, @xtp_can_downgrade OUTPUT

