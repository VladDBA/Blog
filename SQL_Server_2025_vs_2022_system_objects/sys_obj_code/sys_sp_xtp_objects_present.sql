use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure sys.sp_xtp_objects_present
(
	@database_name sysname,
	@xtp_objects_present bit OUTPUT,
	@include_table_types bit = 1
)
as
	exec sys.sp_xtp_objects_present_internal @database_name, @xtp_objects_present OUTPUT, @include_table_types


/*====  SQL Server 2022 version  ====*/

create procedure sys.sp_xtp_objects_present
(
	@database_name sysname,
	@xtp_objects_present bit OUTPUT
)
as
	exec sys.sp_xtp_objects_present_internal @database_name, @xtp_objects_present OUTPUT

