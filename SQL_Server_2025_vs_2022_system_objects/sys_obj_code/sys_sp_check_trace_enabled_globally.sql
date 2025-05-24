SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_check_trace_enabled_globally(@trace_number int, @trace_status bit output, @nomsgs bit = 0)
as
begin
	declare @tracetable
		table(trace_flag sysname,
			status bit,
			global bit,
			session bit)

	declare @cmd nvarchar(1000)

	set @trace_status = 0
	if (@nomsgs = 1)
	begin
		set nocount on
		set @cmd = N'DBCC TRACESTATUS (@trace_number) WITH NO_INFOMSGS'
	end
	else
	begin
		set @cmd = N'DBCC TRACESTATUS (@trace_number)'
	end

	insert into @tracetable exec sp_executesql @cmd, N'@trace_number int', @trace_number = @trace_number

	select @trace_status = global from @tracetable

	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_check_trace_enabled_globally(@trace_number int, @trace_status bit output, @nomsgs bit = 0)
as
begin
	declare @tracetable
		table(trace_flag sysname,
			status bit,
			global bit,
			session bit)

	declare @cmd nvarchar(1000)

	set @trace_status = 0
    if (@nomsgs = 1)
	begin
		set nocount on
		set @cmd = N'DBCC TRACESTATUS (@trace_number) WITH NO_INFOMSGS'
	end
	else
	begin
		set @cmd = N'DBCC TRACESTATUS (@trace_number)'
	end

	insert into @tracetable exec sp_executesql @cmd, N'@trace_number int', @trace_number = @trace_number

	select @trace_status = global from @tracetable

	return 0
end

