SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure sys.sp_db_gb18030_unicode_collations
(
	@dbname sysname = null,
	@status varchar(6) = null
)
as
	return (0);


/*====  SQL Server 2022 version  ====*/

create procedure sys.sp_db_gb18030_unicode_collations
(
	@dbname sysname = null,
	@status varchar(6) = null
)
as
	-- If database is not specified then it is current database
	if (@dbname is null)
	begin
		set @dbname = DB_NAME();
	end

	-- If ON/OFF parameter is specified then it is an action call
	if @status is not null
	begin
		if (lower(@status) not in ('on', 'off', 'true', 'false'))
		begin
			raiserror (15231,16,1,'@GB18030UnicodeCollations','sp_db_gb18030_unicode_collations');
			return (1);
		end

		declare @fOnOff bit = 0;
		set @fOnOff = case lower(@status)
			when 'on' then 1
			when 'true' then 1
			else 0
			end;
		exec %%DatabaseRef(Name = @dbname).SetGB18030UnicodeCollations(GB18030UnicodeCollations = @fOnOff);
	end

	select cast(
			case databasepropertyex(name, 'version')
				when 957 then 0  -- SQL22_DISABLE_GB18030_UNICODE_COLLATIONS
				when 958 then 1  -- SQL22_ENABLE_GB18030_UNICODE_COLLATIONS
			end as bit) as Status
		 from sys.databases
		 where name = @dbname;

	return (0);

