SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_who2  --- 1995/11/03 10:16
	@loginame     sysname = NULL
as

set nocount on

declare
	@retcode         int

declare
	@sidlow         varbinary(85)
   ,@sidhigh        varbinary(85)
   ,@sid1           varbinary(85)
   ,@spidlow         int
   ,@spidhigh        int

declare
	@charMaxLenLoginName      varchar(6)
   ,@charMaxLenDBName         varchar(6)
   ,@charMaxLenCPUTime        varchar(10)
   ,@charMaxLenDiskIO         varchar(10)
   ,@charMaxLenHostName       varchar(10)
   ,@charMaxLenProgramName    varchar(10)
   ,@charMaxLenLastBatch      varchar(10)
   ,@charMaxLenCommand        varchar(10)

declare
    @charspidlow              varchar(11)
   ,@charspidhigh             varchar(11)

-- defaults

select @retcode         = 0      -- 0=good ,1=bad.
select @sidlow = convert(varbinary(85), (replicate(char(0), 85)))
select @sidhigh = convert(varbinary(85), (replicate(char(1), 85)))

select
	@spidlow         = 0
   ,@spidhigh        = 32767

--------------------------------------------------------------
IF (@loginame IS     NULL)  --Simple default to all LoginNames.
	  GOTO LABEL_17PARM1EDITED

-- select @sid1 = suser_sid(@loginame)
select @sid1 = null
if exists(select * from sys.syslogins where loginname = @loginame)
	select @sid1 = sid from sys.syslogins where loginname = @loginame

IF (@sid1 IS NOT NULL)  --Parm is a recognized login name.
   begin
   select @sidlow  = suser_sid(@loginame)
		 ,@sidhigh = suser_sid(@loginame)
   GOTO LABEL_17PARM1EDITED
   end

--------

IF (lower(@loginame collate Latin1_General_CI_AS) IN ('active'))  --Special action, not sleeping.
   begin
   select @loginame = lower(@loginame collate Latin1_General_CI_AS)
   GOTO LABEL_17PARM1EDITED
   end

--------

IF (patindex ('%[^0-9]%' , isnull(@loginame,'z')) = 0)  --Is a number.
   begin
   select
			 @spidlow   = convert(int, @loginame)
			,@spidhigh  = convert(int, @loginame)
   GOTO LABEL_17PARM1EDITED
   end

--------

raiserror(15007,-1,-1,@loginame)
select @retcode = 1
GOTO LABEL_86RETURN


LABEL_17PARM1EDITED:


--------------------  Capture consistent sysprocesses.  -------------------

select

  spid
 ,status
 ,sid
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
		as loginname
 ,spid as 'spid_sort'

 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
	   as 'last_batch_char'
 ,request_id

	  into    #tb1_sysprocesses
	  from sys.sysprocesses_ex with (nolock)

if @@error <> 0
	begin
		select @retcode = @@error
		GOTO LABEL_86RETURN
	end

--------Screen out any rows?

if (@loginame in ('active'))
   delete #tb1_sysprocesses
		 where   lower(status)  = 'sleeping'
		 and     upper(cmd)    in (
					 'AWAITING COMMAND'
					,'LAZY WRITER'
					,'CHECKPOINT SLEEP'
								  )

		 and     blocked       = 0



--------Prepare to dynamically optimize column widths.


select
    @charspidlow     = convert(varchar,@spidlow)
   ,@charspidhigh    = convert(varchar,@spidhigh)



select
			 @charMaxLenLoginName =
				  convert( varchar
						  ,isnull( max( datalength(loginname)) ,5)
						 )

			,@charMaxLenDBName    =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),db_name(dbid))))) ,6)
						 )

			,@charMaxLenCPUTime   =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),cpu)))) ,7)
						 )

			,@charMaxLenDiskIO    =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),physical_io)))) ,6)
						 )

			,@charMaxLenCommand  =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),cmd)))) ,7)
						 )

			,@charMaxLenHostName  =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),hostname)))) ,8)
						 )

			,@charMaxLenProgramName =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),program_name)))) ,11)
						 )

			,@charMaxLenLastBatch =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),last_batch_char)))) ,9)
						 )
	  from
			 #tb1_sysprocesses
	  where
			 spid >= @spidlow
	  and    spid <= @spidhigh



--------Output the report.


EXEC(
'
SET nocount off

SELECT
			 SPID          = convert(char(5),spid)

			,Status        =
				  CASE lower(status)
					 When ''sleeping'' Then lower(status)
					 Else                   upper(status)
				  END

			,Login         = substring(loginname,1,' + @charMaxLenLoginName + ')

			,HostName      =
				  CASE hostname
					 When Null  Then ''  .''
					 When '' '' Then ''  .''
					 Else    substring(hostname,1,' + @charMaxLenHostName + ')
				  END

			,BlkBy         =
				  CASE               isnull(convert(char(5),blocked),''0'')
					 When ''0'' Then ''  .''
					 Else            isnull(convert(char(5),blocked),''0'')
				  END

			,DBName        = substring(case when dbid = 0 then null when dbid <> 0 then db_name(dbid) end,1,' + @charMaxLenDBName + ')
			,Command       = substring(cmd,1,' + @charMaxLenCommand + ')

			,CPUTime       = substring(convert(varchar,cpu),1,' + @charMaxLenCPUTime + ')
			,DiskIO        = substring(convert(varchar,physical_io),1,' + @charMaxLenDiskIO + ')

			,LastBatch     = substring(last_batch_char,1,' + @charMaxLenLastBatch + ')

			,ProgramName   = substring(program_name,1,' + @charMaxLenProgramName + ')
			,SPID          = convert(char(5),spid)  --Handy extra for right-scrolling users.
			,REQUESTID       = convert(char(5),request_id)
	  from
			 #tb1_sysprocesses  --Usually DB qualification is needed in exec().
	  where
			 spid >= ' + @charspidlow  + '
	  and    spid <= ' + @charspidhigh + '

	  order by spid_sort

SET nocount on
'
)


LABEL_86RETURN:


if (object_id('tempdb..#tb1_sysprocesses') is not null)
			drop table #tb1_sysprocesses

return @retcode -- sp_who2


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_who2  --- 1995/11/03 10:16
	@loginame     sysname = NULL
as

set nocount on

declare
	@retcode         int

declare
	@sidlow         varbinary(85)
   ,@sidhigh        varbinary(85)
   ,@sid1           varbinary(85)
   ,@spidlow         int
   ,@spidhigh        int

declare
	@charMaxLenLoginName      varchar(6)
   ,@charMaxLenDBName         varchar(6)
   ,@charMaxLenCPUTime        varchar(10)
   ,@charMaxLenDiskIO         varchar(10)
   ,@charMaxLenHostName       varchar(10)
   ,@charMaxLenProgramName    varchar(10)
   ,@charMaxLenLastBatch      varchar(10)
   ,@charMaxLenCommand        varchar(10)

declare
	@charsidlow              varchar(85)
   ,@charsidhigh             varchar(85)
   ,@charspidlow              varchar(11)
   ,@charspidhigh             varchar(11)

-- defaults

select @retcode         = 0      -- 0=good ,1=bad.
select @sidlow = convert(varbinary(85), (replicate(char(0), 85)))
select @sidhigh = convert(varbinary(85), (replicate(char(1), 85)))

select
	@spidlow         = 0
   ,@spidhigh        = 32767

--------------------------------------------------------------
IF (@loginame IS     NULL)  --Simple default to all LoginNames.
	  GOTO LABEL_17PARM1EDITED

-- select @sid1 = suser_sid(@loginame)
select @sid1 = null
if exists(select * from sys.syslogins where loginname = @loginame)
	select @sid1 = sid from sys.syslogins where loginname = @loginame

IF (@sid1 IS NOT NULL)  --Parm is a recognized login name.
   begin
   select @sidlow  = suser_sid(@loginame)
		 ,@sidhigh = suser_sid(@loginame)
   GOTO LABEL_17PARM1EDITED
   end

--------

IF (lower(@loginame collate Latin1_General_CI_AS) IN ('active'))  --Special action, not sleeping.
   begin
   select @loginame = lower(@loginame collate Latin1_General_CI_AS)
   GOTO LABEL_17PARM1EDITED
   end

--------

IF (patindex ('%[^0-9]%' , isnull(@loginame,'z')) = 0)  --Is a number.
   begin
   select
			 @spidlow   = convert(int, @loginame)
			,@spidhigh  = convert(int, @loginame)
   GOTO LABEL_17PARM1EDITED
   end

--------

raiserror(15007,-1,-1,@loginame)
select @retcode = 1
GOTO LABEL_86RETURN


LABEL_17PARM1EDITED:


--------------------  Capture consistent sysprocesses.  -------------------

select

  spid
 ,status
 ,sid
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
		as loginname
 ,spid as 'spid_sort'

 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
	   as 'last_batch_char'
 ,request_id

	  into    #tb1_sysprocesses
	  from sys.sysprocesses_ex with (nolock)

if @@error <> 0
	begin
		select @retcode = @@error
		GOTO LABEL_86RETURN
	end

--------Screen out any rows?

if (@loginame in ('active'))
   delete #tb1_sysprocesses
		 where   lower(status)  = 'sleeping'
		 and     upper(cmd)    in (
					 'AWAITING COMMAND'
					,'LAZY WRITER'
					,'CHECKPOINT SLEEP'
								  )

		 and     blocked       = 0



--------Prepare to dynamically optimize column widths.


select
	@charsidlow     = convert(varchar(85),@sidlow)
   ,@charsidhigh    = convert(varchar(85),@sidhigh)
   ,@charspidlow     = convert(varchar,@spidlow)
   ,@charspidhigh    = convert(varchar,@spidhigh)



select
			 @charMaxLenLoginName =
				  convert( varchar
						  ,isnull( max( datalength(loginname)) ,5)
						 )

			,@charMaxLenDBName    =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),db_name(dbid))))) ,6)
						 )

			,@charMaxLenCPUTime   =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),cpu)))) ,7)
						 )

			,@charMaxLenDiskIO    =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),physical_io)))) ,6)
						 )

			,@charMaxLenCommand  =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),cmd)))) ,7)
						 )

			,@charMaxLenHostName  =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),hostname)))) ,8)
						 )

			,@charMaxLenProgramName =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),program_name)))) ,11)
						 )

			,@charMaxLenLastBatch =
				  convert( varchar
						  ,isnull( max( datalength( rtrim(convert(varchar(128),last_batch_char)))) ,9)
						 )
	  from
			 #tb1_sysprocesses
	  where
			 spid >= @spidlow
	  and    spid <= @spidhigh



--------Output the report.


EXEC(
'
SET nocount off

SELECT
			 SPID          = convert(char(5),spid)

			,Status        =
				  CASE lower(status)
					 When ''sleeping'' Then lower(status)
					 Else                   upper(status)
				  END

			,Login         = substring(loginname,1,' + @charMaxLenLoginName + ')

			,HostName      =
				  CASE hostname
					 When Null  Then ''  .''
					 When '' '' Then ''  .''
					 Else    substring(hostname,1,' + @charMaxLenHostName + ')
				  END

			,BlkBy         =
				  CASE               isnull(convert(char(5),blocked),''0'')
					 When ''0'' Then ''  .''
					 Else            isnull(convert(char(5),blocked),''0'')
				  END

			,DBName        = substring(case when dbid = 0 then null when dbid <> 0 then db_name(dbid) end,1,' + @charMaxLenDBName + ')
			,Command       = substring(cmd,1,' + @charMaxLenCommand + ')

			,CPUTime       = substring(convert(varchar,cpu),1,' + @charMaxLenCPUTime + ')
			,DiskIO        = substring(convert(varchar,physical_io),1,' + @charMaxLenDiskIO + ')

			,LastBatch     = substring(last_batch_char,1,' + @charMaxLenLastBatch + ')

			,ProgramName   = substring(program_name,1,' + @charMaxLenProgramName + ')
			,SPID          = convert(char(5),spid)  --Handy extra for right-scrolling users.
			,REQUESTID       = convert(char(5),request_id)
	  from
			 #tb1_sysprocesses  --Usually DB qualification is needed in exec().
	  where
			 spid >= ' + @charspidlow  + '
	  and    spid <= ' + @charspidhigh + '

	  order by spid_sort

SET nocount on
'
)


LABEL_86RETURN:


if (object_id('tempdb..#tb1_sysprocesses') is not null)
			drop table #tb1_sysprocesses

return @retcode -- sp_who2

