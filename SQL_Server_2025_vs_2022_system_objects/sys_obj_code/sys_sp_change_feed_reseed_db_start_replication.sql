SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure sys.sp_change_feed_reseed_db_start_replication 
as
begin
	declare 
		@db_name sysname,
		@retcode int,
		@logmessage nvarchar(4000)

	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_reseed_db_start_replication', N'Executing internal proc'
	exec @retcode = sys.sp_change_feed_reseed_db_start_replication_internal

	declare @errcode int = @@error
	declare @status int = iif(@errcode <> 0, @errcode, iif(@retcode <> 0, @retcode, 0))
	IF @status = 0
	BEGIN
		set @logmessage = concat(N'Completed internal proc. Return code: ', @retcode,
			N'. Error code: ', @errcode)
	END
	ELSE
	BEGIN
		set @logmessage = concat(N'Could not complete internal proc. Return code: ', @retcode,
				N'. Error code: ', @errcode)
	END
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_reseed_db_start_replication', @logmessage
	return @status
end

