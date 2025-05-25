use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure sys.sp_change_feed_reseed_db_init
(
	@is_init_needed	tinyint = 0,
	@is_called_from tinyint = 0
)
as
begin
	declare @retcode int
			,@logmessage nvarchar(4000)

	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_reseed_db_init', N'Executing internal proc'

	exec @retcode = sys.sp_change_feed_reseed_db_init_internal @is_init_needed

	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end
	
	set @logmessage = concat(N'Completed internal proc. Return code: ', @retcode,
		N'. Error code: ', @errcode)

	if (@is_init_needed = 1) 
	BEGIN
		set @logmessage = concat(
			@logmessage,
			N'. Reseed DB executed by ',
			case
				when @is_called_from = 0 then 'Default'
				when @is_called_from = 1 then 'SQL Publisher'
				when @is_called_from = 2 then 'CAS Action'
				else 'Unknown'
			end
		)
	END

	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_reseed_db_init', @logmessage
	
	return @retcode
end

