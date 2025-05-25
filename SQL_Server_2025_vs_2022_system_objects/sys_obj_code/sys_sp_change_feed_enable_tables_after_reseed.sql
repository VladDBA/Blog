SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure sys.sp_change_feed_enable_tables_after_reseed
as
begin
	set nocount on
	
	declare @table_group_id uniqueidentifier
			,@table_id uniqueidentifier

	declare @is_trident_publishing_table_reseed_enabled int
	exec @is_trident_publishing_table_reseed_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkPublishingTableReseed'

	declare @check_to_use_internal_tables bit
	-- Get the bit to determine if we need to use user tables or internal tables
	exec sys.sp_check_to_use_internal_tables @check_to_use_internal_tables OUTPUT

	if (@is_trident_publishing_table_reseed_enabled = 0)
	begin
		raiserror(22607, 16, 4, N'Table-Reseed')
		return 22607
	end

	declare @sal_return INT
	exec @sal_return =  sys.sp_trident_native_sal_raise_error_if_needed "sp_change_feed_enable_tables_after_reseed"
	if @sal_return <> 0
	BEGIN
		RETURN @sal_return
	END

	declare @db_name sysname = db_name()
	if (sys.fn_trident_link_is_enabled_for_current_db() <> 1)
	begin
		RAISERROR(22604, 16, 7, @db_name)
		RETURN (22604)
	end
	
	if (@check_to_use_internal_tables = 1)
	begin
		DECLARE #hC CURSOR FOR 
		SELECT table_group_id, table_id
		from [sys].[change_feed_tables]
		where state = 7
	end
	else
	begin
		DECLARE #hC CURSOR FOR 
		SELECT table_group_id, table_id
		from [changefeed].[change_feed_tables]
		where state = 7
	end

	OPEN #hC
	FETCH #hC into @table_group_id, @table_id
	WHILE (@@fetch_status <> -1)
	begin
		declare @retcode int
			,@logmessage nvarchar(4000)

		set @logmessage = concat(N'Executing internal proc. Table Group ID: ', @table_group_id, 
			N'. Table ID: ', @table_id)
		exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_enable_tables_after_reseed', @logmessage

		exec @retcode = sys.sp_change_feed_enable_table_after_reseed_internal @table_group_id = @table_group_id, @table_id = @table_id

		declare @errcode int = @@error
		declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end

		set @logmessage = concat(N'Completed internal proc. Table Group ID: ', @table_group_id, 
			N'. Table ID: ', @table_id,
			N'. Return code: ', @retcode,
			N'. Error code: ', @errcode)
		exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_enable_tables_after_reseed', @logmessage
		FETCH #hC into @table_group_id, @table_id
	end
	CLOSE #hC
	DEALLOCATE #hC

	return 0
end

