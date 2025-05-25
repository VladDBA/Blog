use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure [sys].[sp_help_change_feed_settings]
as
begin
	declare @stmt nvarchar(max),
		@db_name sysname,
		@metadata_schema_name nvarchar(10),
		@is_allow_helper_sprocs_for_SL_enabled int,
		@is_trident_link bit,
		@check_to_use_internal_tables_for_ces bit
	set @db_name = db_name()
	set @is_trident_link = sys.fn_trident_link_is_enabled_for_current_db()
	exec sys.sp_change_streams_check_to_use_internal_tables @check_to_use_internal_tables_for_ces output

	-- Return if Change Feed is not enabled.
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 1, @db_name)
		return 1
	end
	
	exec @is_allow_helper_sprocs_for_SL_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkAllowNewHelperSprocsForChangeFeed'
	if (@is_allow_helper_sprocs_for_SL_enabled = 1)
	begin
		-- Get the metadata table schema name based on the feature enabled. Changefeed for Synapse Link and Change Event Streaming, sys for Trident link.
		exec sys.sp_get_metadata_schema_name @metadata_schema_name OUTPUT
		if(@metadata_schema_name = 'changefeed')
		begin
			if (@is_trident_link = 1)
			begin
				select * from changefeed.change_feed_settings
			end
			else
			begin
				select maxtrans, seqno, schema_version, pollinterval from changefeed.change_feed_settings
			end
			return 0
		end
	end

	if (@is_trident_link = 1)
	begin
		declare @use_box_msi int
		exec @use_box_msi = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkUseBoxMSI'
		if @use_box_msi = 1 and object_id('sys.dm_os_host_info') is not null
		begin
			select @use_box_msi = 0 from sys.dm_os_host_info where host_platform = N'Linux' collate SQL_Latin1_General_CP1_CI_AS
		end
		if @use_box_msi <> 1
		begin
			select * from sys.change_feed_settings
		end
		else
		begin
			declare @autoreseed nvarchar(11)
			declare @autoreseedthreshold nvarchar(11)

			EXEC sys.xp_instance_regread	@rootkey = N'HKEY_LOCAL_MACHINE',
											@key = N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer\Trident',
											@value_name = 'TridentOneLakeLinkPublishingAutoReseed',
											@value = @autoreseed output,
											@no_output = N'no_output'

			EXEC sys.xp_instance_regread	@rootkey = N'HKEY_LOCAL_MACHINE',
											@key = N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer\Trident',
											@value_name = 'TridentLinkAutoReseedLogThresholdPct',
											@value = @autoreseedthreshold output,
											@no_output = N'no_output'

			select *, @autoreseed as autoreseed, @autoreseedthreshold as autoreseedthreshold from sys.change_feed_settings
		end
	end
	else if (@check_to_use_internal_tables_for_ces = 1 and sys.fn_ces_is_enabled_for_current_db () = 1)
	begin
		select maxtrans, seqno, schema_version, pollinterval from sys.change_feed_settings
	end
	return 0
end

