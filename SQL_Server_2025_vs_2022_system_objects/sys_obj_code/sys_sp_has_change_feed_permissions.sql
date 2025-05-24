SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create procedure sys.sp_has_change_feed_permissions
(
	@destination_type int = 2
)
as
begin
	-- Validate if destination_type is valid
	if (@destination_type is null or @destination_type not in (0,1,2))
	BEGIN
		raiserror(22740, 16, 4, N'@destination_type')
		RETURN(22740)
	END
	
	if @destination_type in (0, 1) -- Synapse Link
		begin
			if (sys.fn_has_change_feed_permissions(@destination_type) = 0)
				begin
					RAISERROR(22702, 16, 1, N'CONTROL')
					return (1)
				end
		end

		if @destination_type = 2 -- Fabric Link
			begin
				declare @AlterPerm int
				declare @SelectPerm int 
				declare @ViewSecStatePerm int 
				declare @ViewPerfStatePerm int 

				select @AlterPerm = sys.fn_check_database_permission('ALTER ANY EXTERNAL MIRROR')
				select @SelectPerm = sys.fn_check_database_permission('SELECT')
				select @ViewSecStatePerm = sys.fn_check_database_permission('VIEW DATABASE SECURITY STATE')
				select @ViewPerfStatePerm = sys.fn_check_database_permission('VIEW DATABASE PERFORMANCE STATE')

				if( @AlterPerm = 0 or
					@SelectPerm = 0 or
					@ViewSecStatePerm = 0 or
					@ViewPerfStatePerm = 0
				)
					begin
						RAISERROR(22702, 16, 1, N'ALTER ANY EXTERNAL MIRROR, SELECT, and VIEW DATABASE STATE')
						return (1)
					end
			end

	return 0
end

