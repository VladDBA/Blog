use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create function sys.fn_has_change_feed_permissions (@destination_type int = 2)
returns bit
as
begin
	if @destination_type in (0,1) -- Synapse Link
		begin
			return sys.fn_check_database_permission('CONTROL')
		end

	if @destination_type = 2 -- Fabric Link
		BEGIN
			if( sys.fn_check_database_permission('ALTER ANY EXTERNAL MIRROR') = 1 and
			sys.fn_check_database_permission('SELECT') = 1 and
			sys.fn_check_database_permission('VIEW DATABASE PERFORMANCE STATE') = 1 and
			sys.fn_check_database_permission('VIEW DATABASE SECURITY STATE') = 1)
			begin
				return 1
			end
		end
	ELSE
		begin
			return (0)
		end

	 -- Some undefined destination type value passed in. Return 0 in this case.
	return (0)
end

