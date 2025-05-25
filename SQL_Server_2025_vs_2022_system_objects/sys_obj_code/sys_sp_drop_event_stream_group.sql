use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE   PROCEDURE sys.sp_drop_event_stream_group
	@stream_group_name SYSNAME
AS
BEGIN
	EXEC [sys].[sp_drop_event_stream_group_internal] @stream_group_name
END

