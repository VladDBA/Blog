use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE   PROCEDURE sys.sp_remove_object_from_event_stream_group
	@stream_group_name SYSNAME,
	@object_name NVARCHAR(512)
AS
BEGIN
	EXEC [sys].[sp_remove_object_from_event_stream_group_internal] @stream_group_name, @object_name
END

