use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_get_top_internal_table
	@result NVARCHAR(776) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP 1 @result = QUOTENAME(DB_NAME()) + '.' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
	FROM sys.tables t
	JOIN sys.schemas s
		ON t.schema_id = s.schema_id
	WHERE t.type = 'U' AND
		(s.name = 'queryinsights' OR s.name = '_rsc')

	RETURN;
END

