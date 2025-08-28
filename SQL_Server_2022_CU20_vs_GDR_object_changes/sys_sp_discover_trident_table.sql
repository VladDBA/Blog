use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2022 CU20 version  ====*/

CREATE PROCEDURE sys.sp_discover_trident_table 
	@TableName NVARCHAR(256)
AS
-- Allow only on Trident frontend instance
EXEC sp_ensure_trident_frontend
DECLARE @SQLString NVARCHAR(MAX)
BEGIN
	IF OBJECT_ID (@TableName, N'U') IS NOT NULL 
		RAISERROR('Trident table already exists.', 16,0)
	ELSE
		IF OBJECT_ID (@TableName) IS NOT NULL 
			RAISERROR('Object with the same name already exists.', 16,0)
		ELSE
			BEGIN
				dbcc traceon(10070) 
				SET @SQLString ='CREATE TABLE '+ @TableName + ' AS SCHEMA_INFERRED_TABLE'
				EXEC (@SQLString)				
				dbcc traceoff(10070)
			END
END


/*====  SQL Server 2022 CU20 GDR version  ====*/

CREATE PROCEDURE sys.sp_discover_trident_table 
	@TableName NVARCHAR(256)
AS
	RAISERROR(15817,16,10)

