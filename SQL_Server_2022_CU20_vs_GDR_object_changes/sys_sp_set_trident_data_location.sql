use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2022 CU20 version  ====*/

CREATE PROCEDURE sys.sp_set_trident_data_location
@storagePath NVARCHAR(2000)
AS
	-- Allow only on Trident frontend instance
	EXEC sp_ensure_trident_frontend

	DECLARE @SQLString NVARCHAR(MAX)
	IF EXISTS(SELECT * FROM sys.extended_properties  where class = 0 and name= N'trident_database_data_storage_root_location')
	BEGIN
		RAISERROR('Trident data location is already set.',16,0)
	END
	ELSE
	BEGIN
		SET @SQLString = N'EXEC sys.sp_addextendedproperty N''trident_database_data_storage_root_location''' + N',' + N'N''' + @storagePath + N''''
		EXEC (@SQLString)
	END


/*====  SQL Server 2022 CU20 GDR version  ====*/

CREATE PROCEDURE sys.sp_set_trident_data_location
@storagePath NVARCHAR(2000)
AS
	RAISERROR(15817,16,9)

