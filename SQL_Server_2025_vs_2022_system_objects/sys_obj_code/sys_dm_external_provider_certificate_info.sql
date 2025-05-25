    /*Only the object_id was changed between versions, the code is the same*/
use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_external_provider_certificate_info AS
	SELECT * FROM OpenRowset(TABLE DM_EXTERNAL_PROVIDER_CERTIFICATE_INFO)


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_external_provider_certificate_info AS
	SELECT * FROM OpenRowset(TABLE DM_EXTERNAL_PROVIDER_CERTIFICATE_INFO)

