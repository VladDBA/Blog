use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE PROC sys.sp_help_spatial_geography_histogram
(
	@tabname	SYSNAME,
	@colname	SYSNAME,
	@resolution	INT,
	@sample		FLOAT = 100
)
AS
BEGIN
	-- Check to see that the object names are local to the current database.
	DECLARE @dbname SYSNAME = parsename(@tabname,3)
	IF @dbname is null
		SELECT @dbname = db_name()
	ELSE IF @dbname <> db_name()
		BEGIN
			raiserror(15250,-1,-1)
			return (1)
		END

	-- Check to see if the TABLE exists
	DECLARE @objid int = object_id(@tabname);
	IF @objid is NULL
	BEGIN
		raiserror(15009,-1,-1,@tabname,@dbname)
		return (1)
	END

	declare @quoted_tabname nvarchar(max) = QUOTENAME(@dbname, N']') + N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(@objid), N']') + N'.' + QUOTENAME(OBJECT_NAME(@objid), N']');

	-- Check to see if the geography COLUMN exists
	DECLARE @columns INT = (select COUNT(*) from sys.columns where object_id = @objid and name = @colname and system_type_id = 240 and user_type_id = 130);
	IF @columns <> 1
	BEGIN
		raiserror(15148,-1,-1,@colname)
		return (1)
	END

	-- Check to see if the RESOLUTION is valid
	IF @resolution < 10
	BEGIN
		SET @resolution = 10;
	END
	IF @resolution > 5000
	BEGIN
		SET @resolution = 5000;
	END

	DECLARE @tablesample varchar(max) = N'';
	IF @sample <> 100
	BEGIN
		SET @tablesample = N'TABLESAMPLE (' + cast(@sample as nvarchar) + N' PERCENT)';
	END

	-- Run the query
	DECLARE @query nvarchar(max) = N'SELECT a.id AS CellId, geography::STGeomFromWKB(a.wkb, 4326) AS Cell, COUNT(*) AS IntersectionCount FROM ' + @quoted_tabname + N' ' + @tablesample +
	N' CROSS APPLY sys.GeodeticGridCoverage(' + QUOTENAME(@colname, N']') + N',' + cast(@resolution as nvarchar) + N',' + cast(@resolution as nvarchar) + N') a GROUP BY a.id, a.wkb';
	exec(@query);
END


/*====  SQL Server 2022 version  ====*/
CREATE PROC sys.sp_help_spatial_geography_histogram
(
	@tabname	SYSNAME,
	@colname	SYSNAME,
	@resolution	INT,
	@sample		FLOAT = 100
)
AS
BEGIN
	-- Check to see that the object names are local to the current database.
	DECLARE @dbname SYSNAME = parsename(@tabname,3)
	IF @dbname is null
		SELECT @dbname = db_name()
	ELSE IF @dbname <> db_name()
		BEGIN
			raiserror(15250,-1,-1)
			return (1)
		END

	-- Check to see if the TABLE exists
	DECLARE @objid int = object_id(@tabname);
	IF @objid is NULL
	BEGIN
		raiserror(15009,-1,-1,@tabname,@dbname)
		return (1)
	END

	declare @quoted_tabname nvarchar(max) = QUOTENAME(@dbname, N']') + N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(@objid), N']') + N'.' + QUOTENAME(OBJECT_NAME(@objid), N']');

	-- Check to see if the geography COLUMN exists
	DECLARE @columns INT = (select COUNT(*) from sys.columns where object_id = @objid and name = @colname and system_type_id = 240 and user_type_id = 130);
	IF @columns <> 1
	BEGIN
		raiserror(15148,-1,-1,@colname)
		return (1)
	END

	-- Check to see if the RESOLUTION is valid
	IF @resolution < 10
	BEGIN
		SET @resolution = 10;
	END
	IF @resolution > 5000
	BEGIN
		SET @resolution = 5000;
	END

	DECLARE @tablesample varchar(max) = N'';
	IF @sample <> 100
	BEGIN
		SET @tablesample = N'TABLESAMPLE (' + cast(@sample as nvarchar) + N' PERCENT)';
	END

	-- Run the query
	DECLARE @query nvarchar(max) = N'SELECT a.id AS CellId, geography::STGeomFromWKB(a.wkb, 4326) AS Cell, COUNT(*) AS IntersectionCount FROM ' + @quoted_tabname + N' ' + @tablesample +
	N' CROSS APPLY sys.GeodeticGridCoverage(' + @colname + N',' + cast(@resolution as nvarchar) + N',' + cast(@resolution as nvarchar) + N') a GROUP BY a.id, a.wkb';
	exec(@query);
END

