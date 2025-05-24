SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE PROC sys.sp_help_spatial_geometry_histogram
(
	@tabname	SYSNAME,
	@colname	SYSNAME,
	@resolution	INT,
	@xmin		FLOAT(53),
	@ymin		FLOAT(53),
	@xmax		FLOAT(53),
	@ymax		FLOAT(53),
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

	-- Check to see if the COLUMN exists
	DECLARE @columns INT = (select COUNT(*) from sys.columns where object_id = @objid and name = @colname and system_type_id = 240 and user_type_id = 129);
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

	-- Check to see if the BOUNDING BOX is valid
	DECLARE @bb_query nvarchar(max) = N'DECLARE @a int; SELECT @a = id FROM sys.PlanarGridCoverage(NULL, ' + convert(nvarchar, @xmin, 2) + N',' + convert(nvarchar, @ymin, 2) + N','
	+ convert(nvarchar, @xmax, 2) + N',' + convert(nvarchar, @ymax, 2) + N',' + cast(@resolution as nvarchar) + N',' + cast(@resolution as nvarchar) + N')';
	exec(@bb_query);

	DECLARE @tablesample nvarchar(max) = '';
	IF @sample <> 100
	BEGIN
		SET @tablesample = N'TABLESAMPLE (' + cast(@sample as nvarchar) + N' PERCENT)';
	END

	-- Run the query
	DECLARE @query nvarchar(max) = N'SELECT a.id AS CellId, geometry::STGeomFromWKB(a.wkb, 0) AS Cell, COUNT(*) AS IntersectionCount FROM ' + @quoted_tabname + N' ' + @tablesample +
	N' CROSS APPLY sys.PlanarGridCoverage(' + QUOTENAME(@colname, N']') + N',' + convert(nvarchar, @xmin, 2) + N',' + convert(nvarchar, @ymin, 2) + N',' + convert(nvarchar, @xmax, 2) + N',' + convert(nvarchar, @ymax, 2) +
	N',' + cast(@resolution as nvarchar) + N',' + cast(@resolution as nvarchar) + N') a GROUP BY a.id, a.wkb';
	exec(@query);
END


/*====  SQL Server 2022 version  ====*/
CREATE PROC sys.sp_help_spatial_geometry_histogram
(
	@tabname	SYSNAME,
	@colname	SYSNAME,
	@resolution	INT,
	@xmin		FLOAT(53),
	@ymin		FLOAT(53),
	@xmax		FLOAT(53),
	@ymax		FLOAT(53),
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

	-- Check to see if the COLUMN exists
	DECLARE @columns INT = (select COUNT(*) from sys.columns where object_id = @objid and name = @colname and system_type_id = 240 and user_type_id = 129);
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

	-- Check to see if the BOUNDING BOX is valid
	DECLARE @bb_query nvarchar(max) = N'DECLARE @a int; SELECT @a = id FROM sys.PlanarGridCoverage(NULL, ' + convert(nvarchar, @xmin, 2) + N',' + convert(nvarchar, @ymin, 2) + N','
	+ convert(nvarchar, @xmax, 2) + N',' + convert(nvarchar, @ymax, 2) + N',' + cast(@resolution as nvarchar) + N',' + cast(@resolution as nvarchar) + N')';
	exec(@bb_query);

	DECLARE @tablesample nvarchar(max) = '';
	IF @sample <> 100
	BEGIN
		SET @tablesample = N'TABLESAMPLE (' + cast(@sample as nvarchar) + N' PERCENT)';
	END

	-- Run the query
	DECLARE @query nvarchar(max) = N'SELECT a.id AS CellId, geometry::STGeomFromWKB(a.wkb, 0) AS Cell, COUNT(*) AS IntersectionCount FROM ' + @quoted_tabname + N' ' + @tablesample +
	N' CROSS APPLY sys.PlanarGridCoverage(' + @colname + N',' + convert(nvarchar, @xmin, 2) + N',' + convert(nvarchar, @ymin, 2) + N',' + convert(nvarchar, @xmax, 2) + N',' + convert(nvarchar, @ymax, 2) +
	N',' + cast(@resolution as nvarchar) + N',' + cast(@resolution as nvarchar) + N') a GROUP BY a.id, a.wkb';
	exec(@query);
END

