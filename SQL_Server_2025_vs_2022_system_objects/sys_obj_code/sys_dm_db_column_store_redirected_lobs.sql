use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
create function sys.dm_db_column_store_redirected_lobs
	(
	@DatabaseId		SMALLINT,
	@RowsetId		BIGINT
	)
returns table
as
	return select * from openrowset (table DM_DB_COLUMNSTORE_REDIRECTED_LOBS, @DatabaseId, @RowsetId) 

