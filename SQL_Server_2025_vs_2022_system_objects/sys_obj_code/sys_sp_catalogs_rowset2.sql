use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure sys.sp_catalogs_rowset2
as
-------------------------------------------------------------------------------------------
-- copy & pasted from version 1 of the SProc and removed checks for 1st parameter !
-------------------------------------------------------------------------------------------
    select
        CATALOG_NAME    = name,
        DESCRIPTION     = convert(nvarchar(1),null)
    from
        sys.databases
    where
        has_dbaccess(name)=1 OR serverproperty('EngineEdition') in (5,12)
    order by 1


/*====  SQL Server 2022 version  ====*/

create procedure sys.sp_catalogs_rowset2
as
-------------------------------------------------------------------------------------------
-- copy & pasted from version 1 of the SProc and removed checks for 1st parameter !
-------------------------------------------------------------------------------------------
    select
        CATALOG_NAME    = name,
        DESCRIPTION     = convert(nvarchar(1),null)
    from
        sys.databases
    where
        has_dbaccess(name)=1 OR serverproperty('EngineEdition') = 5
    order by 1

