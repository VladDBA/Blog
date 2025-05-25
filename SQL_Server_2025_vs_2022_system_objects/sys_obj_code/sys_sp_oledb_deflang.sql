SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure sys.sp_oledb_deflang
as
    if serverproperty('EngineEdition') in (5,12)
    begin
        if db_name() <> N'master'
        begin
            raiserror(40634,-1,-1)
            return
        end
        
        EXEC('
        select
            ISNULL(default_language_name,''us_english'')
        from
            sys.sql_logins
        where
            sid=SUSER_SID()')
    end
    else
    begin
        EXEC('
        select
            ISNULL(language,''us_english'')
        from
           master..syslogins
        where
            sid=SUSER_SID()')
    end


/*====  SQL Server 2022 version  ====*/

create procedure sys.sp_oledb_deflang
as
    if serverproperty('EngineEdition') = 5
    begin
        if db_name() <> N'master'
        begin
            raiserror(40634,-1,-1)
            return
        end
        
        EXEC('
        select
            ISNULL(default_language_name,''us_english'')
        from
            sys.sql_logins
        where
            sid=SUSER_SID()')
    end
    else
    begin
        EXEC('
        select
            ISNULL(language,''us_english'')
        from
           master..syslogins
        where
            sid=SUSER_SID()')
    end

