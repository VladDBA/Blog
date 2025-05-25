use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROC sys.sp_fulltext_load_thesaurus
    @lcid int,
    @thesaurus XML 
AS
-- When FS is not enabled or EngineEdition is not Azure SQL DB raise error
IF (((SELECT 1 FROM sys.dm_feature_switches WHERE NAME = N'CloudThesaurusFunctionality' AND is_enabled = 1) is null ) OR
   (SERVERPROPERTY('EngineEdition') <> 5 ))
BEGIN
    DECLARE @procName sysname = N'sys.sp_fulltext_load_thesaurus';
    DECLARE @procNameLen int = datalength(@procName);
    RAISERROR(2813, 16, -1, @procNameLen, @procName)
    RETURN 1
END
BEGIN
    SET NOCOUNT ON
    SET IMPLICIT_TRANSACTIONS OFF

    -- sp_fulltext_load_thesaurus will run under read committed isolation level
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED

    -- CHECK PERMISSIONS (must be a dbowner) --
    if (is_member('db_owner') = 0)
    begin
        raiserror(15247,-1,-1)
        return 1
    end
    
	DECLARE @dbname     sysname = db_name()
    if (@dbname in ('master','tempdb','model','replicatedmaster'))
    begin
        raiserror(9966, -1, -1)
        return 1
    end

    -- Check whether another transaction is already running
    if @@trancount > 0
    begin
        raiserror(15002,-1,-1,'sys.sp_fulltext_load_thesaurus')
        return 1
    end

    BEGIN TRY

    BEGIN TRAN

    DECLARE @dbVersion int
    SELECT @dbVersion = convert(int,databasepropertyex(db_name(),'version'))

    DECLARE @diacritics_sensitive bit
    SELECT @diacritics_sensitive = 0
    SELECT @diacritics_sensitive = Thesaurus.d_s.value('.', 'bit') 
    FROM @thesaurus.nodes(N'declare namespace PD="x-schema:tsSchema.xml";/XML/PD:thesaurus/PD:diacritics_sensitive') AS Thesaurus(d_s)

    -- This takes a lock on the lcid row
    -- All codepaths accessing the state table, phrase table serialize on the lcid row in this table

    BEGIN TRY
    INSERT sys.fulltext_thesaurus_metadata_table VALUES(@lcid, @diacritics_sensitive)
    END TRY
    BEGIN CATCH
    DECLARE @error int
    SELECT @error = ERROR_NUMBER()
    IF (@error = 2601)
    BEGIN
            -- This means this is a user explicitly calling sp_fulltext_load_thesaurus and hence we should
            -- load the thesaurus file again
            -- Note that no code path deletes rows from this table, hence there is no race condition here
            -- 
        UPDATE sys.fulltext_thesaurus_metadata_table 
        SET diacritics_sensitive=@diacritics_sensitive
        WHERE lcid=@lcid
    END
    END CATCH

    -- deleting existing entries for this lcid from phrase table
    DELETE sys.fulltext_thesaurus_phrase_table 
    WHERE lcid = @lcid;

    -- Create temp table to store available wordbreaker versions
    -- During upgrade grace period, if FS 'FulltextIndexVersion2' enabled, available versions: (1), (2)
    -- After upgrade grace period, if FS 'FulltextIndexDisableVersion1" enabled, available versions: (2)
    CREATE TABLE #FtIndexVersions (versionNumber int)
    IF (((SELECT 1 FROM sys.dm_feature_switches WHERE NAME = N'FulltextIndexVersion2' AND is_enabled = 1) = 1) and @dbVersion >=988)
        INSERT INTO #FtIndexVersions (versionNumber) values (1), (2)
    ELSE
        INSERT INTO #FtIndexVersions (versionNumber) values (1)

    DECLARE @indexVersion int 
    DECLARE curVersion cursor local for
        SELECT versionNumber from #FtIndexVersions
    OPEN curVersion
    FETCH NEXT FROM curVersion into @indexVersion
    WHILE @@fetch_status=0
    BEGIN
        -- insert expansions and replacements
        -- Note the cast to 513 below. The max string we allow is 512 characters. If there is a phrase 
        -- longer than 512 in the file, it will get truncated to 513 length below but the word breaker fn will ex_raise
        -- it. If we make it 512 below, then the string will get silently truncated which we dont want to happen
        -- We can change to nvarchar(max) also below, but I am keeping it nvarchar(513) for perf reasons

        with xmlnamespaces (N'x-schema:tsSchema.xml' as PD)
        INSERT INTO sys.fulltext_thesaurus_phrase_table (groupid, isExpansion, isLHSOfReplacement, lcid, terms)
        SELECT X.rowid AS GroupId, 
            X.isexp AS IsExpansion, 
            Sub.Val.value('if (local-name(.) eq "pat") then 1 else 0', 'int') AS isLHSOfReplacement,
            @lcid,
            WordBrokenPhrase.concatenated_terms
        FROM
        (
           SELECT T2.exp.query('.'), 
            T2.exp.value('if (local-name(.) eq "expansion") then 1 else 0', 'int') isexp, 
            row_number() over (order by T3.DummyOrderingColumn) rowid
            FROM @thesaurus.nodes(N'(/XML/PD:thesaurus/PD:expansion, /XML/PD:thesaurus/PD:replacement)') AS T2(exp)
            -- this CROSS APPLY is needed since order by T2.exp is not a supported feature (even though it works)
            -- There is a light weight improvement that exposes ordpaths and when that gets done, one could potentially
            -- directly order by the ordpath above
            --
            CROSS APPLY (SELECT 1 AS DummyOrderingColumn) T3
        ) X(exprep, isexp, rowid)
        CROSS APPLY 
        X.exprep.nodes(N'(/PD:expansion/PD:sub, /PD:replacement/PD:pat, /PD:replacement/PD:sub)') AS Sub(Val)
        CROSS APPLY 
        sys.fn_ft_wordbreaker(@lcid, @diacritics_sensitive, Sub.Val.value('.', 'nvarchar(513)'), @indexVersion) AS WordBrokenPhrase

        IF (@dbVersion >= 988)
            UPDATE sys.fulltext_thesaurus_phrase_table set indexVersion = @indexVersion where indexVersion is null

        -- Update state table corresponding to phrase table
        --
        EXEC sys.sp_fulltext_userdb_thesaurus_update @lcid, @indexVersion

        FETCH NEXT FROM curVersion into @indexVersion
    END
    CLOSE curVersion
    DEALLOCATE curVersion

    -- We need to bump up the version of the thesaurus for this lcid --
    -- This will cause a recompile on any query using an older thesaurus version -- 
    --
    DBCC CALLFULLTEXT(23, 1, @lcid)

    COMMIT TRAN

    RETURN 0

    END TRY
    -- see if the transaction became uncommittable 
    BEGIN CATCH
    IF (XACT_STATE() <> 0)
    BEGIN
        ROLLBACK TRAN
    END

    DECLARE @errorNumber int
    EXEC @errorNumber=sys.sp_fulltext_rethrow_error
    RETURN @errorNumber
    END CATCH 
END

