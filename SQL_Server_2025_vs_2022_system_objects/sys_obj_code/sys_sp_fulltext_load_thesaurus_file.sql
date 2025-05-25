use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE PROC sys.sp_fulltext_load_thesaurus_file
    @lcid int,
    @loadOnlyIfNotLoaded bit = 0
AS
BEGIN
    SET NOCOUNT ON
    SET IMPLICIT_TRANSACTIONS OFF

    -- sp_fulltext_load_thesaurus_files will run under read committed isolation level
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED

    -- CHECK PERMISSIONS (must be serveradmin)
    if (is_srvrolemember('serveradmin') = 0)
    begin
        raiserror(15247,-1,-1)
        return 1
    end

    -- Disallow user transaction on this sp
    --
    if @@trancount > 0
    begin
        raiserror(15002,-1,-1,'sys.sp_fulltext_load_thesaurus_file')
        return 1
    end

    BEGIN TRY

    BEGIN TRAN

    DECLARE @thesaurusFilePath nvarchar(260)
    SELECT @thesaurusFilePath = NULL
    SELECT @thesaurusFilePath = thesaurus_file_path 
    FROM sys.fn_ft_thesaurus_files()
    WHERE lcid = @lcid

    -- raiserror if @filePath is NULL
    --
    IF (@thesaurusFilePath IS NULL)
    BEGIN
        RAISERROR(30050, 16, 1, @lcid) 
    END
 
    -- load the XML thesaurus file into an xml datatype variable, thereby ensuring that the XML is well formed
    -- Note: the XML is not validated against a schema, since there are issues with the Yukon XML files
    --
    DECLARE @thesaurus xml
    DECLARE @sqlString nvarchar(1024)
    SELECT @sqlString=N'SELECT @thesaurusOut=X.root FROM OPENROWSET(BULK N' + QUOTENAME(@thesaurusFilePath, '''') + N', SINGLE_BLOB) AS X(root)'
    EXECUTE sp_executesql @sqlString, N'@thesaurusOut xml OUTPUT', @thesaurusOut = @thesaurus OUTPUT;

    DECLARE @diacritics_sensitive bit
    SELECT @diacritics_sensitive = 0
    SELECT @diacritics_sensitive = Thesaurus.d_s.value('.', 'bit') 
    FROM @thesaurus.nodes(N'declare namespace PD="x-schema:tsSchema.xml";/XML/PD:thesaurus/PD:diacritics_sensitive') AS Thesaurus(d_s)

    -- This takes a lock on the lcid row
    -- All codepaths accessing the state table, phrase table serialize on the lcid row in this table
    BEGIN TRY
       INSERT tempdb.sys.fulltext_thesaurus_metadata_table VALUES(@lcid, @diacritics_sensitive)
    END TRY
    BEGIN CATCH
       DECLARE @error int
       SELECT @error = ERROR_NUMBER()
       IF (@error = 2601)
       BEGIN
          IF (@loadOnlyIfNotLoaded = 0)
          BEGIN
             -- This means this is a user explicitly calling sp_fulltext_load_thesaurus_file and hence we should
             -- load the thesaurus file again
             -- Note that no code path deletes rows from this table, hence there is no race condition here
             -- 
             UPDATE tempdb.sys.fulltext_thesaurus_metadata_table 
             SET diacritics_sensitive=@diacritics_sensitive
             WHERE lcid=@lcid
          END
          ELSE
          BEGIN
             COMMIT TRAN

             -- this means the engine is trying to load the thesaurus file as part of query
             -- and so we dont need to load the thesaurus file again
             RETURN 0
          END
       END
    END CATCH

    -- deleting existing entries for this lcid from phrase table
    --
    DELETE tempdb.sys.fulltext_thesaurus_phrase_table 
    WHERE lcid = @lcid;

    -- Create temp table to store available wordbreaker versions
    -- During upgrade grace period, if FS 'FulltextIndexVersion2' enabled, available versions: (1), (2)
    -- After upgrade grace period, if FS 'FulltextIndexDisableVersion1" enabled, available versions: (2)
    CREATE TABLE #FtIndexVersions (versionNumber int)
    IF ((SELECT 1 FROM sys.dm_feature_switches WHERE NAME = N'FulltextIndexVersion2' AND is_enabled = 1) IS NULL)
        INSERT INTO #FtIndexVersions (versionNumber) values (1)
    ELSE
        INSERT INTO #FtIndexVersions (versionNumber) values (1), (2)

    DECLARE @indexVersion int
    DECLARE crs_version cursor local for
        SELECT versionNumber from #FtIndexVersions

    OPEN crs_version
    FETCH NEXT FROM crs_version into @indexVersion
    WHILE @@fetch_status=0
    BEGIN
        -- insert expansions and replacements
        -- Note the cast to 513 below. The max string we allow is 512 characters. If there is a phrase 
        -- longer than 512 in the file, it will get truncated to 513 length below but the word breaker fn will ex_raise
        -- it. If we make it 512 below, then the string will get silently truncated which we dont want to happen
        -- We can change to nvarchar(max) also below, but I am keeping it nvarchar(513) for perf reasons
        --
        with xmlnamespaces (N'x-schema:tsSchema.xml' as PD)
        INSERT INTO tempdb.sys.fulltext_thesaurus_phrase_table (groupid, isExpansion, isLHSOfReplacement, lcid, terms, indexVersion)
        SELECT X.rowid AS GroupId, 
               X.isexp AS IsExpansion, 
               Sub.Val.value('if (local-name(.) eq "pat") then 1 else 0', 'int') AS isLHSOfReplacement,
               @lcid,
               WordBrokenPhrase.concatenated_terms,
               @indexVersion
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

            -- Update state table corresponding to phrase table
            --
        EXEC sys.sp_fulltext_thesaurus_update @lcid, @indexVersion
        FETCH NEXT FROM crs_version into @indexVersion
    END
    CLOSE crs_version
    DEALLOCATE crs_version

    -- We need to bump up the version of the thesaurus for this lcid --
    -- This will cause a recompile on any query using an older thesaurus version -- 
    DBCC CALLFULLTEXT(23, 1, @lcid)

    COMMIT TRAN

    RETURN 0

    END TRY
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


/*====  SQL Server 2022 version  ====*/
CREATE PROC sys.sp_fulltext_load_thesaurus_file
    @lcid int,
    @loadOnlyIfNotLoaded bit = 0
AS
BEGIN
    SET NOCOUNT ON
    SET IMPLICIT_TRANSACTIONS OFF

    -- sp_fulltext_load_thesaurus_files will run under read committed isolation level
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED

    -- CHECK PERMISSIONS (must be serveradmin)
    if (is_srvrolemember('serveradmin') = 0)
    begin
        raiserror(15247,-1,-1)
        return 1
    end

    -- Disallow user transaction on this sp
    --
    if @@trancount > 0
    begin
        raiserror(15002,-1,-1,'sys.sp_fulltext_load_thesaurus_file')
        return 1
    end

    BEGIN TRY

    BEGIN TRAN

    DECLARE @thesaurusFilePath nvarchar(260)
    SELECT @thesaurusFilePath = NULL
    SELECT @thesaurusFilePath = thesaurus_file_path 
    FROM sys.fn_ft_thesaurus_files()
    WHERE lcid = @lcid

    -- raiserror if @filePath is NULL
    --
    IF (@thesaurusFilePath IS NULL)
    BEGIN
        RAISERROR(30050, 16, 1, @lcid) 
    END
 
    -- load the XML thesaurus file into an xml datatype variable, thereby ensuring that the XML is well formed
    -- Note: the XML is not validated against a schema, since there are issues with the Yukon XML files
    --
    DECLARE @thesaurus xml
    DECLARE @sqlString nvarchar(1024)
    SELECT @sqlString=N'SELECT @thesaurusOut=X.root FROM OPENROWSET(BULK N' + QUOTENAME(@thesaurusFilePath, '''') + N', SINGLE_BLOB) AS X(root)'
    EXECUTE sp_executesql @sqlString, N'@thesaurusOut xml OUTPUT', @thesaurusOut = @thesaurus OUTPUT;

    DECLARE @diacritics_sensitive bit
    SELECT @diacritics_sensitive = 0
    SELECT @diacritics_sensitive = Thesaurus.d_s.value('.', 'bit') 
    FROM @thesaurus.nodes(N'declare namespace PD="x-schema:tsSchema.xml";/XML/PD:thesaurus/PD:diacritics_sensitive') AS Thesaurus(d_s)

    -- This takes a lock on the lcid row
    -- All codepaths accessing the state table, phrase table serialize on the lcid row in this table
    BEGIN TRY
       INSERT tempdb.sys.fulltext_thesaurus_metadata_table VALUES(@lcid, @diacritics_sensitive)
    END TRY
    BEGIN CATCH
       DECLARE @error int
       SELECT @error = ERROR_NUMBER()
       IF (@error = 2601)
       BEGIN
          IF (@loadOnlyIfNotLoaded = 0)
          BEGIN
             -- This means this is a user explicitly calling sp_fulltext_load_thesaurus_file and hence we should
             -- load the thesaurus file again
             -- Note that no code path deletes rows from this table, hence there is no race condition here
             -- 
             UPDATE tempdb.sys.fulltext_thesaurus_metadata_table 
             SET diacritics_sensitive=@diacritics_sensitive
             WHERE lcid=@lcid
          END
          ELSE
          BEGIN
             COMMIT TRAN

             -- this means the engine is trying to load the thesaurus file as part of query
             -- and so we dont need to load the thesaurus file again
             RETURN 0
          END
       END
    END CATCH

    -- deleting existing entries for this lcid from phrase table
    --
    DELETE tempdb.sys.fulltext_thesaurus_phrase_table 
    WHERE lcid = @lcid;

    -- insert expansions and replacements
    -- Note the cast to 513 below. The max string we allow is 512 characters. If there is a phrase 
    -- longer than 512 in the file, it will get truncated to 513 length below but the word breaker fn will ex_raise
    -- it. If we make it 512 below, then the string will get silently truncated which we dont want to happen
    -- We can change to nvarchar(max) also below, but I am keeping it nvarchar(513) for perf reasons
    --
    with xmlnamespaces (N'x-schema:tsSchema.xml' as PD)
    INSERT INTO tempdb.sys.fulltext_thesaurus_phrase_table (groupid, isExpansion, isLHSOfReplacement, lcid, terms)
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
    sys.fn_ft_wordbreaker(@lcid, @diacritics_sensitive, Sub.Val.value('.', 'nvarchar(513)')) AS WordBrokenPhrase

    -- Update state table corresponding to phrase table
    --
    EXEC sys.sp_fulltext_thesaurus_update @lcid

    -- We need to bump up the version of the thesaurus for this lcid --
    -- This will cause a recompile on any query using an older thesaurus version -- 
    DBCC CALLFULLTEXT(23, 1, @lcid)

    COMMIT TRAN

    RETURN 0

    END TRY
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

