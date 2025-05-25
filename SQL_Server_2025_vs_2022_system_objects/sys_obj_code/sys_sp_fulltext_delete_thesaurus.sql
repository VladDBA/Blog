SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE PROCEDURE sys.sp_fulltext_delete_thesaurus
        @lcid int = -1
AS
-- When FS is not enabled or EngineEdition is not Azure SQL DB raise error
IF (((SELECT 1 FROM sys.dm_feature_switches WHERE NAME = N'CloudThesaurusFunctionality' AND is_enabled = 1) IS NULL ) OR
    (SERVERPROPERTY('EngineEdition') <> 5 ))
BEGIN
    DECLARE @procName sysname = N'sys.sp_fulltext_delete_thesaurus';
    DECLARE @procNameLen int = datalength(@procName);
    RAISERROR(2813, 16, -1, @procNameLen, @procName)
    RETURN 1
END
BEGIN
    SET NOCOUNT ON
    SET IMPLICIT_TRANSACTIONS OFF

    -- sp_fulltext_delete_thesaurus will run under read committed isolation level
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
        raiserror(15002,-1,-1,'sys.sp_fulltext_delete_thesaurus')
        return 1
    end

    BEGIN TRY

    BEGIN TRAN
	
    -- delete existing entries from thesaurus internal tables
    -- 
    if (@lcid != -1)
    BEGIN 
        DELETE from sys.fulltext_thesaurus_state_table 
        WHERE lcid=@lcid

        DELETE from sys.fulltext_thesaurus_phrase_table 
        WHERE lcid=@lcid 

        DELETE from sys.fulltext_thesaurus_metadata_table 
        WHERE lcid=@lcid 
	
		-- We need to bump up the version of the thesaurus
		-- This will cause a recompile on any query using an older thesaurus version
		--
		DBCC CALLFULLTEXT(23, 1, @lcid)
    END

    ELSE
    BEGIN
		-- First bump version of thesaurus for all language ids
		-- This will cause a recompile on any query using an older thesaurus version
		--
		DECLARE @tempLcid INT

		DECLARE CurLcid CURSOR LOCAL
		FOR
			SELECT  lcid
			FROM    sys.fulltext_thesaurus_metadata_table

		OPEN CurLcid

		FETCH NEXT FROM CurLcid INTO @tempLcid

		WHILE @@FETCH_STATUS = 0
			BEGIN
				DBCC CALLFULLTEXT(23, 1, @tempLcid)
				
				FETCH NEXT FROM CurLcid INTO @tempLcid

			END

		CLOSE CurLcid
		DEALLOCATE CurLcid
		
		-- Then delete all content from thesaurus internal tables
		--
        DELETE from sys.fulltext_thesaurus_state_table

        DELETE from sys.fulltext_thesaurus_phrase_table 

        DELETE from sys.fulltext_thesaurus_metadata_table 
    END	

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

