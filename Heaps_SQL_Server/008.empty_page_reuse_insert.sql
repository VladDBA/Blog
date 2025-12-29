/*drop the nonclustered index*/
DROP INDEX [ix_active_heap] ON [active_heap];
GO
/*
ALTER TABLE [active_heap] REBUILD WITH (MAXDOP=0, ONLINE=OFF);
GO
*/
GO
CHECKPOINT;
GO
DBCC DROPCLEANBUFFERS;
DBCC FREEPROCCACHE;
GO
SET NOCOUNT ON;
GO
/*insert 1mill records in the heap table*/
DECLARE @i       INT           = 0,
        @rows    INT           = 1000000,
        @string1 NVARCHAR(50)  = REPLICATE(N'a', 50),
        @string2 NVARCHAR(120) = REPLICATE(N'b', 120),
        @string3 NVARCHAR(200) = REPLICATE(N'c', 200),
        @start   DATETIME      = GETDATE(),
        @end     DATETIME;
WHILE @i < @rows
BEGIN
    SET @i +=1;
    INSERT INTO [active_heap] ([short_nvarchar],[medium_nvarchar],[long_nvarchar])
    VALUES (@string1,@string2,@string3);
    IF (@i % 20000 = 0 OR @i = @rows)
        BEGIN
            RAISERROR ('Processed %d rows',10,1,@i) WITH NOWAIT;
        END;
END;
SELECT @end = GETDATE();
SELECT 'active_heap' AS target_table, 
       CAST(DATEDIFF(MILLISECOND,@start,@end)/1000. AS DECIMAL(23,2)) AS total_duration_sec;
GO
CHECKPOINT;
GO
/*insert 1mill records in the clustered index*/
DECLARE @i       INT           = 0,
        @rows    INT           = 1000000,
        @string1 NVARCHAR(50)  = REPLICATE(N'a', 50),
        @string2 NVARCHAR(120) = REPLICATE(N'b', 120),
        @string3 NVARCHAR(200) = REPLICATE(N'c', 200),
        @start   DATETIME      = GETDATE(),
        @end     DATETIME;
WHILE @i < @rows
BEGIN
    SET @i +=1;
    INSERT INTO [active_cluster] ([short_nvarchar],[medium_nvarchar],[long_nvarchar])
    VALUES (@string1,@string2,@string3);
    IF (@i % 20000 = 0 OR @i = @rows)
        BEGIN
            RAISERROR ('Processed %d rows',10,1,@i) WITH NOWAIT;
        END;
END;
SELECT @end = GETDATE();
SELECT 'active_cluster' AS target_table, 
       CAST(DATEDIFF(MILLISECOND,@start,@end)/1000. AS DECIMAL(23,2)) AS total_duration_sec;
GO