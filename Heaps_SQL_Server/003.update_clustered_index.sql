SET NOCOUNT ON;
GO
DECLARE @i       INT           = 0,
        @rows    INT           = 1010100,
        @i2      INT,
        @string1 NVARCHAR(50)  = REPLICATE(N'a', 50),
        @string2 NVARCHAR(120) = REPLICATE(N'b', 120),
        @string3 NVARCHAR(200) = REPLICATE(N'c', 200),
        @start   DATETIME      = GETDATE(),
        @end     DATETIME;
WHILE @i < @rows+200
BEGIN
    SET @i +=1;
    UPDATE [active_cluster] 
    SET [short_nvarchar] = @string1 
    WHERE id = @i;
    IF (@i % 100 = 0)
         BEGIN
             INSERT INTO [active_cluster]([short_nvarchar]) VALUES (@string1);
         END;
    IF (@i > 100)
       BEGIN
           SET @i2 = @i - 100;
           UPDATE [active_cluster] 
           SET [medium_nvarchar] = @string2, 
               [long_nvarchar] = @string3 
           WHERE id = @i2;
       END;
    IF (@i > 200)
       BEGIN
           SET @i2 = @i - 200;
           UPDATE [active_cluster] 
           SET [medium_nvarchar] = [medium_nvarchar] + @string2,
               [long_nvarchar] = [long_nvarchar] + @string3 
           WHERE id = @i2;
       END;
    IF (@i % 20000 = 0 OR @i = @rows)
        BEGIN
            RAISERROR ('Processed %d rows',10,1,@i) WITH NOWAIT;
        END;
END;
SELECT @end = GETDATE();
SELECT 'active_cluster' AS target_table, 
       CAST(DATEDIFF(MILLISECOND,@start,@end)/1000. AS DECIMAL(23,2)) AS total_duration_sec;
GO