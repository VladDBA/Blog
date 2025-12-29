/*delete data*/
SET NOCOUNT ON;
GO
DECLARE @batch_size INT = 1000,
        @deleted    INT;
WHILE 1=1 
    BEGIN
       DELETE TOP (@batch_size) FROM [active_cluster] WHERE [id] <> 1;
       DELETE TOP (@batch_size) FROM [active_heap] WHERE [id] <> 1;
       SET @deleted = @@ROWCOUNT;
          IF @deleted < @batch_size
            BEGIN
                BREAK;
            END;
    END;
GO
CHECKPOINT;
GO
SET NOCOUNT OFF;
GO
/*query remaining data*/
SET STATISTICS IO ON;
GO
SELECT * FROM [active_heap];
GO
SELECT * FROM [active_cluster];
GO