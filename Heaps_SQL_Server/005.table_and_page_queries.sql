/*get clustered index info*/
SELECT OBJECT_NAME([i].[object_id]) AS [table_name],
       [ips].[index_type_desc],
       [ips].[alloc_unit_type_desc],
       [ips].[record_count],
       [ips].[forwarded_record_count],
       [ips].[avg_fragmentation_in_percent]
FROM   sys.[indexes] AS [i]
       CROSS APPLY sys.dm_db_index_physical_stats (DB_ID(), [i].[object_id], NULL, NULL, 'DETAILED') AS [ips]
WHERE  [ips].[index_id] = 1 AND [ips].[index_level] = 0; 

/*find heaps with forwarded records*/
SELECT OBJECT_NAME([i].[object_id]) AS [table_name],
       [ips].[index_type_desc],
       [ips].[alloc_unit_type_desc],
       [ips].[record_count],
       [ips].[forwarded_record_count],
       [ips].[avg_fragmentation_in_percent]
FROM   sys.[indexes] AS [i]
       CROSS APPLY sys.dm_db_index_physical_stats (DB_ID(), [i].[object_id], NULL, NULL, 'DETAILED') AS [ips]
WHERE  [i].[index_id] = 0
       AND ([ips].[forwarded_record_count] IS NOT NULL
          OR [ips].[forwarded_record_count] > 0); 

/*get first 10 data pages for active_heap and active_cluster*/
SELECT TOP(10) DB_NAME([p_info].[database_id])   AS [database_name],
       OBJECT_NAME([p_info].[object_id]) AS [table_name],
       [p_info].[page_id],
       [p_info].[page_type_desc],
       [p_info].[free_bytes]
FROM   sys.dm_db_database_page_allocations(db_id(), object_id('active_heap'), NULL, NULL, 'DETAILED') [p_alloc]
       CROSS APPLY sys.dm_db_page_info([p_alloc].[database_id], [p_alloc].[allocated_page_file_id], [p_alloc].[allocated_page_page_id], 'DETAILED') AS [p_info]
WHERE  [p_alloc].[page_type_desc] = N'DATA_PAGE'
UNION 
SELECT TOP(10) DB_NAME([p_info].[database_id])   AS [database_name],
       OBJECT_NAME([p_info].[object_id]) AS [table_name],
       [p_info].[page_id],
       [p_info].[page_type_desc],
       [p_info].[free_bytes]
FROM   sys.dm_db_database_page_allocations(db_id(), object_id('active_cluster'), NULL, NULL, 'DETAILED') [p_alloc]
       CROSS APPLY sys.dm_db_page_info([p_alloc].[database_id], [p_alloc].[allocated_page_file_id], [p_alloc].[allocated_page_page_id], 'DETAILED') AS [p_info]
WHERE  [p_alloc].[page_type_desc] = N'DATA_PAGE'
ORDER BY [table_name] DESC, [page_id] ASC;

/*poke at a data page in the heap*/
DBCC PAGE ('oh_my_heap'/*db name*/,1 /*db file id*/,
111400 /*page id*/, 3 /*info level*/)
WITH TABLERESULTS;


/*poke at multiple data pages in the clustered index*/
CREATE TABLE #dbcc_results
  (
     [id]            INT NOT NULL IDENTITY(1, 1),
     [page_id]       INT,
     [parent_object] NVARCHAR(MAX),
     [object]       NVARCHAR(MAX),
     [field]       NVARCHAR(MAX),
     [value]       NVARCHAR(MAX)
  );
GO
INSERT INTO #dbcc_results([parent_object],[object],[field],[value])
EXEC('DBCC PAGE (''oh_my_heap'',1 ,111400, 3) WITH TABLERESULTS');
UPDATE #dbcc_results SET [page_id] = 111400 WHERE [page_id] IS NULL;
GO
INSERT INTO #dbcc_results([parent_object],[object],[field],[value])
EXEC('DBCC PAGE (''oh_my_heap'',1 ,111401, 3) WITH TABLERESULTS');
UPDATE #dbcc_results SET [page_id] = 111401 WHERE [page_id] IS NULL;
GO
INSERT INTO #dbcc_results([parent_object],[object],[field],[value])
EXEC('DBCC PAGE (''oh_my_heap'',1 ,111402, 3) WITH TABLERESULTS');
UPDATE #dbcc_results SET [page_id] = 111402 WHERE [page_id] IS NULL;
GO

SELECT * FROM #dbcc_results 
WHERE [object] LIKE N'Slot % Column % Offset %'
OR [value] LIKE N'FORWARD%'
ORDER BY [id];