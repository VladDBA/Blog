SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE PROCEDURE sys.sp_detour_top_memory_allocations(
	@maxRecords INT = 10,
	@clerkType VARCHAR(256) = NULL,
	@clerkName VARCHAR(256) = NULL)
AS
BEGIN
	DECLARE @total_bytes BIGINT;
	DECLARE @line_no INT;
	DECLARE @source_file VARCHAR(1024);
	DECLARE @leaked_clerk_type VARCHAR(256);
	DECLARE @leaked_clerk_name VARCHAR(256);
	DECLARE @memory_object_address VARBINARY(8);
	DECLARE @leaked_page_allocator_address VARBINARY(8);
	DECLARE @hasClerkFilter BIT;
	SET @hasClerkFilter = 
	CASE
		WHEN (@clerkType is null or @clerkName is null) THEN 0 ELSE 1
	END
	DECLARE leak_cursor CURSOR FAST_FORWARD READ_ONLY FOR 
		select TOP (@maxRecords) L.*, C.name, C.type, C.page_allocator_address from sys.dm_os_memory_allocations_filtered L
			inner join sys.dm_os_memory_objects O ON L.memory_object_address = O.memory_object_address
			inner join sys.dm_os_memory_clerks C ON C.page_allocator_address = O.page_allocator_address 
		WHERE
			@hasClerkFilter = 0 OR (@hasClerkFilter = 1 AND C.name = @clerkName AND	C.type = @clerkType)
		ORDER BY sum_bytes DESC
	    ;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
	OPEN leak_cursor;
	FETCH NEXT FROM leak_cursor
		INTO  @memory_object_address, @total_bytes,@line_no, @source_file, @leaked_clerk_name, @leaked_clerk_type,  @leaked_page_allocator_address
		;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC sys.sp_process_detour_memory_allocation_record @memory_object_address, @total_bytes,@line_no, @source_file, @leaked_clerk_name, @leaked_clerk_type,  @leaked_page_allocator_address
		;
		FETCH NEXT FROM leak_cursor
			INTO @memory_object_address, @total_bytes,@line_no, @source_file, @leaked_clerk_name, @leaked_clerk_type,  @leaked_page_allocator_address
			;
	END
	CLOSE leak_cursor;
	DEALLOCATE leak_cursor;

END

