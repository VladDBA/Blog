use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_tableoption
    @TableNamePattern      nvarchar(776)
   ,@OptionName            varchar(35)
   ,@OptionValue           varchar(12)
as
	-- declare AND INIT VARIABLES
	declare @OPTbulklock	varchar(25)
			,@OPTtextinrow	varchar(25)
			,@OPTpintable		varchar(25)
			,@OPLargeValOutOfRow varchar(28)
			,@OPUseVarDecimal varchar(25)
			,@CurrentDBId	int
			,@ColId		int
			,@TabId		int
			,@opt_value int
			,@opt_flag	bit
			,@permit	bit	-- locked after permission check?
			,@ret		int

	select @OptionName = lower(@OptionName collate Latin1_General_CI_AS)
			,@OPTbulklock = 'table lock on bulk load'
			,@OPTtextinrow = 'text in row'
			,@OPTpintable	= 'pintable'
			,@OPLargeValOutOfRow = 'large value types out of row'
			,@OPUseVarDecimal = 'vardecimal storage format'
			,@permit = 1
			,@CurrentDBId = db_id()

    -- DISALLOW USER TRANSACTION (except for in 'text in row' and 'large value types out of row' ) --
	set nocount on
	set implicit_transactions off
	if (@@trancount > 0 AND @OptionName = @OPTbulklock)
	begin
		raiserror(15002,-1,-1,'sys.sp_tableoption')
		return @@error
	end

	-- VALIDATE OPTION VALUE
	select @opt_value =
		case when (lower(@OptionValue) in ('1' ,'on' ,'yes' ,'true')) then 1
			when (lower(@OptionValue) in ('0' ,'off' ,'no' ,'false')) then 0
			when (@OptionName = @OPTtextinrow AND ISNUMERIC (@OptionValue) <> 0)
			then convert (int, @OptionValue)
		end

	-- ERROR if INVALID OPTION NAME OR VALUE
	if @opt_value is null OR @OptionName is null OR
		(@OptionName NOT IN (@OPTbulklock, @OPTtextinrow, @OPLargeValOutOfRow, @OPTpintable, @OPUseVarDecimal))
	begin
		raiserror(15600,-1,-1, 'sys.sp_tableoption')
		return @@error
	end

	-- Return silently when option pintable is specifed. This functionality no longer exists. 
	if (@OptionName = @OPTpintable)
		return 0
	
	if (@OptionName = @OPTtextinrow)
	begin
		if (@opt_value != 0 and @opt_value != 1 and
			(@opt_value < 24 or @opt_value > 7000))
		begin	-- Invalid value
			raiserror (15112,-1,-1)
			return @@error
		end
	end

	-- vardecimal storage format is locked down in SQL Azure.
	if (@OptionName = @OPUseVarDecimal and serverproperty('EngineEdition') in (5,12))
	begin
		raiserror(40512,-1,-1,@OptionName)
		return @@error
	end

	BEGIN TRANSACTION

	-- VERIFY WE HAVE A USER-TABLE BY THIS NAME IN THE DATABASE
	select @TabId = object_id from sys.tables
		where object_id = object_id(@TableNamePattern, 'local')

	if not (@TabId is null)
	begin
		-- LOCK TABLE, CHECK STANDARD TABLE-DDL PERMISSIONS
		EXEC %%Object(MultiName = @TableNamePattern).LockMatchID(ID = @TabId, Exclusive = 1, BindInternal = 0)
		if @@error <> 0
			select @permit = 0, @TabId = null
	end

	if @TabId is null	-- Not found/permission deny
	begin
		COMMIT TRANSACTION
		raiserror(15388,-1,-1,@TableNamePattern)
		return @@error
	end

	-- HANDLE TEXT-IN-ROW option
	if (@OptionName = @OPTtextinrow)
	begin
		-- invalidate inrow text pointer for the table
		--
		dbcc invalidate_textptr_objid(@TabId)
		dbcc no_textptr(@TabId, @opt_value)
	end

	-- HANDLE TABLOCK-ON-BCP option
	else if (@OptionName = @OPTbulklock)
	begin
		-- Make required change
		if ObjectProperty(@TabId, 'TableIsLockedOnBulkLoad') <> @opt_value
		begin
			-- SetLockOnBulkLoad expect bit value
			select @opt_flag = @opt_value
			EXEC %%Relation(ID = @TabId).SetLockOnBulkLoad(Value = @opt_flag)
		end
	end
	else if (@OptionName = @OPLargeValOutOfRow)
	begin
		-- SetLargeValuesTypeOutOfRow expects bit value
		select @opt_flag = @opt_value
		EXEC %%Relation(ID = @TabId).SetLargeValuesTypeOutOfRow(Value = @opt_flag)
		select @ret = @@error
		if @ret <> 0
		begin
			COMMIT TRANSACTION
			return @ret
		end
	end
	else if (@OptionName = @OPUseVarDecimal)
	begin
		-- SetUseVarDecimal expects bit value.
		select @opt_flag = convert(bit, @opt_value)
		
		-- No-op if the property is already in the desired state.
		if ObjectProperty(@TabId, 'TableHasVarDecimalStorageFormat') <> @opt_flag
		begin
			exec %%Relation(ID = @TabId).SetUseVarDecimal(Value = @opt_flag)
			select @ret = @@error
			if @ret <> 0
			begin
				COMMIT TRANSACTION
				return @ret
			end
		end
	end

	-- EMDEventType(x_eet_AlterTable), EMDUniversalClass( x_eunc_Table), src major id, src minor id, src name
	-- -1 means ignore target stuff, target major id, target minor id, target name,
	-- # of parameters, 5 parameters
	EXEC %%System().FireTrigger(ID = 22, ID = 1, ID = @TabId, ID = 0, Value = NULL,
			ID = -1, ID = 0, ID = 0, Value = NULL, 
			ID = 3, Value = @TableNamePattern, Value = @OptionName, Value = @OptionValue, Value = NULL, Value = NULL, Value = NULL, Value = NULL)

	COMMIT TRANSACTION

	-- return success
	return 0  --sp_tableoption


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_tableoption
    @TableNamePattern      nvarchar(776)
   ,@OptionName            varchar(35)
   ,@OptionValue           varchar(12)
as
	-- declare AND INIT VARIABLES
	declare @OPTbulklock	varchar(25)
			,@OPTtextinrow	varchar(25)
			,@OPTpintable		varchar(25)
			,@OPLargeValOutOfRow varchar(28)
			,@OPUseVarDecimal varchar(25)
			,@CurrentDBId	int
			,@ColId		int
			,@TabId		int
			,@opt_value int
			,@opt_flag	bit
			,@permit	bit	-- locked after permission check?
			,@ret		int

	select @OptionName = lower(@OptionName collate Latin1_General_CI_AS)
			,@OPTbulklock = 'table lock on bulk load'
			,@OPTtextinrow = 'text in row'
			,@OPTpintable	= 'pintable'
			,@OPLargeValOutOfRow = 'large value types out of row'
			,@OPUseVarDecimal = 'vardecimal storage format'
			,@permit = 1
			,@CurrentDBId = db_id()

    -- DISALLOW USER TRANSACTION (except for in 'text in row' and 'large value types out of row' ) --
	set nocount on
	set implicit_transactions off
	if (@@trancount > 0 AND @OptionName = @OPTbulklock)
	begin
		raiserror(15002,-1,-1,'sys.sp_tableoption')
		return @@error
	end

	-- VALIDATE OPTION VALUE
	select @opt_value =
		case when (lower(@OptionValue) in ('1' ,'on' ,'yes' ,'true')) then 1
			when (lower(@OptionValue) in ('0' ,'off' ,'no' ,'false')) then 0
			when (@OptionName = @OPTtextinrow AND ISNUMERIC (@OptionValue) <> 0)
			then convert (int, @OptionValue)
		end

	-- ERROR if INVALID OPTION NAME OR VALUE
	if @opt_value is null OR @OptionName is null OR
		(@OptionName NOT IN (@OPTbulklock, @OPTtextinrow, @OPLargeValOutOfRow, @OPTpintable, @OPUseVarDecimal))
	begin
		raiserror(15600,-1,-1, 'sys.sp_tableoption')
		return @@error
	end

	-- Return silently when option pintable is specifed. This functionality no longer exists. 
	if (@OptionName = @OPTpintable)
		return 0
	
	if (@OptionName = @OPTtextinrow)
	begin
		if (@opt_value != 0 and @opt_value != 1 and
			(@opt_value < 24 or @opt_value > 7000))
		begin	-- Invalid value
			raiserror (15112,-1,-1)
			return @@error
		end
	end

	-- vardecimal storage format is locked down in SQL Azure.
	if (@OptionName = @OPUseVarDecimal and serverproperty('EngineEdition') = 5)
	begin
		raiserror(40512,-1,-1,@OptionName)
		return @@error
	end

	BEGIN TRANSACTION

	-- VERIFY WE HAVE A USER-TABLE BY THIS NAME IN THE DATABASE
	select @TabId = object_id from sys.tables
		where object_id = object_id(@TableNamePattern, 'local')

	if not (@TabId is null)
	begin
		-- LOCK TABLE, CHECK STANDARD TABLE-DDL PERMISSIONS
		EXEC %%Object(MultiName = @TableNamePattern).LockMatchID(ID = @TabId, Exclusive = 1, BindInternal = 0)
		if @@error <> 0
			select @permit = 0, @TabId = null
	end

	if @TabId is null	-- Not found/permission deny
	begin
		COMMIT TRANSACTION
		raiserror(15388,-1,-1,@TableNamePattern)
		return @@error
	end

	-- HANDLE TEXT-IN-ROW option
	if (@OptionName = @OPTtextinrow)
	begin
		-- invalidate inrow text pointer for the table
		--
		dbcc invalidate_textptr_objid(@TabId)
		dbcc no_textptr(@TabId, @opt_value)
	end

	-- HANDLE TABLOCK-ON-BCP option
	else if (@OptionName = @OPTbulklock)
	begin
		-- Make required change
		if ObjectProperty(@TabId, 'TableIsLockedOnBulkLoad') <> @opt_value
		begin
			-- SetLockOnBulkLoad expect bit value
			select @opt_flag = @opt_value
			EXEC %%Relation(ID = @TabId).SetLockOnBulkLoad(Value = @opt_flag)
		end
	end
	else if (@OptionName = @OPLargeValOutOfRow)
	begin
		-- SetLargeValuesTypeOutOfRow expects bit value
		select @opt_flag = @opt_value
		EXEC %%Relation(ID = @TabId).SetLargeValuesTypeOutOfRow(Value = @opt_flag)
		select @ret = @@error
		if @ret <> 0
		begin
			COMMIT TRANSACTION
			return @ret
		end
	end
	else if (@OptionName = @OPUseVarDecimal)
	begin
		-- SetUseVarDecimal expects bit value.
		select @opt_flag = convert(bit, @opt_value)
		
		-- No-op if the property is already in the desired state.
		if ObjectProperty(@TabId, 'TableHasVarDecimalStorageFormat') <> @opt_flag
		begin
			exec %%Relation(ID = @TabId).SetUseVarDecimal(Value = @opt_flag)
			select @ret = @@error
			if @ret <> 0
			begin
				COMMIT TRANSACTION
				return @ret
			end
		end
	end

	-- EMDEventType(x_eet_AlterTable), EMDUniversalClass( x_eunc_Table), src major id, src minor id, src name
	-- -1 means ignore target stuff, target major id, target minor id, target name,
	-- # of parameters, 5 parameters
	EXEC %%System().FireTrigger(ID = 22, ID = 1, ID = @TabId, ID = 0, Value = NULL,
			ID = -1, ID = 0, ID = 0, Value = NULL, 
			ID = 3, Value = @TableNamePattern, Value = @OptionName, Value = @OptionValue, Value = NULL, Value = NULL, Value = NULL, Value = NULL)

	COMMIT TRANSACTION

	-- return success
	return 0  --sp_tableoption

