SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_changeobjectowner
	@objname	nvarchar(776),		-- may be "[owner].[object]"
	@newowner	sysname				-- must be entry from sysusers
as
	Set nocount      on
	Set ansi_padding on
	declare	@objid		int,
			@newuid		int,
			@ret		int,
			@oldowner	sysname,
			@stmtS		nvarchar(4000),
			@objnameLen	int

	-- CHECK PERMISSIONS: Because changing owner changes both schema and
	--	permissions, the caller must be one of:
	-- (1) db_owner
	-- (2) db_ddladmin AND db_securityadmin
    if (is_member('db_owner') = 0) and
		(is_member('db_securityadmin') = 0 OR is_member('db_ddladmin') = 0)
    begin
		EXEC %%System().AuditEvent(ID = 1094864724, Success = 0, TargetLoginName = NULL, TargetUserName = @newowner, Role = NULL, Object = @objname, Provider = NULL, Server = NULL)
		raiserror(15247,-1,-1)
		return (1)
    end
    else
    begin
		EXEC %%System().AuditEvent(ID = 1094864724, Success = 1, TargetLoginName = NULL, TargetUserName = @newowner, Role = NULL, Object = @objname, Provider = NULL, Server = NULL)
    end

	if parsename(@objname, 1) is null
	begin
		raiserror(15253, -1, -1, @objname)
		return (1)
	end

	BEGIN TRANSACTION

	-- RESOLVE OBJECT NAME (CANNOT BE A CHILD OBJECT: TRIGGER/CONSTRAINT) --
	select @objid = object_id(@objname, 'local')
	if not (@objid is null)
	begin
		EXEC %%Object(MultiName = @objname).LockMatchID(ID = @objid, Exclusive = 1, BindInternal = 0)
		if (@@error <> 0)	-- lock failed
			select @objid = null
	end
	if (@objid is null) OR
		(select parent_obj from sysobjects where id = @objid) <> 0 OR
		ObjectProperty(@objid, 'IsSystemTable') = 1 OR
		parsename(@objname, 3) is not null OR
		parsename(@objname, 4) is not null OR
		exists (select * from sys.objects where object_id = @objid and schema_id in (3,4))  OR -- INFORMATION_SCHEMA, sys
		-- Check for Dependencies: No RENAME or CHANGEOWNER of OBJECT when exists:
		EXISTS (SELECT * FROM sysdepends d WHERE
			d.depid = @objid		-- A dependency on this object
			AND d.deptype > 0		-- that is enforced
			AND @objid <> d.id		-- that isn't a self-reference (self-references don't use object name)
			AND @objid <>			-- And isn't a reference from a child object (also don't use object name)
				(SELECT o.parent_obj FROM sysobjects o WHERE o.id = d.id)
			)
	begin
		-- OBJECT NOT FOUND
		COMMIT TRANSACTION
		select @objnameLen = datalength(@objname)
		raiserror(15001,-1,-1, @objnameLen, @objname)
		return 1
	end

	-- object's schema name must be the same as the schema owner's name
	if not exists (select so.name
		from sys.objects so
		join sys.schemas ss on so.schema_id = ss.schema_id
		join sys.database_principals su on ss.principal_id = su.principal_id
		where object_id = @objid and so.principal_id is null and ss.name = su.name)
	begin
		-- OBJECT NOT FOUND
		COMMIT TRANSACTION
		select @objnameLen = datalength(@objname)
		raiserror(15001,-1,-1, @objnameLen, @objname)
		return 1
	end

	select @oldowner = ssch.name from sys.schemas ssch join sys.objects so on (so.schema_id = ssch.schema_id) where object_id = @objid
	-- SHARE LOCK OLD SCHEMA, PREVENT DROP OF THE OWNER WHILE TXN ACTIVE --
		-- (rollback could cause phantom owner) --
	EXEC %%ObjectSchema (Name = @oldowner).Lock(Exclusive = 0) -- should succeed due to object lock above

	-- SHARE LOCK NEW SCHEMA --
	EXEC %%ObjectSchema (Name = @newowner).Lock(Exclusive = 0) -- may fail, check below anyway

	-- RESOLVE NEW OWNER NAME (ATTEMPT ADDING IMPLICIT ROW FOR NT NAME) --
    --  Disallow aliases, and public cannot own objects --
	if @@error = 0 -- lock success, indicate new owner may exist, verify further
		select @newuid = schema_id from sys.schemas where name = @newowner
							and schema_id not in (3,4) -- INFORMATION_SCHEMA, sys

    if @newuid is null -- indicate lock failed
    begin
		EXEC @ret = sys.sp_MSadduser_implicit_ntlogin @newowner
		if (@ret = 0) -- success
			select @newuid = schema_id from sys.schemas where name = @newowner
			-- Member locked by sp_MSadduser_implicit_ntlogin
    end

    if @newuid is null OR
		-- the schema name and its owner name must be the same
		not exists (select ss.name
			from sys.schemas ss
			join sys.database_principals su on ss.principal_id = su.principal_id
			where ss.name = @newowner and ss.name = su.name)
    begin
		-- Implicit login added above is not rolled back
		-- This is same as SQL 2000
		COMMIT TRANSACTION
		raiserror(15411, -1, -1, @newowner)
		return (1)
    end

	select @stmtS = 'ALTER SCHEMA '
	select @stmtS = @stmtS + quotename(@newowner)
	select @stmtS = @stmtS + ' TRANSFER '
	if parsename(@objname, 2) is not null
		select @stmtS = @stmtS + quotename(parsename(@objname, 2)) + '.'
	select @stmtS = @stmtS + quotename(parsename(@objname, 1))

	exec (@stmtS)
	IF @@ERROR <> 0
	BEGIN
		-- Nested transaction is used by alter schema statement
		COMMIT TRANSACTION
		return (1)
	END

	COMMIT TRANSACTION
	-- WARNING AFTER THE OWNER TRANSFER --
	raiserror(15477,-1,-1)
	return (0)	-- sp_changeobjectowner


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_changeobjectowner
	@objname	nvarchar(776),		-- may be "[owner].[object]"
	@newowner	sysname				-- must be entry from sysusers
as
	Set nocount      on
	Set ansi_padding on
	declare	@objid		int,
			@newuid		int,
			@ret		int,
			@oldowner	sysname,
			@stmtS		nvarchar(4000)

	-- CHECK PERMISSIONS: Because changing owner changes both schema and
	--	permissions, the caller must be one of:
	-- (1) db_owner
	-- (2) db_ddladmin AND db_securityadmin
    if (is_member('db_owner') = 0) and
		(is_member('db_securityadmin') = 0 OR is_member('db_ddladmin') = 0)
    begin
		EXEC %%System().AuditEvent(ID = 1094864724, Success = 0, TargetLoginName = NULL, TargetUserName = @newowner, Role = NULL, Object = @objname, Provider = NULL, Server = NULL)
		raiserror(15247,-1,-1)
		return (1)
    end
    else
    begin
		EXEC %%System().AuditEvent(ID = 1094864724, Success = 1, TargetLoginName = NULL, TargetUserName = @newowner, Role = NULL, Object = @objname, Provider = NULL, Server = NULL)
    end

	if parsename(@objname, 1) is null
	begin
		raiserror(15253, -1, -1, @objname)
		return (1)
	end

	BEGIN TRANSACTION

	-- RESOLVE OBJECT NAME (CANNOT BE A CHILD OBJECT: TRIGGER/CONSTRAINT) --
	select @objid = object_id(@objname, 'local')
	if not (@objid is null)
	begin
		EXEC %%Object(MultiName = @objname).LockMatchID(ID = @objid, Exclusive = 1, BindInternal = 0)
		if (@@error <> 0)	-- lock failed
			select @objid = null
	end
	if (@objid is null) OR
		(select parent_obj from sysobjects where id = @objid) <> 0 OR
		ObjectProperty(@objid, 'IsSystemTable') = 1 OR
		parsename(@objname, 3) is not null OR
		parsename(@objname, 4) is not null OR
		exists (select * from sys.objects where object_id = @objid and schema_id in (3,4))  OR -- INFORMATION_SCHEMA, sys
		-- Check for Dependencies: No RENAME or CHANGEOWNER of OBJECT when exists:
		EXISTS (SELECT * FROM sysdepends d WHERE
			d.depid = @objid		-- A dependency on this object
			AND d.deptype > 0		-- that is enforced
			AND @objid <> d.id		-- that isn't a self-reference (self-references don't use object name)
			AND @objid <>			-- And isn't a reference from a child object (also don't use object name)
				(SELECT o.parent_obj FROM sysobjects o WHERE o.id = d.id)
			)
	begin
		-- OBJECT NOT FOUND
		COMMIT TRANSACTION
		raiserror(15001,-1,-1,@objname)
		return 1
	end

	-- object's schema name must be the same as the schema owner's name
	if not exists (select so.name
		from sys.objects so
		join sys.schemas ss on so.schema_id = ss.schema_id
		join sys.database_principals su on ss.principal_id = su.principal_id
		where object_id = @objid and so.principal_id is null and ss.name = su.name)
	begin
		-- OBJECT NOT FOUND
		COMMIT TRANSACTION
		raiserror(15001,-1,-1,@objname)
		return 1
	end

	select @oldowner = ssch.name from sys.schemas ssch join sys.objects so on (so.schema_id = ssch.schema_id) where object_id = @objid
	-- SHARE LOCK OLD SCHEMA, PREVENT DROP OF THE OWNER WHILE TXN ACTIVE --
		-- (rollback could cause phantom owner) --
	EXEC %%ObjectSchema (Name = @oldowner).Lock(Exclusive = 0) -- should succeed due to object lock above

	-- SHARE LOCK NEW SCHEMA --
	EXEC %%ObjectSchema (Name = @newowner).Lock(Exclusive = 0) -- may fail, check below anyway

	-- RESOLVE NEW OWNER NAME (ATTEMPT ADDING IMPLICIT ROW FOR NT NAME) --
    --  Disallow aliases, and public cannot own objects --
	if @@error = 0 -- lock success, indicate new owner may exist, verify further
		select @newuid = schema_id from sys.schemas where name = @newowner
							and schema_id not in (3,4) -- INFORMATION_SCHEMA, sys

    if @newuid is null -- indicate lock failed
    begin
		EXEC @ret = sys.sp_MSadduser_implicit_ntlogin @newowner
		if (@ret = 0) -- success
			select @newuid = schema_id from sys.schemas where name = @newowner
			-- Member locked by sp_MSadduser_implicit_ntlogin
    end

    if @newuid is null OR
		-- the schema name and its owner name must be the same
		not exists (select ss.name
			from sys.schemas ss
			join sys.database_principals su on ss.principal_id = su.principal_id
			where ss.name = @newowner and ss.name = su.name)
    begin
		-- Implicit login added above is not rolled back
		-- This is same as SQL 2000
		COMMIT TRANSACTION
		raiserror(15411, -1, -1, @newowner)
		return (1)
    end

	select @stmtS = 'ALTER SCHEMA '
	select @stmtS = @stmtS + quotename(@newowner)
	select @stmtS = @stmtS + ' TRANSFER '
	if parsename(@objname, 2) is not null
		select @stmtS = @stmtS + quotename(parsename(@objname, 2)) + '.'
	select @stmtS = @stmtS + quotename(parsename(@objname, 1))

	exec (@stmtS)
	IF @@ERROR <> 0
	BEGIN
		-- Nested transaction is used by alter schema statement
		COMMIT TRANSACTION
		return (1)
	END

	COMMIT TRANSACTION
	-- WARNING AFTER THE OWNER TRANSFER --
	raiserror(15477,-1,-1)
	return (0)	-- sp_changeobjectowner

