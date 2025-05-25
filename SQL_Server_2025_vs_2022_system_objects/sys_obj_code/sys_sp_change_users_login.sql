use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_change_users_login
    @Action               varchar(10)       -- REPORT / UPDATE_ONE / AUTO_FIX
   ,@UserNamePattern      sysname  = Null
   ,@LoginName            sysname  = Null
   ,@Password             sysname  = Null
AS
    -- SETUP RUNTIME OPTIONS / DECLARE VARIABLES --
	set nocount on
	declare @exec_stmt nvarchar(4000)

	declare @ret            int,
            @FixMode        char(5),
            @cfixesupdate   int,        -- count of fixes by update
            @cfixesaddlogin int,        -- count of fixes by sp_addlogin
            @dbname         sysname,
            @loginsid       varbinary(85),
            @110name        sysname,
            @ActionIn       varchar(10),
            @sanitizedPwd sysname, -- We use this to pass on the password parameter down to SetSID
			@ExistingUserName sysname

    -- SET INITIAL VALUES --
    select  @dbname         = db_name(),
            @cfixesupdate   = 0,
            @cfixesaddlogin = 0,
            @ActionIn = @Action,
            @Action = UPPER(@Action collate Latin1_General_CI_AS)

    -- set the sanitized password to NULL - we do not want to give any information about this parameter in eventdata
    set @sanitizedPwd = NULL


    -- INVALIDATE USE OF SPECIAL LOGIN/USER NAMES --
    if suser_sid(@LoginName) = 0x1 -- 'sa'
    begin
        raiserror(15287,-1,-1,@LoginName)
        return (1)
    end
    if database_principal_id(@UserNamePattern) in (0,1,2,3,4) --public, dbo, guest, INFORMATION_SCHEMA, sys
    begin
        raiserror(15287,-1,-1,@UserNamePattern)
        return (1)
    end

    -- HANDLE REPORT --
    if @Action = 'REPORT'
    begin

        -- CHECK PERMISSIONS --
        if not is_member('db_owner') = 1
        begin
		raiserror(15247,-1,-1)
            	return (1)
        end

        -- VALIDATE PARAMS --
        if @UserNamePattern IS NOT Null or @LoginName IS NOT Null or @Password IS NOT Null
        begin
            raiserror(15600,-1,-1,'sys.sp_change_users_login')
            return (1)
        end

        -- GENERATE REPORT --
        select UserName = name, UserSID = sid from sysusers
            where issqluser = 1
            and   (sid is not null and sid <> 0x0)
            and   (datalength(sid) <= 16)
            and   suser_sname(sid) is null
            order by name
        return (0)
    end

   -- ERROR IF IN USER TRANSACTION --
    if @@trancount > 0
    begin
        raiserror(15289,-1,-1)
        return (1)
    end

    -- HANDLE UPDATE_ONE --
    if @Action = 'UPDATE_ONE'
    begin
        -- CHECK PERMISSIONS --
        if not is_member('db_owner') = 1
        begin
			EXEC %%System().AuditEvent(ID = 1196184405, Success = 0, TargetLoginName = @LoginName, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
			raiserror(15247,-1,-1)
            return (1)
        end
        else
        begin
			EXEC %%System().AuditEvent(ID = 1196184405, Success = 1, TargetLoginName = @LoginName, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
        end

        -- ERROR IF PARAMS NULL --
        -- OR IF A PASSWORD IS SPECIFIED --
        if @UserNamePattern IS Null or @LoginName IS Null or @Password IS NOT Null
        begin
            raiserror(15600,-1,-1,'sys.sp_change_users_login')
            return (1)
        end

        -- VALIDATE PARAMS --
        -- Can ONLY remap SQL Users to SQL Logins!  Should be no need
        --  for re-mapping NT logins, and if you try, you'll mess up
        --  the user status bits!
        declare @LoginType char(1)
        select @LoginType = type from sys.server_principals where
                        name = @LoginName              -- match login name
        if @LoginType IS Null
        begin
            -- perform a check on the user name like before the change to allow
            -- 'Update_one' on certificate based logins. This is to maintain compatibility
            -- with the error message that used to be returned before.
            if not exists 
                (select name 
                 from   sysusers 
                 where  name = @UserNamePattern		-- match user name
                 and    issqluser = 1               -- must be sql user
                 and    sid is not NULL
                 and    datalength(sid)  <= 16)				-- must not be a sql-user for the database
            begin
                raiserror(15291,-1,-1,'User',@UserNamePattern)
                return (1)
            end
            else
            begin
                raiserror(15600,-1,-1,'sys.sp_change_users_login')
                return (1)
            end
        end

        if not exists
            (select name
             from   sysusers
             where  name = @UserNamePattern		-- match user name
             and    ((issqluser = 1 AND datalength(sid)  <= 16) OR @LoginType = 'C')               -- must be sql user or cert based
             and    sid is not NULL)
        begin
            raiserror(15291,-1,-1,'User',@UserNamePattern)
            return (1)
        end

		if (@LoginType = 'K')        -- check if it is a asymmetric key login
		begin
		    raiserror(15291,-1,-1,'login', @LoginName)
            return (1)
		end

		BEGIN TRANSACTION

		-- LOCK USER --
		EXEC %%Owner(Name = @UserNamePattern).Lock(Exclusive = 1) -- may fail, back out below
		if @@error = 0
			select @loginsid = sid from master.dbo.syslogins where
				    loginname = @LoginName              -- match login name
				and isntname = 0                        -- cannot use nt logins
        if @loginsid is null
        begin
			ROLLBACK TRANSACTION
            raiserror(15291,-1,-1,'Login',@LoginName)
            return (1)
        end

        -- CHANGE THE USERS LOGIN (SID) - IF DUP, @@ERROR WILL INDICATE --
		EXEC %%UserOrGroup(Name = @UserNamePattern).SetSID(SID = @loginsid,
			IsExternal = 0, IsGroup = 0,
			Action = @Action, UserNamePattern = @UserNamePattern, LoginName = @LoginName, Password = @sanitizedPwd) -- may fail

        -- FINALIZATION: REPORT (ONLY IF NOT SUCCESSFUL) AND EXIT --
        if @@error <> 0
		begin
			select @ExistingUserName = name from sysusers where sid = @loginsid
			ROLLBACK TRANSACTION
		    raiserror(15063,-1,-1, @ExistingUserName)
		    return (1)
        end

		COMMIT TRANSACTION
        return (0)
    end

    -- ERROR IF NOT AUTO_FIX --
    if @Action <> 'AUTO_FIX'
    begin
        raiserror(15286,-1,-1,@ActionIn)
        return (1)
    end

    -- HANDLE AUTO_FIX --
    -- CHECK PERMISSIONS --
    if not is_srvrolemember('sysadmin') = 1
    begin
		EXEC %%System().AuditEvent(ID = 1178686293, Success = 0, TargetLoginName = NULL, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
        raiserror(15247,-1,-1)
        return (1)
    end
    else
    begin
		EXEC %%System().AuditEvent(ID = 1178686293, Success = 1, TargetLoginName = NULL, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
    end

    -- VALIDATE PARAMS --
    if @UserNamePattern IS Null or @LoginName IS NOT Null
    begin
		raiserror(15600,-1,-1,'sys.sp_change_users_login')
        return (1)
    end

    -- LOOP THRU ORPHANED USERS --
	select @exec_stmt = 'declare ms_crs_110_Users cursor global for
            select name from sysusers
            where name = N' + quotename( @UserNamePattern , '''')+ '
            and   issqluser = 1
            and   sid is not NULL
            and   datalength(sid) <= 16
            and   suser_sname(sid) is null'
    EXEC (@exec_stmt)
    open ms_crs_110_Users
    fetch next from ms_crs_110_Users into @110name

    while (@@fetch_status = 0)
    begin
   		if exists (select * from sys.server_principals where
						name = @110name              -- match login name
						and type in ('C', 'K'))            -- check if it is a certificate or asymmetric key login
		begin
			raiserror(15291,-1,-1, 'login', @110name)
			deallocate ms_crs_110_Users
       	    return (1)
       	end

        -- IS NAME ALREADY IN USE? --
		if not exists(select * from master.dbo.syslogins where loginname = @110name)
        begin
			-- VALIDATE PARAMS --
			if @Password IS Null
			begin
				raiserror(15600,-1,-1,'sys.sp_change_users_login')
				deallocate ms_crs_110_Users
				return (1)
			end

            -- ADD LOGIN --
            EXEC @ret = sys.sp_addlogin @110name, @Password, @dbname
            if @ret <> 0 or suser_sid(@110name) is null
            begin
                raiserror(15497,16,1,@110name)
                deallocate ms_crs_110_Users
                return (1)
            end
            select @FixMode = '1AddL'
            raiserror(15293,-1,-1,@110name)
        end
        else
        begin
            -- REPORT ERROR & CONTINUE IF DUPLICATE SID IN DB --
            select @FixMode = '2UpdU'
            raiserror(15292,-1,-1,@110name)
        end

        select @loginsid = suser_sid(@110name)
        if not exists (select * from sysusers where sid = @loginsid)
        begin
			-- LOCK USER --
			BEGIN TRANSACTION
			EXEC %%Owner(Name = @110name).Lock(Exclusive = 1)
			-- UPDATE SYSUSERS ROW --
			if @@error = 0
			begin
				EXEC %%UserOrGroup(Name = @110name).SetSID(SID = @loginsid,
						IsExternal = 0, IsGroup = 0,
						Action = @Action, UserNamePattern = @UserNamePattern, LoginName = @LoginName, Password = @sanitizedPwd) -- may fail
				if @@error <> 0
				begin
					select @ExistingUserName = name from sysusers where sid = @loginsid
					ROLLBACK TRANSACTION
					deallocate ms_crs_110_Users
					raiserror(15063,-1,-1, @ExistingUserName)
					return (1)
				end
			end
			COMMIT TRANSACTION

			if @FixMode = '1AddL'
				select @cfixesaddlogin = @cfixesaddlogin + 1
			else
				select @cfixesupdate = @cfixesupdate + 1
		end
		else
			raiserror(15331,-1,-1,@110name)

	    fetch next from ms_crs_110_Users into @110name
    end -- loop
	close ms_crs_110_Users
    deallocate ms_crs_110_Users

    -- REPORT AND RETURN SUCCESS --
    raiserror(15295,-1,-1,@cfixesupdate)
    raiserror(15294,-1,-1,@cfixesaddlogin)
    return (0) -- sp_change_users_login


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_change_users_login
    @Action               varchar(10)       -- REPORT / UPDATE_ONE / AUTO_FIX
   ,@UserNamePattern      sysname  = Null
   ,@LoginName            sysname  = Null
   ,@Password             sysname  = Null
AS
    -- SETUP RUNTIME OPTIONS / DECLARE VARIABLES --
	set nocount on
	declare @exec_stmt nvarchar(4000)

	declare @ret            int,
            @FixMode        char(5),
            @cfixesupdate   int,        -- count of fixes by update
            @cfixesaddlogin int,        -- count of fixes by sp_addlogin
            @dbname         sysname,
            @loginsid       varbinary(85),
            @110name        sysname,
            @ActionIn       varchar(10),
            @sanitizedPwd sysname -- We use this to pass on the password parameter down to SetSID

    -- SET INITIAL VALUES --
    select  @dbname         = db_name(),
            @cfixesupdate   = 0,
            @cfixesaddlogin = 0,
            @ActionIn = @Action,
            @Action = UPPER(@Action collate Latin1_General_CI_AS)

    -- set the sanitized password to NULL - we do not want to give any information about this parameter in eventdata
    set @sanitizedPwd = NULL


    -- INVALIDATE USE OF SPECIAL LOGIN/USER NAMES --
    if suser_sid(@LoginName) = 0x1 -- 'sa'
    begin
        raiserror(15287,-1,-1,@LoginName)
        return (1)
    end
    if database_principal_id(@UserNamePattern) in (0,1,2,3,4) --public, dbo, guest, INFORMATION_SCHEMA, sys
    begin
        raiserror(15287,-1,-1,@UserNamePattern)
        return (1)
    end

    -- HANDLE REPORT --
    if @Action = 'REPORT'
    begin

        -- CHECK PERMISSIONS --
        if not is_member('db_owner') = 1
        begin
		raiserror(15247,-1,-1)
            	return (1)
        end

        -- VALIDATE PARAMS --
        if @UserNamePattern IS NOT Null or @LoginName IS NOT Null or @Password IS NOT Null
        begin
            raiserror(15600,-1,-1,'sys.sp_change_users_login')
            return (1)
        end

        -- GENERATE REPORT --
        select UserName = name, UserSID = sid from sysusers
            where issqluser = 1
            and   (sid is not null and sid <> 0x0)
            and   (datalength(sid) <= 16)
            and   suser_sname(sid) is null
            order by name
        return (0)
    end

   -- ERROR IF IN USER TRANSACTION --
    if @@trancount > 0
    begin
        raiserror(15289,-1,-1)
        return (1)
    end

    -- HANDLE UPDATE_ONE --
    if @Action = 'UPDATE_ONE'
    begin
        -- CHECK PERMISSIONS --
        if not is_member('db_owner') = 1
        begin
			EXEC %%System().AuditEvent(ID = 1196184405, Success = 0, TargetLoginName = @LoginName, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
			raiserror(15247,-1,-1)
            return (1)
        end
        else
        begin
			EXEC %%System().AuditEvent(ID = 1196184405, Success = 1, TargetLoginName = @LoginName, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
        end

        -- ERROR IF PARAMS NULL --
        -- OR IF A PASSWORD IS SPECIFIED --
        if @UserNamePattern IS Null or @LoginName IS Null or @Password IS NOT Null
        begin
            raiserror(15600,-1,-1,'sys.sp_change_users_login')
            return (1)
        end

        -- VALIDATE PARAMS --
        -- Can ONLY remap SQL Users to SQL Logins!  Should be no need
        --  for re-mapping NT logins, and if you try, you'll mess up
        --  the user status bits!
        declare @LoginType char(1)
        select @LoginType = type from sys.server_principals where
                        name = @LoginName              -- match login name
        if @LoginType IS Null
        begin
            -- perform a check on the user name like before the change to allow
            -- 'Update_one' on certificate based logins. This is to maintain compatibility
            -- with the error message that used to be returned before.
            if not exists 
                (select name 
                 from   sysusers 
                 where  name = @UserNamePattern		-- match user name
                 and    issqluser = 1               -- must be sql user
                 and    sid is not NULL
                 and    datalength(sid)  <= 16)				-- must not be a sql-user for the database
            begin
                raiserror(15291,-1,-1,'User',@UserNamePattern)
                return (1)
            end
            else
            begin
                raiserror(15600,-1,-1,'sys.sp_change_users_login')
                return (1)
            end
        end

        if not exists
            (select name
             from   sysusers
             where  name = @UserNamePattern		-- match user name
             and    ((issqluser = 1 AND datalength(sid)  <= 16) OR @LoginType = 'C')               -- must be sql user or cert based
             and    sid is not NULL)
        begin
            raiserror(15291,-1,-1,'User',@UserNamePattern)
            return (1)
        end

		if (@LoginType = 'K')        -- check if it is a asymmetric key login
		begin
		    raiserror(15291,-1,-1,'login', @LoginName)
            return (1)
		end

		BEGIN TRANSACTION

		-- LOCK USER --
		EXEC %%Owner(Name = @UserNamePattern).Lock(Exclusive = 1) -- may fail, back out below
		if @@error = 0
			select @loginsid = sid from master.dbo.syslogins where
				    loginname = @LoginName              -- match login name
				and isntname = 0                        -- cannot use nt logins
        if @loginsid is null
        begin
			ROLLBACK TRANSACTION
            raiserror(15291,-1,-1,'Login',@LoginName)
            return (1)
        end

        -- CHANGE THE USERS LOGIN (SID) - IF DUP, @@ERROR WILL INDICATE --
		EXEC %%UserOrGroup(Name = @UserNamePattern).SetSID(SID = @loginsid,
			IsExternal = 0, IsGroup = 0,
			Action = @Action, UserNamePattern = @UserNamePattern, LoginName = @LoginName, Password = @sanitizedPwd) -- may fail

        -- FINALIZATION: REPORT (ONLY IF NOT SUCCESSFUL) AND EXIT --
        if @@error <> 0
		begin
			ROLLBACK TRANSACTION
		    raiserror(15063,-1,-1)
		    return (1)
        end

		COMMIT TRANSACTION
        return (0)
    end

    -- ERROR IF NOT AUTO_FIX --
    if @Action <> 'AUTO_FIX'
    begin
        raiserror(15286,-1,-1,@ActionIn)
        return (1)
    end

    -- HANDLE AUTO_FIX --
    -- CHECK PERMISSIONS --
    if not is_srvrolemember('sysadmin') = 1
    begin
		EXEC %%System().AuditEvent(ID = 1178686293, Success = 0, TargetLoginName = NULL, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
        raiserror(15247,-1,-1)
        return (1)
    end
    else
    begin
		EXEC %%System().AuditEvent(ID = 1178686293, Success = 1, TargetLoginName = NULL, TargetUserName = @UserNamePattern, Role = NULL, Object = NULL, Provider = NULL, Server = NULL)
    end

    -- VALIDATE PARAMS --
    if @UserNamePattern IS Null or @LoginName IS NOT Null
    begin
		raiserror(15600,-1,-1,'sys.sp_change_users_login')
        return (1)
    end

    -- LOOP THRU ORPHANED USERS --
	select @exec_stmt = 'declare ms_crs_110_Users cursor global for
            select name from sysusers
            where name = N' + quotename( @UserNamePattern , '''')+ '
            and   issqluser = 1
            and   sid is not NULL
            and   datalength(sid) <= 16
            and   suser_sname(sid) is null'
    EXEC (@exec_stmt)
    open ms_crs_110_Users
    fetch next from ms_crs_110_Users into @110name

    while (@@fetch_status = 0)
    begin
   		if exists (select * from sys.server_principals where
						name = @110name              -- match login name
						and type in ('C', 'K'))            -- check if it is a certificate or asymmetric key login
		begin
			raiserror(15291,-1,-1, 'login', @110name)
			deallocate ms_crs_110_Users
       	    return (1)
       	end

        -- IS NAME ALREADY IN USE? --
		if not exists(select * from master.dbo.syslogins where loginname = @110name)
        begin
			-- VALIDATE PARAMS --
			if @Password IS Null
			begin
				raiserror(15600,-1,-1,'sys.sp_change_users_login')
				deallocate ms_crs_110_Users
				return (1)
			end

            -- ADD LOGIN --
            EXEC @ret = sys.sp_addlogin @110name, @Password, @dbname
            if @ret <> 0 or suser_sid(@110name) is null
            begin
                raiserror(15497,16,1,@110name)
                deallocate ms_crs_110_Users
                return (1)
            end
            select @FixMode = '1AddL'
            raiserror(15293,-1,-1,@110name)
        end
        else
        begin
            -- REPORT ERROR & CONTINUE IF DUPLICATE SID IN DB --
            select @FixMode = '2UpdU'
            raiserror(15292,-1,-1,@110name)
        end

        select @loginsid = suser_sid(@110name)
        if not exists (select * from sysusers where sid = @loginsid)
        begin
			-- LOCK USER --
			BEGIN TRANSACTION
			EXEC %%Owner(Name = @110name).Lock(Exclusive = 1)
			-- UPDATE SYSUSERS ROW --
			if @@error = 0
			begin
				EXEC %%UserOrGroup(Name = @110name).SetSID(SID = @loginsid,
						IsExternal = 0, IsGroup = 0,
						Action = @Action, UserNamePattern = @UserNamePattern, LoginName = @LoginName, Password = @sanitizedPwd) -- may fail
				if @@error <> 0
				begin
					ROLLBACK TRANSACTION
					deallocate ms_crs_110_Users
					raiserror(15063,-1,-1)
					return (1)
				end
			end
			COMMIT TRANSACTION

			if @FixMode = '1AddL'
				select @cfixesaddlogin = @cfixesaddlogin + 1
			else
				select @cfixesupdate = @cfixesupdate + 1
		end
		else
			raiserror(15331,-1,-1,@110name)

	    fetch next from ms_crs_110_Users into @110name
    end -- loop
	close ms_crs_110_Users
    deallocate ms_crs_110_Users

    -- REPORT AND RETURN SUCCESS --
    raiserror(15295,-1,-1,@cfixesupdate)
    raiserror(15294,-1,-1,@cfixesaddlogin)
    return (0) -- sp_change_users_login

