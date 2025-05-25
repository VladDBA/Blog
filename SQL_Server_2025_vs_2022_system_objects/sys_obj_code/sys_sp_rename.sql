use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_rename
	@objname	nvarchar(1035),		-- up to 4-part "old" name
	@newname	sysname,			-- one-part new name
	@objtype	varchar(13) = null	-- identifying the name
as
/* DOCUMENTATION:
   [1]  To rename a table, the @objname (meaning OldName) parm can be
passed in totally unqualified or fully qualified.
   [2]  The SA or DBO can rename objects owned by lesser users,
without the need for SetUser.
   [3]  The Owner portion of a qualified name can usually be
passed in in the omitted form (as in MyDb..MyTab or MyTab).  The
typical exception is when the SA/DBO is trying to rename a table
where the @objname is present twice in sysobjects as a table
owned only by two different lesser users; requiring an explicit
owner qualifier in @objname.
   [4]  An unspecified Owner qualifier will default to the
current user if doing so will either resolve what would
otherwise be an ambiguity within @objtype, or will result
in exactly one match.
   [5]  If Database is part of the qualified @objname,
then it must match the current database.  The @newname parm can
never be qualified.
   [6]  Here are the valid @objtype values.  They correspond to
system tables which track each type:
      'column'  'database'  'index'  'object'  'userdatatype'  'statistics'
The @objtype parm is sometimes required.  It is always required
for databases.  It is required whenever ambiguities would
otherwise exist.  Explicit use of @objtype is always encouraged.
   [7]  Parms can use quoted_identifiers.  For example:
   Execute sp_rename 'amy."his table"','"her table"','object'
*/
	set nocount      on
	set ansi_padding on

	declare @objtypeIN		varchar(13),
		@objtypeINOrig		varchar(13),
			@objnameLen 		int,
			@CurrentDb		sysname,
			@CountNumNodes	int,
			@UnqualOldName	sysname,
			@QualName1		sysname,
			@QualName2		sysname,
			@QualName3		sysname,
			@OwnAndObjName	nvarchar(517),	-- "[owner].[object]"
			@SchemaAndTypeName	nvarchar(517),	-- "[schema].[type]"
			@objid			int,
			@xtype			nchar(2),
			@indexstatsid		int,
			@colid			int,
			@cnstid			int,
			@xusertype		int,
			@schid			int,
			@objid_tmp		int,
			@xtype_tmp		nchar(2),
			@retcode		int,
			@published		bit,	-- Indicates table is used in replication
			@isstretched	tinyint,		-- Indicates table is stretched
			@isParentTableTemporal bit,
			@historyObjId int,
			@historySchId int,
			@inMemHistoryObjId int,
			@ledgerType		tinyint,
			@ledgerViewType	tinyint,
			@colname		sysname,
			@colnameLen		int,
			@objname_tmp	sysname,
			@objnameLen_tmp	int,
			@dwLakehouse	bit,
			@dwDataWarehouse bit



	-- initial (non-null) settings
	select	@CurrentDb		= db_name(),
			@objtypeIN		= @objtype,
			@objtypeINOrig		= @objtype

	-- make type case insensitive
	select @objtype = lower(@objtypeIN collate Latin1_General_CI_AS)

	------------------------------------------------------------------------
	-------------------  PHASE 10:  Simple parm edits  ---------------------
	------------------------------------------------------------------------

	-- Valid rename-type param?
	if (@objtype is not null AND
		@objtype not in ('column', 'database', 'index', 'object', 'userdatatype', 'statistics'))
	begin
		raiserror(15249,-1,-1,@objtypeIN)
		return 1
	end
	-- null names?
	if (@newname IS null)
	begin
		raiserror(15223,-1,11,'NewName')
		return 1
	end
	if (@objname IS null)
	begin
		raiserror(15223,-1,-1,'OldName')
		return 1
	end

	--	Is NewName minimally valid?
	exec @retcode = sp_validname @newname
	if @retcode <> 0
	begin
		raiserror(15224,-1,15,@newname)
		return 1
	end

	--	Parse apart the perhaps dots-qualified old name.
	select @UnqualOldName = parsename(@objname, 1),
			@QualName1 = parsename(@objname, 2),
			@QualName2 = parsename(@objname, 3),
			@QualName3 = parsename(@objname, 4)
	if (@UnqualOldName IS Null)
	begin
		raiserror(15253,-1,-1,@objname)
		return 1
	end

	-- count name parts --
	select @CountNumNodes = (case
				when @QualName3 is not null then 4
				when @QualName2 is not null then 3
				when @QualName1 is not null then 2
				else 1
			end)
	if (@objtype  = 'database' AND @CountNumNodes > 1)
	begin
		raiserror(15395,-1,20,@objtypeIN)
		return 1
	end
	if (@objtype in ('object','userdatatype') AND @CountNumNodes > 3)
	begin
		raiserror(15225,-1,-1,@objname, @CurrentDb, @objtypeIN)
		return 1
	end

	---------------------------------------------------------------------------
	------------------------   PHASE 15:  Lock Down Checks   ------------------
	---------------------------------------------------------------------------

	-- Check if this is an SDS session before applying checks. This is equivalent to chec eflNotAllowedInSds
	if (serverproperty('EngineEdition') in (5,12))
	begin
		if ((@objtype = 'object' and (@QualName3 is not null or (@QualName2 is not null and @QualName2 != @CurrentDb))) or 
			(@objtype != 'database' and (@QualName3 is not null and @QualName3 != @CurrentDb)) )
		begin
			select @objnameLen = len(@objname) * 2 -- length in bytes
			raiserror(40515,-1,-1, @objnameLen, @objname)
			return 1
		end
		if (@objtype = 'database')
		begin
			raiserror(40514,-1,-1, 'Renaming a given database with sp_rename')
			return 1
		end
	end

	-- Lock down checks for Synapse Data Warehouse in Microsoft Fabric (aka Trident).
	if (serverproperty('EngineEdition') = 11)
	begin
		-- Get the database edition.
		declare @dwEdition sysname
		set @dwEdition = cast(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS sysname)
		select @dwLakehouse = case when @dwEdition in ('LakeWarehouse') then 1 else 0 end
		select @dwDataWarehouse = case when @dwEdition in ('DataWarehouse') then 1 else 0 end
	end

	---------------------------------------------------------------------------
	----------------------  PHASE 20:  Settle Parm1ItemType  ------------------
	---------------------------------------------------------------------------

	------------- database?
	if (@objtype  = 'database')
	begin
		execute @retcode = sys.sp_renamedb @UnqualOldName ,@newname -- de-docu old sproc
		if @retcode <> 0
			return 1
		return 0
	end

	BEGIN TRANSACTION

	------------ type?
	if (@objtype = 'userdatatype' or @objtypeIN is null)
	begin
		select @xusertype = user_type_id, @schid = schema_id
			from sys.types where user_type_id = type_id(@objname)
		
		-- Lock scalar type exclusively and check permissions
		if not (@xusertype is null)
		begin
					
		-- cannot pass @objname	as it is nvarchar(1035) and Invoke fails %%ScalarType does not compile with four part name.
		-- cross db type resolution is not allowed (already taken care of by type_id) so only two part name is required.
		if (@QualName1 is not null)
			select @SchemaAndTypeName = QuoteName(@QualName1) + '.' + QuoteName(@UnqualOldName)
		else
			select @SchemaAndTypeName = QuoteName(@UnqualOldName)
			
			EXEC %%ScalarType(MultiName = @SchemaAndTypeName).LockMatchID(ID = @xusertype, Exclusive = 1)
			if (@@error <> 0)
				select @xusertype = null
		end
	
		-- check for wrong param
		if ((@xusertype is not null AND @objtype <> 'userdatatype') OR
			(@xusertype is null AND @objtype = 'userdatatype'))
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		if not (@xusertype is null)
			select	@objtype = 'userdatatype'

	end

	-- assuming column/index-name, obtain object/column id's
	if (@objtype in ('column', 'index', 'statistics') or @objtypeIN is null)
	begin
		if @QualName2 is not null
			select @objid = object_id(QuoteName(@QualName2) +'.'+ QuoteName(@QualName1))
		else
			select @objid = object_id(QuoteName(@QualName1))

		if not (@objid is null)	-- nice try?
		begin

			-- obtain owner-qual object name
			select @schid = ObjectProperty(@objid, 'schemaid')
			select @OwnAndObjName = QuoteName(schema_name(@schid))+'.'+QuoteName(object_name(@objid))

			-- make sure @OwnAndObjName is not null (i.e. that object was not dropped in the meantime)
			if (isnull(@OwnAndObjName, '') = @OwnAndObjName)
			begin
				EXEC %%Object(MultiName = @OwnAndObjName).LockMatchIDHekaton(ID = @objid, Exclusive = 1, BindInternal = 0)
				if @@error <> 0
					select @objid = null
				else
					select @xtype = type from sys.objects where object_id = @objid
			end
			else
				select @objid = null
		end
	end

	------------ column?
	if (@objtype = 'column' or @objtypeIN is null)
	begin
		-- find column; if it's in an internal table, it must be
		-- internal history table for in-memory temporal table (internal_type = 252)
		-- otherwise rename is not allowed
		if (@xtype in ('U','V') OR (@xtype = 'IT' AND 
		EXISTS (select internal_type from sys.internal_tables where object_id = @objid and internal_type = 252)))
			select @colid = column_id from sys.columns
					where object_id = @objid and name = @UnqualOldName

		-- check for wrong param
		if ((@colid is not null AND @objtype <> 'column') OR
			(@colid is null AND @objtype = 'column'))
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		-- remember if we've found a column
		if not (@colid is null)
		begin

			-- Block col rename when TridentOneLakeLinkPublishingDDLSupport is enabled, unless TridentOneLakeLinkPublishingDDLRenameColumns is also enabled.
			declare @is_trident_publishing_column_ddl_enabled int = 0;
			exec @is_trident_publishing_column_ddl_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkPublishingDDLSupport';
			declare @is_trident_publishing_ddl_rename_column_enabled int = 0; 
			exec @is_trident_publishing_ddl_rename_column_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkPublishingDDLRenameColumns';
	
			-- Block column rename for user tables in Native scenario in Microsoft Fabric (aka Trident).
			if (serverproperty('EngineEdition') = 12 and @CountNumNodes > 1 and exists (select * from sys.dm_feature_switches where name = N'TridentNativeSAL' and is_enabled = 1) and (@is_trident_publishing_ddl_rename_column_enabled = 0 or @is_trident_publishing_column_ddl_enabled = 0))
			begin
				select @xtype_tmp = type
					from sys.objects where object_id = object_id(@QualName1)

				if (@xtype_tmp = 'U')
				begin
					COMMIT TRANSACTION
					raiserror(22623,-1,-1, 'columns')
					return 1
				end
			end

			declare @is_table_trident_enabled int = 0;
			if ((select is_replicated from [sys].[tables] where object_id = @objid) = 1)
			begin
				set @is_table_trident_enabled = 1;
			end
			
			if (sys.fn_trident_link_is_enabled_for_current_db() = 1 and @is_table_trident_enabled = 1 and @is_trident_publishing_column_ddl_enabled = 1 and @is_trident_publishing_ddl_rename_column_enabled = 0)
			begin
				select @xtype_tmp = type
					from sys.objects where object_id = object_id(@QualName1)

				if (@xtype_tmp = 'U')
				begin
					COMMIT TRANSACTION
					raiserror(22623,-1,-1, @UnqualOldName)
					return 1
				end
			end

			-- Is this a dropped ledger column?
			if ((select is_dropped_ledger_column from sys.columns where column_id = @colid and object_id = @objid) = 1)
			begin
				-- dropped ledger columns cannot be renamed
				COMMIT TRANSACTION
				set @colname = @UnqualOldName
				set @colnameLen = len(@colname) * 2; -- length in bytes
				raiserror(37427,-1,-1, @colnameLen, @colname)
				return 1
			end

			
			-- If this is a ledger table, lock the object again but with the method that
			-- also checks for ALTER LEDGER permissions since this is needed to rename
			-- columns on Ledger tables. Avoid the overhead if not ledger.
			select @ledgerType = ledger_type from sys.tables where object_id = @objid
			if (@ledgerType = 2 or @ledgerType = 3)
				EXEC %%Object(MultiName = @OwnAndObjName).LockMatchIDHekatonCheckAlterLedger(ID = @objid, Exclusive = 1, BindInternal = 0)

			-- Is this a ledger view?
			select @ledgerViewType = ledger_view_type from sys.views where object_id = @objid
			if (@ledgerViewType <> 0)
			begin
				COMMIT TRANSACTION
				set @colname = @UnqualOldName
				set @objname_tmp = @QualName1
				set @colnameLen = len(@colname) * 2; -- length in bytes
				set @objnameLen_tmp = len(@objname_tmp) * 2; -- length in bytes
				raiserror(37429,-1,-1, @colnameLen, @colname, @objnameLen_tmp, @objname_tmp)
				return 1
			end

			-- Is this a temporal table column?
			if (objectproperty(@objid, 'tabletemporaltype') = 1 OR @ledgerType = 1)
			begin
				-- This column belongs to a temporal history table so it can't be renamed.
				COMMIT TRANSACTION

				declare @objQualifiedName nvarchar(776) = QuoteName(db_name()) + '.' + QuoteName(schema_name(@schid)) + '.' + QuoteName(object_name(@objid))
				raiserror(13759,-1,-1,@objQualifiedName)
				return 1
			end
			-- ledger_type of 2 indicates updatable, which has a history table (append_only ledger tables do not)
			else if (objectproperty(@objid, 'tabletemporaltype') = 2 or @ledgerType = 2)
			begin
				-- The column being renamed belongs to a temporal table, so we need to obtain a lock on its history table as well
				set @isParentTableTemporal = 1

				select @historyObjId = history_table_id from sys.tables where object_id = @objid
				select @inMemHistoryObjId = object_id from sys.internal_tables where parent_object_id = @objid

				set @historySchId  = (select objectproperty(@historyObjId, 'schemaid'))
				declare @OwnHistoryTableName nvarchar(517) =  QuoteName(schema_name(@historySchId)) + '.' + QuoteName(object_name(@historyObjId))
			
				EXEC %%Object(MultiName = @OwnHistoryTableName).LockMatchID(ID = @historyObjId, Exclusive = 1, BindInternal = 0)
				if @@error <> 0
				begin
					-- This is not an expected situation! We are already holding a lock on the
					-- current table by now, so obtaining the lock on history should succeed.
					COMMIT TRANSACTION
					raiserror(1204,-1,-1)
					return 1
				end
				
				if (@inMemHistoryObjId is not null)
				begin
					declare @OwnInMemHistoryTableName nvarchar(300) = '[sys].' + QuoteName(object_name(@inMemHistoryObjId))
					EXEC %%Object(MultiName = @OwnInMemHistoryTableName).LockMatchIDHekaton(ID = @inMemHistoryObjId, Exclusive = 1, BindInternal = 1)
					if @@error <> 0
					begin
						-- This is not an expected situation! We are already holding a lock on the
						-- current table by now, so obtaining the lock on internal history table should succeed.
						COMMIT TRANSACTION
						raiserror(1204,-1,-1)
						return 1
					end
				end

				-- Columns are not renamable in Lakehouse tables in Trident
				if (@dwLakehouse = 1)
				begin
					COMMIT TRANSACTION
					raiserror(24780,-1,-1)
					return 1
				end
			end

			select @published = is_published from sys.objects where object_id = @objid
			if @published = 0
				select @published = is_merge_published from sys.tables where object_id = @objid

			if (@published <> 0)
				begin
					COMMIT TRANSACTION
					raiserror(15051,-1,-1)
					return (0)
				end

			select @objtype = 'column'
		end
		
		-- check whether the table is stretched (locked down)
		select @isstretched = is_remote_data_archive_enabled FROM sys.tables WHERE object_id = @objid
		if @isstretched = 1
		begin
			COMMIT TRANSACTION
			raiserror(14903,-1,-1, @UnqualOldName)
			return 1
		end
	end

	------------ index?
	if (@objtype = 'index' or @objtypeIN is null)
	begin
		-- find index; if it's on an internal table, it must be
		-- internal history table for in-memory temporal table (internal_type = 252)
		-- otherwise rename is not allowed
		if (@xtype in ('U','V') OR (@xtype = 'IT' AND 
		EXISTS (select internal_type from sys.internal_tables where object_id = @objid and internal_type = 252)))
			select @indexstatsid = indexproperty(@objid, @UnqualOldName, 'IndexID')

		if (@indexstatsid = 0) 
			set @indexstatsid = NULL;

		-- find columnstore index where there is no stats
		if (@indexstatsid is null)
			select @indexstatsid = index_id from sys.indexes
					where object_id = @objid and name = @UnqualOldName and (type = 5 or type = 6)

		-- check for wrong param
		if ((@indexstatsid is not null AND @objtype <> 'index') OR
		    (@indexstatsid is null AND @objtype = 'index'))			
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		if not (@indexstatsid is null)
		begin
			select @objtype = 'index'
			select @cnstid = object_id, @xtype = type from sys.objects
				where name = @UnqualOldName and parent_object_id = @objid and type in ('PK','UQ')
		end

		-- check whether the table is stretched (locked down)
		select @isstretched = is_remote_data_archive_enabled FROM sys.tables WHERE object_id = @objid
		if @isstretched = 1
		begin
			COMMIT TRANSACTION
			raiserror(14862,-1,-1, @UnqualOldName)
			return 1
		end
	end
	
	------------ statistics?
	if (@objtype = 'statistics')
	begin
		select @indexstatsid = stats_id from sys.stats 
					where object_id = @objid and name = @UnqualOldName
		
		-- check for wrong param
		if (@indexstatsid is null)
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end	
	end

	------------ object?
	if (@objtype = 'object' or @objtypeIN is null)
	begin
		-- get object id, type
		select @objid_tmp = object_id(@objname)
		
		if not (@objid_tmp is null)
		begin
			-- obtain owner-qual object name
			select @schid = ObjectProperty(@objid_tmp, 'schemaid')
			select @OwnAndObjName = QuoteName(schema_name(@schid))+'.'+QuoteName(object_name(@objid_tmp))

			-- make sure @OwnAndObjName is not null (i.e. that object was not dropped in the meantime)
			if (isnull(@OwnAndObjName, '') = @OwnAndObjName)
			begin
				-- Also request an ALTER LEDGER permission check (this will be done only if this is a ledger table or view)
				EXEC %%Object(MultiName = @OwnAndObjName).LockMatchIDHekatonCheckAlterLedger(ID = @objid_tmp, Exclusive = 1, BindInternal = 0)
				if @@error <> 0
					select @objid_tmp = null
			end
			else
				select @objid_tmp = null
		end

		select @xtype_tmp = type
			from sys.objects where object_id = @objid_tmp

		declare @is_trident_notifications_enabled int = 0;
		exec @is_trident_notifications_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkLZNotifications';
		declare @is_trident_publishing_ddl_rename_table int = 0; 
		exec @is_trident_publishing_ddl_rename_table = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkPublishingDDLRenameAndTransferringTables';
	
		-- Block table rename for user tables in Native scenario in Microsoft Fabric (aka Trident).
		if (serverproperty('EngineEdition') = 12 and @xtype_tmp = 'U'
			and exists (select * from sys.dm_feature_switches where name = N'TridentNativeSAL' and is_enabled = 1)
			and (@is_trident_notifications_enabled = 0 or @is_trident_publishing_ddl_rename_table = 0))
		begin
			COMMIT TRANSACTION
			raiserror(22623,-1,-1, 'tables')
			return 1
		end

		-- if object is a system table, a Scalar function, or a table valued function, skip it.

		-- Cannot rename system table
		if @xtype_tmp = 'S'
			select @objid_tmp = NULL

		-- Locks parent object and increments schema_ver
		if not (@objid_tmp is null)
		begin

			if (@xtype_tmp in ('U'))
			begin

				select @published = is_merge_published | is_published from sys.tables where object_id = @objid_tmp
				if (@published <> 0)
				begin
					COMMIT TRANSACTION
					raiserror(15051,-1,-1)
					return (0)
				end

				declare @is_trident_link_notifications_enabled int = 0
				declare @is_trident_link_rename_enabled int = 0
				exec @is_trident_link_notifications_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkLZNotifications'
				exec @is_trident_link_rename_enabled = sys.sp_is_featureswitch_enabled N'TridentOneLakeLinkPublishingDDLRenameAndTransferringTables'

				if ([sys].[fn_synapselink_is_table_enabled](object_id(@objname)) = 1 and [sys].[fn_ces_is_enabled_for_current_db]() = 1)
				begin
					COMMIT TRANSACTION
					raiserror(23661,-1,-1)
					return (0)
				end

				if ([sys].[fn_synapselink_is_table_enabled](object_id(@objname)) = 1 and ([sys].[fn_trident_link_is_enabled_for_current_db]() != 1 or @is_trident_link_notifications_enabled = 0 or @is_trident_link_rename_enabled = 0)) 
				begin
					COMMIT TRANSACTION
					raiserror(22749,-1,-1)
					return (0)
				end
			end

			-- parent already locked via locking object

		end

		if (@objid_tmp is not null and (@dwLakehouse = 1 or @dwDataWarehouse = 1))
		begin
			-- The following objects are supported and renamable in a Trident data warehouse:
			--
			-- User-defined tables (U) (DataWarehouse edition only)
			-- Views (V)
			-- PRIMARY KEY constraints (PK)
			-- FOREIGN KEY constraints (F)
			-- UNIQUE constraints (UQ)
			-- Stored procedures (P)
			-- Inline table-valued functions (IF)
			-- Table-valued functions (TF)
			if (@xtype_tmp not in ('U', 'V', 'PK', 'F', 'UQ', 'P', 'IF', 'TF'))
			begin
				COMMIT TRANSACTION
				raiserror(15600,-1,-1, 'sys.sp_rename')
				return 1
			end

			-- Lakehouse tables cannot be renamed.
			if (@dwLakehouse = 1 and (@xtype_tmp = 'U'))
			begin
				COMMIT TRANSACTION
				raiserror(24718,-1,-1)
				return 1
			end

			-- Names of user defined tables must not contain
			-- forward slash (/) or backward slash (\) characters.
			-- Note: The starting position returned by charindex is 1-based, not 0-based.
			if (@xtype_tmp in ('U'))
			begin
				EXEC %%Object(ID = @objid_tmp).DwCheckValidTableName(Name = @newname)
				if @@error <> 0
				begin
					COMMIT TRANSACTION
					return 1
				end
			end
		end

		-- check for wrong param
		if ((@objid_tmp is not null AND @objtype <> 'object') OR
			(@objid_tmp is null AND @objtype = 'object'))
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		if not (@objid_tmp is null)
			select @objtype = 'object', @objid = @objid_tmp, @xtype = @xtype_tmp

	end

	---------------------------------------------------------------------
	-------------------  PHASE 30:  More parm edits  --------------------
	---------------------------------------------------------------------

	-- item type determined?
	if (@objtype is null)
	begin
		COMMIT TRANSACTION
		raiserror(15225,-1,-1,@objname, @CurrentDb, @objtypeIN)
		return 1
	end

	-- renaming is only supported for columns and some types of objects in trident warehouses and lakehouses.
	if (@objtype not in ('object','column') AND (@dwLakehouse = 1 or @dwDataWarehouse = 1))
	begin
		COMMIT TRANSACTION
		raiserror(15600,-1,-1,'sys.sp_rename')
		return 1
	end

	-- was the original name valid given this type?
	if (@objtype in ('object','userdatatype') AND @CountNumNodes > 3)
	begin
		COMMIT TRANSACTION
		raiserror(15225,-1,-1,@objname, @CurrentDb, @objtypeIN)
		return 1
	end

	-- verify db qualifier is current db
	if (@objtype in ('object','userdatatype'))
		select @QualName3 = @QualName2
	if (isnull(@QualName3, @CurrentDb) <> @CurrentDb)
	begin
		COMMIT TRANSACTION
		raiserror(15333,-1,-1,@QualName3)
		return 1
	end

	if (@objtype <> 'userdatatype')
	begin
		-- check if system object except for internal tables
		-- which might be allowed to be renamed in specific cases
		select @schid = ObjectProperty(@objid, 'schemaid')
		if ((ObjectProperty(@objid, 'IsMSShipped') = 1 OR ObjectProperty(@objid, 'IsSystemTable') = 1) 
			AND NOT EXISTS (select internal_type from sys.internal_tables where object_id = @objid and internal_type = 252))
		begin
			COMMIT TRANSACTION
			select @objnameLen = datalength(@objname)
			raiserror(15001,-1,-1, @objnameLen, @objname)
			return 1
		end
	end

	-- System created Date Correlation view and its columns/indexes cannot be renamed.
	if (@objid is not null
		AND @xtype = 'V '
		AND 1 = Isnull((select is_date_correlation_view from sys.views where object_id = @objid), 0)
		)
	begin
		COMMIT TRANSACTION
		raiserror(15168,-1,-1, @objname)
		return 1
	end

	-- FileTable system defined objects and columns cannot be renamed.
	if ((@objtype = 'object'
		AND exists (select object_id from sys.filetable_system_defined_objects where object_id = @objid))
		OR 
		(@objtype = 'index'
		AND exists (select object_id from sys.filetable_system_defined_objects where object_id = @cnstid))
		OR
		(@objtype = 'column' 
		AND exists (select object_id from sys.tables where object_id = @objid and is_filetable = 1)))
	begin
		COMMIT TRANSACTION
		set @objnameLen = len(@objname) * 2; -- length in bytes
		raiserror(3865,-1,-1,@objnameLen,@objname)
		return 1
	end

	-- make sure orig no longer shows null
	if @objtypeIN is null
		select @objtypeIN = @objtype

	-- Check for name clashing with existing name(s)
	if (@newname <> @UnqualOldName)
	begin
		-- column name clash?
		if (@objtype = 'column')
			if (ColumnProperty(@objid, @newname, 'isidentity') is not null)
				select @UnqualOldName = NULL
		-- index name clash?
		if ( (@objtype = 'object' AND @xtype in ('PK','UQ'))
				OR @objtype = 'index')
			if indexproperty(@objid, @newname, 'IndexID') is not null 

				select @UnqualOldName = NULL
		-- cnst name clash?
		if (@cnstid IS NOT null)
			if (object_id(QuoteName(schema_name(@schid)) +'.'+ QuoteName(@newname), 'local') is not null)
				select @UnqualOldName = NULL
		-- object name clash?		
		if (@objtype = 'object')
			if (object_id(QuoteName(schema_name(@schid)) +'.'+ QuoteName(@newname)) is not null)
				select @UnqualOldName = NULL
		-- stop on clash
		if (@UnqualOldName is null)
		begin
			COMMIT TRANSACTION
			raiserror(15335,-1,-1,@newname,@objtypeIN)
			return 1
		end
	end

	--------------------------------------------------------------------------
	--------------------  PHASE 32:  Temporary Table Issue -------------------
	--------------------------------------------------------------------------
	-- Disallow renaming object to or from a temp name (starts with #)
	if (@objtype = 'object' AND
		(substring(@newname,1,1) = N'#' OR
		substring(object_name(@objid),1,1) = N'#'))
	begin
		COMMIT TRANSACTION
		raiserror(15600,-1,-1, 'sys.sp_rename')
		return 1
	end

	--------------------------------------------------------------------------
	--------------------  PHASE 34:  Cautionary messages  --------------------
	--------------------------------------------------------------------------

	if @objtype = 'column'
	begin
		-- Check for Dependencies: No column rename if enforced dependency on column
		if exists (select * from sys.sql_dependencies d where
			d.referenced_major_id = @objid
			AND d.referenced_minor_id = @colid
			AND d.class = 1
			-- filter dependencies where the dependent object d.object_id is a date correlation view.
			-- Date correlation view owner is the same as of any of the tables they are dependent upon so user having alter access
			-- to any of the participating tables will have catalog view access to the MPIV for the following query to succeed.
			AND 0 = Isnull((select is_date_correlation_view from sys.views o where o.object_id =  d.object_id), 0)
			)
		begin
			COMMIT TRANSACTION
			raiserror(15336,-1,-1, @objname)
			return 1
		end
	end
	else if @objtype = 'object'
	begin
		-- Check for Dependencies: No RENAME or CHANGEOWNER of OBJECT when exists:
		if exists (select * from sys.sql_dependencies d where
			d.referenced_major_id = @objid		-- A dependency on this object
			AND d.class > 0		-- that is enforced
			AND @objid <> d.object_id		-- that isn't a self-reference (self-references don't use object name)
			-- filter dependencies where the dependent object d.object_id is a date correlation view.
			AND 0 = Isnull((select is_date_correlation_view from sys.views o where o.object_id =  d.object_id), 0)
			-- And isn't a reference from a child object (also don't use object name)			
			-- As we might not have permission on the dependent object, we list all the child
			-- objects of the object to be renamed and make sure that child object id is not the depending object here.
			AND 0 = (select count(*) from sys.objects o where o.parent_object_id = @objid and o.object_id = d.object_id)
			)
		begin
			COMMIT TRANSACTION
			raiserror(15336,-1,-1, @objname)
			return 1
		end
	end

	-- WITH DEFERRED RESOLUTION, sql_dependencies  IS NOT VERY ACCURATE, SO WE ALSO
	--	RAISE THIS WARNING **UNCONDITIONALLY**, EVEN FOR NON-OBJECT RENAMES
	raiserror(15477,-1,-1)

	-- warn about dependencies...
	if (@objtype = 'objects'
		and exists (select * from sys.sql_dependencies d where 
						d.referenced_major_id = @objid
						-- filter dependencies where the dependent object d.object_id is a date correlation view.
						AND 0 = Isnull((select is_date_correlation_view from sys.views o where o.object_id =  d.object_id), 0)
						))
		raiserror(15337,-1,-1)

	--------------------------------------------------------------------------
	---------------------  PHASE 40:  Update system tables  ------------------
	--------------------------------------------------------------------------

	-- DO THE UPDATES --	
	if (@objtype = 'userdatatype')						-------- change type name
	begin

		EXEC %%ScalarType(ID = @xusertype).SetName(Name = @newname)
		if @@error <> 0
		begin
			COMMIT TRANSACTION
			raiserror(15335,-1,-1,@newname,@objtypeIN)
			return 1
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass( x_eunc_Type), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		EXEC %%System().FireTrigger(ID = 241, ID = 6, ID = @xusertype, ID = 0, Value = @UnqualOldName,
			ID = -1, ID = 0, ID = 0, Value = NULL, 
			ID = 3, Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)

	end
	else if (@objtype = 'object')						-------- change object name
	begin

		-- update the object name
		declare @invoke_object_setname_last_error int
		EXEC %%Object(ID = @objid).SetName(Name = @newname)
		set @invoke_object_setname_last_error = @@error
		if @invoke_object_setname_last_error <> 0
		begin
			COMMIT TRANSACTION
			if @invoke_object_setname_last_error = 6
				raiserror(15336, -1, -1, @objname)
			else if @invoke_object_setname_last_error <> 37427 and @invoke_object_setname_last_error <> 15001
			and @invoke_object_setname_last_error <> 15248
			-- rename attempted on dropped ledger object or internal table
				raiserror(15335,-1,-1,@newname,@objtypeIN)
			return 1
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass( x_eunc_Table), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		EXEC %%System().FireTrigger(ID = 241, ID = 1, ID = @objid, ID = 0, Value = @UnqualOldName,
			ID = -1, ID = 0, ID = 0, Value = NULL, ID = 3,
			Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)

	end
	else if (@objtype in ('index', 'statistics'))						-------- change index name
	begin		
		-- update the index or statistics name, and change object name if cnst	 	
		declare @last_error int
		EXEC %%IndexOrStats(ObjectID = @objid, Name = @UnqualOldName).SetName(Name = @newname)
		set @last_error = @@error
		if @last_error <> 0
		begin
			COMMIT TRANSACTION
			if @last_error <> 15001 and @last_error <> 15248
			begin
				declare @len int = 2*len(@newname)
				if @last_error = 4				
					raiserror(12802,-1,2, @len, @newname, 100)
				else
				begin
					if @last_error = 6
						raiserror(15336, -1, -1, @objname)
					else
						raiserror(15335,-1,-1,@newname,@objtypeIN)
				end
			end
			return 1
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass( x_eunc_Index | x_eunc_Stats), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		declare @eunc int
		if (@objtype = 'index')
			set @eunc = 7;
		else if (@objtype = 'statistics') 
			set @eunc = 9;
			
		EXEC %%System().FireTrigger(ID = 241, ID = @eunc, ID = @objid, ID = @indexstatsid, Value = @UnqualOldName,
			ID = 1, ID = @objid, ID = 0, Value = NULL, ID = 3,
			Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)
	end	
	else if (@objtype = 'column')						-------- change column name
	begin
		declare @historyColName nvarchar(776),  @inMemhistoryColName nvarchar(550)
		declare @historyColId int, @inMemhistoryColId int
		EXEC %%ColumnEx(ObjectID = @objid, Name = @UnqualOldName, AllowInternalTable = 0).SetName(Name = @newname)
		if @@error <> 0
		begin
			COMMIT TRANSACTION
			return 1
		end

		if (@isParentTableTemporal = 1)
		begin
			set @historyColName = QuoteName(object_name(@historyObjId)) + '.' + QuoteName(@UnqualOldName)
			if (@CountNumNodes = 3)
				set @historyColName = QuoteName(schema_name(@historySchId)) + '.' + @historyColName

			set @historyColId = COLUMNPROPERTY(@historyObjId, @UnqualOldName, 'ColumnId')
			EXEC %%ColumnEx(ObjectID = @historyObjId, Name = @UnqualOldName, AllowInternalTable = 0).SetName(Name = @newname)

			if @@error <> 0
			begin
				ROLLBACK TRANSACTION
				return 1
			end

			if (@inMemHistoryObjId is not null)
			begin
				set @inMemhistoryColName = '[sys].' + QuoteName(object_name(@inMemHistoryObjId)) + '.' + QuoteName(@UnqualOldName)

				set @inMemhistoryColId = COLUMNPROPERTY(@inMemHistoryObjId, @UnqualOldName, 'ColumnId')
				EXEC %%ColumnEx(ObjectID = @inMemHistoryObjId, Name = @UnqualOldName, AllowInternalTable = 1).SetName(Name = @newname)

				if @@error <> 0
				begin
					ROLLBACK TRANSACTION
					return 1
				end
			end
		end

		-- for ledger table columns also recreate the corresponding ledger view
		if (@ledgerType <> 0)
		begin
			EXEC %%Object(ID = @objid).RecreateLedgerView()

			if @@error <> 0
			begin
				ROLLBACK TRANSACTION
				return 1
			end
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass(x_eunc_Table), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		EXEC %%System().FireTrigger(ID = 241, ID = 1, ID = @objid, ID = @colid, Value = @UnqualOldName,
			ID = 1, ID = @objid, ID = 0, Value = NULL, ID = 3,
			Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)
		
		if (@isParentTableTemporal = 1)
			EXEC %%System().FireTrigger(ID = 241, ID = 1, ID = @historyObjId, ID = @historyColId, Value = @UnqualOldName,
				ID = 1, ID = @historyObjId, ID = 0, Value = NULL, ID = 3,
				Value = @historyColName, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)
	end

	COMMIT TRANSACTION

	-- Success --
	return 0 -- sp_rename


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_rename
	@objname	nvarchar(1035),		-- up to 4-part "old" name
	@newname	sysname,			-- one-part new name
	@objtype	varchar(13) = null	-- identifying the name
as
/* DOCUMENTATION:
   [1]  To rename a table, the @objname (meaning OldName) parm can be
passed in totally unqualified or fully qualified.
   [2]  The SA or DBO can rename objects owned by lesser users,
without the need for SetUser.
   [3]  The Owner portion of a qualified name can usually be
passed in in the omitted form (as in MyDb..MyTab or MyTab).  The
typical exception is when the SA/DBO is trying to rename a table
where the @objname is present twice in sysobjects as a table
owned only by two different lesser users; requiring an explicit
owner qualifier in @objname.
   [4]  An unspecified Owner qualifier will default to the
current user if doing so will either resolve what would
otherwise be an ambiguity within @objtype, or will result
in exactly one match.
   [5]  If Database is part of the qualified @objname,
then it must match the current database.  The @newname parm can
never be qualified.
   [6]  Here are the valid @objtype values.  They correspond to
system tables which track each type:
      'column'  'database'  'index'  'object'  'userdatatype'  'statistics'
The @objtype parm is sometimes required.  It is always required
for databases.  It is required whenever ambiguities would
otherwise exist.  Explicit use of @objtype is always encouraged.
   [7]  Parms can use quoted_identifiers.  For example:
   Execute sp_rename 'amy."his table"','"her table"','object'
*/
	set nocount      on
	set ansi_padding on

	declare @objtypeIN		varchar(13),
		@objtypeINOrig		varchar(13),
			@objnameLen 		int,
			@CurrentDb		sysname,
			@CountNumNodes	int,
			@UnqualOldName	sysname,
			@QualName1		sysname,
			@QualName2		sysname,
			@QualName3		sysname,
			@OwnAndObjName	nvarchar(517),	-- "[owner].[object]"
			@SchemaAndTypeName	nvarchar(517),	-- "[schema].[type]"
			@objid			int,
			@xtype			nchar(2),
			@indexstatsid		int,
			@colid			int,
			@cnstid			int,
			@xusertype		int,
			@schid			int,
			@objid_tmp		int,
			@xtype_tmp		nchar(2),
			@retcode		int,
			@published		bit,	-- Indicates table is used in replication
			@isstretched	tinyint,		-- Indicates table is stretched
			@isParentTableTemporal bit,
			@historyObjId int,
			@historySchId int,
			@ledgerType		tinyint,
			@ledgerViewType	tinyint,
			@colname		sysname,
			@colnameLen		int,
			@objname_tmp	sysname,
			@objnameLen_tmp	int



	-- initial (non-null) settings
	select	@CurrentDb		= db_name(),
			@objtypeIN		= @objtype,
			@objtypeINOrig		= @objtype

	-- make type case insensitive
	select @objtype = lower(@objtypeIN collate Latin1_General_CI_AS)

	------------------------------------------------------------------------
	-------------------  PHASE 10:  Simple parm edits  ---------------------
	------------------------------------------------------------------------

	-- Valid rename-type param?
	if (@objtype is not null AND
		@objtype not in ('column', 'database', 'index', 'object', 'userdatatype', 'statistics'))
	begin
		raiserror(15249,-1,-1,@objtypeIN)
		return 1
	end
	-- null names?
	if (@newname IS null)
	begin
		raiserror(15223,-1,11,'NewName')
		return 1
	end
	if (@objname IS null)
	begin
		raiserror(15223,-1,-1,'OldName')
		return 1
	end

	--	Is NewName minimally valid?
	exec @retcode = sp_validname @newname
	if @retcode <> 0
	begin
		raiserror(15224,-1,15,@newname)
		return 1
	end

	--	Parse apart the perhaps dots-qualified old name.
	select @UnqualOldName = parsename(@objname, 1),
			@QualName1 = parsename(@objname, 2),
			@QualName2 = parsename(@objname, 3),
			@QualName3 = parsename(@objname, 4)
	if (@UnqualOldName IS Null)
	begin
		raiserror(15253,-1,-1,@objname)
		return 1
	end

	-- count name parts --
	select @CountNumNodes = (case
				when @QualName3 is not null then 4
				when @QualName2 is not null then 3
				when @QualName1 is not null then 2
				else 1
			end)
	if (@objtype  = 'database' AND @CountNumNodes > 1)
	begin
		raiserror(15395,-1,20,@objtypeIN)
		return 1
	end
	if (@objtype in ('object','userdatatype') AND @CountNumNodes > 3)
	begin
		raiserror(15225,-1,-1,@objname, @CurrentDb, @objtypeIN)
		return 1
	end

	---------------------------------------------------------------------------
	------------------------   PHASE 15:  Lock Down Checks   ------------------
	---------------------------------------------------------------------------

	-- Check if this is an SDS session before applying checks. This is equivalent to chec eflNotAllowedInSds
	if (serverproperty('EngineEdition') = 5)
	begin
		if ((@objtype = 'object' and (@QualName3 is not null or (@QualName2 is not null and @QualName2 != @CurrentDb))) or 
			(@objtype != 'database' and (@QualName3 is not null and @QualName3 != @CurrentDb)) )
		begin
			select @objnameLen = len(@objname) * 2 -- length in bytes
			raiserror(40515,-1,-1, @objnameLen, @objname)
			return 1
		end
		if (@objtype = 'database')
		begin
			raiserror(40514,-1,-1, 'Renaming a given database with sp_rename')
			return 1
		end
	end

	---------------------------------------------------------------------------
	----------------------  PHASE 20:  Settle Parm1ItemType  ------------------
	---------------------------------------------------------------------------

	------------- database?
	if (@objtype  = 'database')
	begin
		execute @retcode = sys.sp_renamedb @UnqualOldName ,@newname -- de-docu old sproc
		if @retcode <> 0
			return 1
		return 0
	end

	BEGIN TRANSACTION

	------------ type?
	if (@objtype = 'userdatatype' or @objtypeIN is null)
	begin
		select @xusertype = user_type_id, @schid = schema_id
			from sys.types where user_type_id = type_id(@objname)
		
		-- Lock scalar type exclusively and check permissions
		if not (@xusertype is null)
		begin
					
		-- cannot pass @objname	as it is nvarchar(1035) and Invoke fails %%ScalarType does not compile with four part name.
		-- cross db type resolution is not allowed (already taken care of by type_id) so only two part name is required.
		if (@QualName1 is not null)
			select @SchemaAndTypeName = QuoteName(@QualName1) + '.' + QuoteName(@UnqualOldName)
		else
			select @SchemaAndTypeName = QuoteName(@UnqualOldName)
			
			EXEC %%ScalarType(MultiName = @SchemaAndTypeName).LockMatchID(ID = @xusertype, Exclusive = 1)
			if (@@error <> 0)
				select @xusertype = null
		end
	
		-- check for wrong param
		if ((@xusertype is not null AND @objtype <> 'userdatatype') OR
			(@xusertype is null AND @objtype = 'userdatatype'))
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		if not (@xusertype is null)
			select	@objtype = 'userdatatype'

	end

	-- assuming column/index-name, obtain object/column id's
	if (@objtype in ('column', 'index', 'statistics') or @objtypeIN is null)
	begin
		if @QualName2 is not null
			select @objid = object_id(QuoteName(@QualName2) +'.'+ QuoteName(@QualName1))
		else
			select @objid = object_id(QuoteName(@QualName1))

		if not (@objid is null)	-- nice try?
		begin

			-- obtain owner-qual object name
			select @schid = ObjectProperty(@objid, 'schemaid')
			select @OwnAndObjName = QuoteName(schema_name(@schid))+'.'+QuoteName(object_name(@objid))

			-- make sure @OwnAndObjName is not null (i.e. that object was not dropped in the meantime)
			if (isnull(@OwnAndObjName, '') = @OwnAndObjName)
			begin
				EXEC %%Object(MultiName = @OwnAndObjName).LockMatchIDHekaton(ID = @objid, Exclusive = 1, BindInternal = 0)
				if @@error <> 0
					select @objid = null
				else
					select @xtype = type from sys.objects where object_id = @objid
			end
			else
				select @objid = null
		end
	end

	------------ column?
	if (@objtype = 'column' or @objtypeIN is null)
	begin
		-- find column
		if (@xtype in ('U','V'))
			select @colid = column_id from sys.columns
					where object_id = @objid and name = @UnqualOldName

		-- check for wrong param
		if ((@colid is not null AND @objtype <> 'column') OR
			(@colid is null AND @objtype = 'column'))
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		-- remember if we've found a column
		if not (@colid is null)
		begin
	
			-- Is this a dropped ledger column?
			if ((select is_dropped_ledger_column from sys.columns where column_id = @colid and object_id = @objid) = 1)
			begin
				-- dropped ledger columns cannot be renamed
				COMMIT TRANSACTION
				set @colname = @UnqualOldName
				set @colnameLen = len(@colname) * 2; -- length in bytes
				raiserror(37427,-1,-1, @colnameLen, @colname)
				return 1
			end

			
			-- If this is a ledger table, lock the object again but with the method that
			-- also checks for ALTER LEDGER permissions since this is needed to rename
			-- columns on Ledger tables. Avoid the overhead if not ledger.
			select @ledgerType = ledger_type from sys.tables where object_id = @objid
			if (@ledgerType = 2 or @ledgerType = 3)
				EXEC %%Object(MultiName = @OwnAndObjName).LockMatchIDHekatonCheckAlterLedger(ID = @objid, Exclusive = 1, BindInternal = 0)

			-- Is this a ledger view?
			select @ledgerViewType = ledger_view_type from sys.views where object_id = @objid
			if (@ledgerViewType <> 0)
			begin
				COMMIT TRANSACTION
				set @colname = @UnqualOldName
				set @objname_tmp = @QualName1
				set @colnameLen = len(@colname) * 2; -- length in bytes
				set @objnameLen_tmp = len(@objname_tmp) * 2; -- length in bytes
				raiserror(37429,-1,-1, @colnameLen, @colname, @objnameLen_tmp, @objname_tmp)
				return 1
			end

			-- Is this a temporal table column?
			if (objectproperty(@objid, 'tabletemporaltype') = 1 OR @ledgerType = 1)
			begin
				-- This column belongs to a temporal history table so it can't be renamed.
				COMMIT TRANSACTION

				declare @objQualifiedName nvarchar(776) = db_name() + '.' + QuoteName(schema_name(@schid)) + '.' + QuoteName(object_name(@objid))
				raiserror(13759,-1,-1,@objQualifiedName)
				return 1
			end
			-- ledger_type of 2 indicates updatable, which has a history table (append_only ledger tables do not)
			else if (objectproperty(@objid, 'tabletemporaltype') = 2 or @ledgerType = 2)
			begin
				-- The column being renamed belongs to a temporal table, so we need to obtain a lock on its history table as well
				set @isParentTableTemporal = 1
				set @historyObjId  = (select history_table_id from sys.tables where object_id = @objid)
				set @historySchId  = (select objectproperty(@historyObjId, 'schemaid'))
				declare @OwnHistoryTableName nvarchar(517) =  QuoteName(schema_name(@historySchId)) + '.' + QuoteName(object_name(@historyObjId))
			
				EXEC %%Object(MultiName = @OwnHistoryTableName).LockMatchID(ID = @historyObjId, Exclusive = 1, BindInternal = 0)
				if @@error <> 0
				begin
					-- This is not an expected situation! We are already holding a lock on the
					-- current table by now, so obtaining the lock on history should succeed.
					COMMIT TRANSACTION
					raiserror(1204,-1,-1)
					return 1
				end
			end

			select @published = is_published from sys.objects where object_id = @objid
			if @published = 0
				select @published = is_merge_published from sys.tables where object_id = @objid

			if (@published <> 0)
				begin
					COMMIT TRANSACTION
					raiserror(15051,-1,-1)
					return (0)
				end

			select @objtype = 'column'
		end
		
		-- check whether the table is stretched (locked down)
		select @isstretched = is_remote_data_archive_enabled FROM sys.tables WHERE object_id = @objid
		if @isstretched = 1
		begin
			COMMIT TRANSACTION
			raiserror(14903,-1,-1, @UnqualOldName)
			return 1
		end
	end

	------------ index?
	if (@objtype = 'index' or @objtypeIN is null)
	begin
		-- find index
		if (@xtype in ('U','V'))
			select @indexstatsid = indexproperty(@objid, @UnqualOldName, 'IndexID')

		if (@indexstatsid = 0) 
			set @indexstatsid = NULL;

		-- find columnstore index where there is no stats
		if (@indexstatsid is null)
			select @indexstatsid = index_id from sys.indexes
					where object_id = @objid and name = @UnqualOldName and (type = 5 or type = 6)

		-- check for wrong param
		if ((@indexstatsid is not null AND @objtype <> 'index') OR
		    (@indexstatsid is null AND @objtype = 'index'))			
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		if not (@indexstatsid is null)
		begin
			select @objtype = 'index'
			select @cnstid = object_id, @xtype = type from sys.objects
				where name = @UnqualOldName and parent_object_id = @objid and type in ('PK','UQ')
		end

		-- check whether the table is stretched (locked down)
		select @isstretched = is_remote_data_archive_enabled FROM sys.tables WHERE object_id = @objid
		if @isstretched = 1
		begin
			COMMIT TRANSACTION
			raiserror(14862,-1,-1, @UnqualOldName)
			return 1
		end
	end
	
	------------ statistics?
	if (@objtype = 'statistics')
	begin
		select @indexstatsid = stats_id from sys.stats 
					where object_id = @objid and name = @UnqualOldName
		
		-- check for wrong param
		if (@indexstatsid is null)
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end	
	end

	------------ object?
	if (@objtype = 'object' or @objtypeIN is null)
	begin
		-- get object id, type
		select @objid_tmp = object_id(@objname)
		
		if not (@objid_tmp is null)
		begin
			-- obtain owner-qual object name
			select @schid = ObjectProperty(@objid_tmp, 'schemaid')
			select @OwnAndObjName = QuoteName(schema_name(@schid))+'.'+QuoteName(object_name(@objid_tmp))

			-- make sure @OwnAndObjName is not null (i.e. that object was not dropped in the meantime)
			if (isnull(@OwnAndObjName, '') = @OwnAndObjName)
			begin
				-- Also request an ALTER LEDGER permission check (this will be done only if this is a ledger table or view)
				EXEC %%Object(MultiName = @OwnAndObjName).LockMatchIDHekatonCheckAlterLedger(ID = @objid_tmp, Exclusive = 1, BindInternal = 0)
				if @@error <> 0
					select @objid_tmp = null
			end
			else
				select @objid_tmp = null
		end

		select @xtype_tmp = type
			from sys.objects where object_id = @objid_tmp

		-- if object is a system table, a Scalar function, or a table valued function, skip it.

		-- Cannot rename system table
		if @xtype_tmp = 'S'
			select @objid_tmp = NULL

		-- Locks parent object and increments schema_ver
		if not (@objid_tmp is null)
		begin

			if (@xtype_tmp in ('U'))
			begin

				select @published = is_merge_published | is_published from sys.tables where object_id = @objid_tmp
				if (@published <> 0)
				begin
					COMMIT TRANSACTION
					raiserror(15051,-1,-1)
					return (0)
				end

				if ([sys].[fn_synapselink_is_table_enabled](object_id(@objname)) = 1) 
				begin
					COMMIT TRANSACTION
					raiserror(22749,-1,-1)
					return (0)
				end
			end

			-- parent already locked via locking object

		end

		-- check for wrong param
		if ((@objid_tmp is not null AND @objtype <> 'object') OR
			(@objid_tmp is null AND @objtype = 'object'))
		begin
			COMMIT TRANSACTION
			raiserror(15248,-1,-1,@objtypeIN)
			return 1
		end

		if not (@objid_tmp is null)
			select @objtype = 'object', @objid = @objid_tmp, @xtype = @xtype_tmp

	end

	---------------------------------------------------------------------
	-------------------  PHASE 30:  More parm edits  --------------------
	---------------------------------------------------------------------

	-- item type determined?
	if (@objtype is null)
	begin
		COMMIT TRANSACTION
		raiserror(15225,-1,-1,@objname, @CurrentDb, @objtypeIN)
		return 1
	end

	-- was the original name valid given this type?
	if (@objtype in ('object','userdatatype') AND @CountNumNodes > 3)
	begin
		COMMIT TRANSACTION
		raiserror(15225,-1,-1,@objname, @CurrentDb, @objtypeIN)
		return 1
	end

	-- verify db qualifier is current db
	if (@objtype in ('object','userdatatype'))
		select @QualName3 = @QualName2
	if (isnull(@QualName3, @CurrentDb) <> @CurrentDb)
	begin
		COMMIT TRANSACTION
		raiserror(15333,-1,-1,@QualName3)
		return 1
	end

	if (@objtype <> 'userdatatype')
	begin
		-- check if system object
		select @schid = ObjectProperty(@objid, 'schemaid')
		if (ObjectProperty(@objid, 'IsMSShipped') = 1 OR
			ObjectProperty(@objid, 'IsSystemTable') = 1)
		begin
			COMMIT TRANSACTION
			raiserror(15001,-1,-1, @objname)
			return 1
		end
	end
	
	-- System created Date Correlation view and its columns/indexes cannot be renamed.
	if (@objid is not null
		AND @xtype = 'V '
		AND 1 = Isnull((select is_date_correlation_view from sys.views where object_id = @objid), 0)
		)
	begin
		COMMIT TRANSACTION
		raiserror(15168,-1,-1, @objname)
		return 1
	end

	-- FileTable system defined objects and columns cannot be renamed.
	if ((@objtype = 'object'
		AND exists (select object_id from sys.filetable_system_defined_objects where object_id = @objid))
		OR 
		(@objtype = 'index'
		AND exists (select object_id from sys.filetable_system_defined_objects where object_id = @cnstid))
		OR
		(@objtype = 'column' 
		AND exists (select object_id from sys.tables where object_id = @objid and is_filetable = 1)))
	begin
		COMMIT TRANSACTION
		set @objnameLen = len(@objname) * 2; -- length in bytes
		raiserror(3865,-1,-1,@objnameLen,@objname)
		return 1
	end

	-- make sure orig no longer shows null
	if @objtypeIN is null
		select @objtypeIN = @objtype

	-- Check for name clashing with existing name(s)
	if (@newname <> @UnqualOldName)
	begin
		-- column name clash?
		if (@objtype = 'column')
			if (ColumnProperty(@objid, @newname, 'isidentity') is not null)
				select @UnqualOldName = NULL
		-- index name clash?
		if ( (@objtype = 'object' AND @xtype in ('PK','UQ'))
				OR @objtype = 'index')
			if indexproperty(@objid, @newname, 'IndexID') is not null 

				select @UnqualOldName = NULL
		-- cnst name clash?
		if (@cnstid IS NOT null)
			if (object_id(QuoteName(schema_name(@schid)) +'.'+ QuoteName(@newname), 'local') is not null)
				select @UnqualOldName = NULL
		-- object name clash?		
		if (@objtype = 'object')
			if (object_id(QuoteName(schema_name(@schid)) +'.'+ QuoteName(@newname)) is not null)
				select @UnqualOldName = NULL
		-- stop on clash
		if (@UnqualOldName is null)
		begin
			COMMIT TRANSACTION
			raiserror(15335,-1,-1,@newname,@objtypeIN)
			return 1
		end
	end

	--------------------------------------------------------------------------
	--------------------  PHASE 32:  Temporay Table Isssue -------------------
	--------------------------------------------------------------------------
	-- Disallow renaming object to or from a temp name (starts with #)
	if (@objtype = 'object' AND
		(substring(@newname,1,1) = N'#' OR
		substring(object_name(@objid),1,1) = N'#'))
	begin
		COMMIT TRANSACTION
		raiserror(15600,-1,-1, 'sys.sp_rename')
		return 1
	end

	--------------------------------------------------------------------------
	--------------------  PHASE 34:  Cautionary messages  --------------------
	--------------------------------------------------------------------------

	if @objtype = 'column'
	begin
		-- Check for Dependencies: No column rename if enforced dependency on column
		if exists (select * from sys.sql_dependencies d where
			d.referenced_major_id = @objid
			AND d.referenced_minor_id = @colid
			AND d.class = 1
			-- filter dependencies where the dependent object d.object_id is a date correlation view.
			-- Date correlation view owner is the same as of any of the tables they are dependent upon so user having alter access
			-- to any of the participating tables will have catalog view access to the MPIV for the following query to succeed.
			AND 0 = Isnull((select is_date_correlation_view from sys.views o where o.object_id =  d.object_id), 0)
			)
		begin
			COMMIT TRANSACTION
			raiserror(15336,-1,-1, @objname)
			return 1
		end
	end
	else if @objtype = 'object'
	begin
		-- Check for Dependencies: No RENAME or CHANGEOWNER of OBJECT when exists:
		if exists (select * from sys.sql_dependencies d where
			d.referenced_major_id = @objid		-- A dependency on this object
			AND d.class > 0		-- that is enforced
			AND @objid <> d.object_id		-- that isn't a self-reference (self-references don't use object name)
			-- filter dependencies where the dependent object d.object_id is a date correlation view.
			AND 0 = Isnull((select is_date_correlation_view from sys.views o where o.object_id =  d.object_id), 0)
			-- And isn't a reference from a child object (also don't use object name)			
			-- As we might not have permission on the dependent object, we list all the child
			-- objects of the object to be renamed and make sure that child object id is not the depending object here.
			AND 0 = (select count(*) from sys.objects o where o.parent_object_id = @objid and o.object_id = d.object_id)
			)
		begin
			COMMIT TRANSACTION
			raiserror(15336,-1,-1, @objname)
			return 1
		end
	end

	-- WITH DEFERRED RESOLUTION, sql_dependencies  IS NOT VERY ACCURATE, SO WE ALSO
	--	RAISE THIS WARNING **UNCONDITIONALLY**, EVEN FOR NON-OBJECT RENAMES
	raiserror(15477,-1,-1)

	-- warn about dependencies...
	if (@objtype = 'objects'
		and exists (select * from sys.sql_dependencies d where 
						d.referenced_major_id = @objid
						-- filter dependencies where the dependent object d.object_id is a date correlation view.
						AND 0 = Isnull((select is_date_correlation_view from sys.views o where o.object_id =  d.object_id), 0)
						))
		raiserror(15337,-1,-1)

	--------------------------------------------------------------------------
	---------------------  PHASE 40:  Update system tables  ------------------
	--------------------------------------------------------------------------

	-- DO THE UPDATES --	
	if (@objtype = 'userdatatype')						-------- change type name
	begin

		EXEC %%ScalarType(ID = @xusertype).SetName(Name = @newname)
		if @@error <> 0
		begin
			COMMIT TRANSACTION
			raiserror(15335,-1,-1,@newname,@objtypeIN)
			return 1
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass( x_eunc_Type), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		EXEC %%System().FireTrigger(ID = 241, ID = 6, ID = @xusertype, ID = 0, Value = @UnqualOldName,
			ID = -1, ID = 0, ID = 0, Value = NULL, 
			ID = 3, Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)

	end
	else if (@objtype = 'object')						-------- change object name
	begin

		-- update the object name
		declare @invoke_object_setname_last_error int
		EXEC %%Object(ID = @objid).SetName(Name = @newname)
		set @invoke_object_setname_last_error = @@error
		if @invoke_object_setname_last_error <> 0
		begin
			COMMIT TRANSACTION
			if @invoke_object_setname_last_error = 6
				raiserror(15336, -1, -1, @objname)
			else if @invoke_object_setname_last_error <> 37427 -- rename attempted on dropped ledger object
				raiserror(15335,-1,-1,@newname,@objtypeIN)
			return 1
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass( x_eunc_Table), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		EXEC %%System().FireTrigger(ID = 241, ID = 1, ID = @objid, ID = 0, Value = @UnqualOldName,
			ID = -1, ID = 0, ID = 0, Value = NULL, ID = 3,
			Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)

	end
	else if (@objtype in ('index', 'statistics'))						-------- change index name
	begin		
		-- update the index or statistics name, and change object name if cnst	 	
		declare @last_error int
		EXEC %%IndexOrStats(ObjectID = @objid, Name = @UnqualOldName).SetName(Name = @newname)
		set @last_error = @@error
		if @last_error <> 0
		begin
			COMMIT TRANSACTION
			declare @len int = 2*len(@newname)
			if @last_error = 4				
				raiserror(12802,-1,2, @len, @newname, 100)
			else
			begin
				if @last_error = 6
					raiserror(15336, -1, -1, @objname)
				else
					raiserror(15335,-1,-1,@newname,@objtypeIN)
			end
			return 1
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass( x_eunc_Index | x_eunc_Stats), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		declare @eunc int
		if (@objtype = 'index')
			set @eunc = 7;
		else if (@objtype = 'statistics') 
			set @eunc = 9;
			
		EXEC %%System().FireTrigger(ID = 241, ID = @eunc, ID = @objid, ID = @indexstatsid, Value = @UnqualOldName,
			ID = 1, ID = @objid, ID = 0, Value = NULL, ID = 3,
			Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)
	end	
	else if (@objtype = 'column')						-------- change column name
	begin

		declare @historyColName nvarchar(776)
		declare @historyColId int
		EXEC %%ColumnEx(ObjectID = @objid, Name = @UnqualOldName).SetName(Name = @newname)
		if @@error <> 0
		begin
			COMMIT TRANSACTION
			return 1
		end

		if (@isParentTableTemporal = 1)
		begin
			set @historyColName = QuoteName(object_name(@historyObjId)) + '.' + QuoteName(@UnqualOldName)
			if (@CountNumNodes = 3)
				set @historyColName = QuoteName(schema_name(@historySchId)) + '.' + QuoteName(@historyColName)

			set @historyColId = COLUMNPROPERTY(@historyObjId, @UnqualOldName, 'ColumnId')
			EXEC %%ColumnEx(ObjectID = @historyObjId, Name = @UnqualOldName).SetName(Name = @newname)

			if @@error <> 0
			begin
				ROLLBACK TRANSACTION
				return 1
			end
		end

		-- for ledger table columns also recreate the corresponding ledger view
		if (@ledgerType <> 0)
		begin
			EXEC %%Object(ID = @objid).RecreateLedgerView()

			if @@error <> 0
			begin
				ROLLBACK TRANSACTION
				return 1
			end
		end

		-- EMDEventType(x_eet_Rename), EMDUniversalClass(x_eunc_Table), src major id, src minor id, src name
		-- -1 means ignore target stuff, target major id, target minor id, target name,
		-- # of parameters, 5 parameters
		EXEC %%System().FireTrigger(ID = 241, ID = 1, ID = @objid, ID = @colid, Value = @UnqualOldName,
			ID = 1, ID = @objid, ID = 0, Value = NULL, ID = 3,
			Value = @objname, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)
		
		if (@isParentTableTemporal = 1)
			EXEC %%System().FireTrigger(ID = 241, ID = 1, ID = @historyObjId, ID = @historyColId, Value = @UnqualOldName,
				ID = 1, ID = @historyObjId, ID = 0, Value = NULL, ID = 3,
				Value = @historyColName, Value = @newname, Value = @objtypeINOrig, Value = NULL, Value = NULL, Value = NULL, Value = NULL)
	end

	COMMIT TRANSACTION

	-- Success --
	return 0 -- sp_rename

