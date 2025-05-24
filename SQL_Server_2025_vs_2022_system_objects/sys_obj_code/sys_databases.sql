SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.databases AS
	SELECT d.database_logical_name as name,
		d.id_nonrepl AS database_id,
		r.indepid AS source_database_id,
		d.sid AS owner_sid,
		d.crdate AS create_date,
		d.cmptlevel AS compatibility_level,
		-- coll.value = null means that a collation was not specified for the DB and the server default is used instead
		convert(sysname, case when serverproperty('EngineEdition') in (5,12) AND d.id = 1 then serverproperty('collation')
                                 else CollationPropertyFromID(convert(int, isnull(coll.value, p.cid)), 'name') end) AS collation_name,
		iif ((serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x10000000) = 1), cast (3 as tinyint), p.user_access) AS user_access,
		iif ((serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x10000000) = 1), 'NO_ACCESS', ua.name) AS user_access_desc,
		sysconv(bit, d.status & 0x400) AS is_read_only,			-- DBR_RDONLY
		sysconv(bit, d.status & 1) AS is_auto_close_on,			-- DBR_CLOSE_ON_EXIT
		sysconv(bit, d.status & 0x400000) AS is_auto_shrink_on,		-- DBR_AUTOSHRINK
		case when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x00000020) = 1) then cast (1 as tinyint) -- RESTORING
			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x00000080) = 1) then cast (7 as tinyint) -- COPYING
			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x00000100) = 1) then cast (4 as tinyint) -- SUSPECT
			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x08000000) = 1) then cast (8 as tinyint) -- QUORUM_RECOVERY_PENDING
  			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x04000000) = 1) then cast (9 as tinyint) -- CREATING
			 else p.state 
			 end AS state, -- 7 is COPYING and 4 is SUSPECT state for database copy (UNDO: Need to have a clean way to set states in dbtable for a user db)
		case when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x00000020) = 1) then 'RESTORING' 
			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x00000080) = 1) then 'COPYING' 
			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x00000100) = 1) then 'SUSPECT'
			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x08000000) = 1) then CONVERT(nvarchar(60), N'QUORUM_RECOVERY_PENDING')
  			 when (serverproperty('EngineEdition') in (5,12)) AND (sysconv(bit, d.status & 0x04000000) = 1) then 'CREATING'
			 else st.name 
			 end AS state_desc,
		sysconv(bit, d.status & 0x200000) AS is_in_standby,		-- DBR_STANDBY
		case when serverproperty('EngineEdition') in (5,12) then convert(bit, 0) else p.is_cleanly_shutdown end AS is_cleanly_shutdown,
		sysconv(bit, d.status & 0x80000000) AS is_supplemental_logging_enabled,	-- DBR_SUPPLEMENT_LOG
        case when (serverproperty('EngineEdition') in (5,12)) then sysconv(tinyint, sysconv(bit,(d.status & 0x00100000)))
             else p.snapshot_isolation_state end AS snapshot_isolation_state,
        case when (serverproperty('EngineEdition') in (5,12)) and (sysconv(bit, d.status & 0x00100000) = 1) then 'ON'
             when (serverproperty('EngineEdition') in (5,12)) and (sysconv(bit, d.status & 0x00100000) = 0) then 'OFF'
             else si.name end AS snapshot_isolation_state_desc,		
		sysconv(bit, d.status & 0x800000) AS is_read_committed_snapshot_on,		-- DBR_READCOMMITTED_SNAPSHOT
        case when (serverproperty('EngineEdition') in (5,12)) 
	         then case 
	         		when sysconv(bit,(d.status & 0x00000008)) = 1
	        	 		then cast(3 as tinyint)
	        	  	when sysconv(bit,(d.status & 0x00000004)) = 1
	        	 		then cast(2 as tinyint)
	        	 	else
	        	 		cast(1 as tinyint)
	          	   end	
              else p.recovery_model 
        end AS recovery_model,
		case when (serverproperty('EngineEdition') in (5,12)) 
	         then case 
	         		when sysconv(bit,(d.status & 0x00000008)) = 1
	        	 		then CONVERT(nvarchar(60), N'SIMPLE')
	        	 	when sysconv(bit,(d.status & 0x00000004)) = 1
	        	 		then CONVERT(nvarchar(60), N'BULK_LOGGED')
	        	 	else
	        	 		CONVERT(nvarchar(60), N'FULL')
	        	  end	
			  else ro.name 
	    end AS recovery_model_desc,              
		p.page_verify_option, pv.name AS page_verify_option_desc,
		sysconv(bit, d.status2 & 0x1000000) AS is_auto_create_stats_on,			-- DBR_AUTOCRTSTATS
		sysconv(bit, d.status2 & 0x00400000) AS is_auto_create_stats_incremental_on,	-- DBR_AUTOCRTSTATSINC
		sysconv(bit, d.status2 & 0x40000000) AS is_auto_update_stats_on,		-- DBR_AUTOUPDSTATS
		sysconv(bit, d.status2 & 0x80000000) AS is_auto_update_stats_async_on,	-- DBR_AUTOUPDSTATSASYNC
		sysconv(bit, d.status2 & 0x4000) AS is_ansi_null_default_on,			-- DBR_ANSINULLDFLT
		sysconv(bit, d.status2 & 0x4000000) AS is_ansi_nulls_on,				-- DBR_ANSINULLS
		sysconv(bit, d.status2 & 0x2000) AS is_ansi_padding_on,					-- DBR_ANSIPADDING
		sysconv(bit, d.status2 & 0x10000000) AS is_ansi_warnings_on,			-- DBR_ANSIWARNINGS
		sysconv(bit, d.status2 & 0x1000) AS is_arithabort_on,					-- DBR_ARITHABORT
		sysconv(bit, d.status2 & 0x10000) AS is_concat_null_yields_null_on,		-- DBR_CATNULL
		sysconv(bit, d.status2 & 0x800) AS is_numeric_roundabort_on,			-- DBR_NUMEABORT
		sysconv(bit, d.status2 & 0x800000) AS is_quoted_identifier_on,			-- DBR_QUOTEDIDENT
		sysconv(bit, d.status2 & 0x20000) AS is_recursive_triggers_on,			-- DBR_RECURTRIG
		sysconv(bit, d.status2 & 0x2000000) AS is_cursor_close_on_commit_on,	-- DBR_CURSCLOSEONCOM
		sysconv(bit, d.status2 & 0x100000) AS is_local_cursor_default,			-- DBR_DEFLOCALCURS
		sysconv(bit, d.status2 & 0x20000000) AS is_fulltext_enabled,			-- DBR_FTENABLED
		sysconv(bit, d.status2 & 0x200) AS is_trustworthy_on,				-- DBR_TRUSTWORTHY
		sysconv(bit, d.status2 & 0x400) AS is_db_chaining_on,				-- DBR_DBCHAINING
		sysconv(bit, d.status2 & 0x08000000) AS is_parameterization_forced,	-- DBR_UNIVERSALAUTOPARAM
		sysconv(bit, d.status2 & 64) AS is_master_key_encrypted_by_server,	-- DBR_MASTKEY
		sysconv(bit, d.status2 & 0x00000010) AS is_query_store_on,			-- DBR_QDSENABLED
		sysconv(bit, d.category & 1) AS is_published,
		sysconv(bit, d.category & 2) AS is_subscribed,
		sysconv(bit, d.category & 4) AS is_merge_published,
		sysconv(bit, d.category & 16) AS is_distributor,
		sysconv(bit, d.category & 32) AS is_sync_with_backup,
		d.svcbrkrguid AS service_broker_guid,
		sysconv(bit, case when d.scope = 0 then 1 else 0 end) AS is_broker_enabled,
		p.log_reuse_wait, lr.name AS log_reuse_wait_desc,
		sysconv(bit, d.status2 & 4) AS is_date_correlation_on, 		-- DBR_DATECORRELATIONOPT
		sysconv(bit, d.category & 64) AS is_cdc_enabled,
		case 
			when (d.id = db_id('tempdb')) then sysconv(bit, p.is_db_encrypted)
			else sysconv(bit, d.status2 & 0x100)					-- DBR_ENCRYPTION
			end AS is_encrypted,
		convert(bit, d.status2 & 0x8) AS is_honor_broker_priority_on,				-- DBR_HONORBRKPRI
		sgr.guid AS replica_id,
		sgr2.guid AS group_database_id,
		ssr.indepid AS resource_pool_id,
		default_language_lcid = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(smallint, p.default_language) else null end,
		default_language_name = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(sysname, sld.name) else null end,
		default_fulltext_language_lcid = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(int, p.default_fulltext_language) else null end,
		default_fulltext_language_name = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(sysname, slft.name) else null end,
		is_nested_triggers_on = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(bit, p.allow_nested_triggers) else null end,
		is_transform_noise_words_on = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(bit, p.transform_noise_words) else null end,
		two_digit_year_cutoff = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(smallint, p.two_digit_year_cutoff) else null end,
		containment = sysconv(tinyint, (d.status2 & 0x80000)/0x80000), -- DBR_IS_CDB
		containment_desc = convert(nvarchar(60), cdb.name),
		p.recovery_seconds AS target_recovery_time_in_seconds,
		p.delayed_durability,
		case when (p.delayed_durability = 0) then CAST('DISABLED' AS nvarchar(60)) -- LCOPT_DISABLED
			 when (p.delayed_durability = 1) then CAST('ALLOWED' AS nvarchar(60)) -- LCOPT_ALLOWED
			 when (p.delayed_durability = 2) then CAST('FORCED' AS nvarchar(60)) -- LCOPT_FORCED
			 else NULL
			 end AS delayed_durability_desc,
		convert(bit, d.status2 & 0x80) AS 
		is_memory_optimized_elevate_to_snapshot_on,				-- DBR_HKELEVATETOSNAPSHOT
		sysconv(bit, d.category & 0x100) AS is_federation_member,
		convert(bit, isnull(rda.value, 0)) AS is_remote_data_archive_enabled,
		convert(bit, p.is_mixed_page_allocation_on) AS is_mixed_page_allocation_on,
		sysconv(bit, p.is_temporal_history_retention_on) AS is_temporal_history_retention_enabled,
		case 
			when ((d.category & 0x200)=0x200) then 2
			when ((d.status2 & 0x80000)=0x80000) then 1
			else 0
			end AS catalog_collation_type,
		case when ((d.category & 0x200)=0x200) then CAST('SQL_Latin1_General_CP1_CI_AS' AS nvarchar(60)) 
			when ((d.status2 & 0x80000)=0x80000) then CAST('Latin1_General_100_CI_AS_KS_WS_SC' AS nvarchar(60))
			else CAST('DATABASE_DEFAULT' AS nvarchar(60))
			end AS catalog_collation_type_desc,
		p.physical_database_name,
	        case when p.is_trident_db_rsc_on = 1 then CAST(1 AS bit) else sysconv(bit, d.status & 0x00004000) end AS is_result_set_caching_on,
		p.is_accelerated_database_recovery_on, 
		p.is_scalable_tempdb_on AS is_tempdb_spill_to_remote_store,
		p.is_stale_page_detection_on,
	    p.is_memory_optimized_enabled,
		sysconv(bit, p.is_data_retention_on) AS is_data_retention_enabled,
		p.is_ledger_on,
		sysconv(bit, d.category & 0x800) AS is_change_feed_enabled,
		sysconv(bit, d.category & 0x2000) AS is_data_lake_replication_enabled,
		sysconv(bit, d.category & 0x4000) AS is_event_stream_enabled,
		p.data_compaction, v7.name AS data_compaction_desc,
		p.data_lake_log_publishing, v8.name AS data_lake_log_publishing_desc,
		sysconv(bit, p.is_vorder_on) AS is_vorder_enabled,
		sysconv(bit, p.is_proactive_statistics_refresh_on) AS is_proactive_statistics_refresh_on,
		p.is_optimized_locking_on
	FROM sys.sysdbreg$ d OUTER APPLY OpenRowset(TABLE DBPROP, (case when serverproperty('EngineEdition') in (5,12) then DB_ID() else d.id end)) p
	LEFT JOIN sys.syssingleobjrefs r ON r.depid = d.id AND r.class = 96 AND r.depsubid = 0	-- SRC_VIEWPOINTDB
	LEFT JOIN sys.syspalvalues st ON st.class = 'DBST' AND st.value = p.state
	LEFT JOIN sys.syspalvalues ua ON ua.class = 'DBUA' AND ua.value = p.user_access
	LEFT JOIN sys.syspalvalues si ON si.class = 'DBSI' AND si.value = p.snapshot_isolation_state
	LEFT JOIN sys.syspalvalues ro ON ro.class = 'DBRO' AND ro.value = p.recovery_model
	LEFT JOIN sys.syspalvalues pv ON pv.class = 'DBPV' AND pv.value = p.page_verify_option
	LEFT JOIN sys.syspalvalues lr ON lr.class = 'LRWT' AND lr.value = p.log_reuse_wait
	LEFT JOIN sys.syspalvalues v7 ON v7.class = 'SYST' AND v7.value = p.data_compaction
	LEFT JOIN sys.syspalvalues v8 ON v8.class = 'SYST' AND v8.value = p.data_lake_log_publishing
	LEFT JOIN sys.syssingleobjrefs agdb ON agdb.depid = d.id AND agdb.class = 104 AND agdb.depsubid = 0	-- SRC_AVAILABILITYGROUP 
	LEFT JOIN master.sys.syssingleobjrefs ssr ON ssr.class = 108 AND ssr.depid = d.id -- SRC_RG_DB_TO_POOL
	LEFT JOIN master.sys.sysclsobjs  ag ON ag.id = agdb.indepid AND ag.class = 67 -- SOC_AVAILABILITY_GROUP
	LEFT JOIN master.sys.sysguidrefs sgr ON sgr.class = 8 AND sgr.id = ag.id AND sgr.subid = 1 -- GRC_AGGUID / AGGUID_REPLICA_ID
	LEFT JOIN master.sys.sysguidrefs sgr2 ON sgr2.class = 9 AND sgr2.id = ag.id AND sgr2.subid = d.id -- GRC_AGDBGUID
	LEFT JOIN sys.syspalvalues cdb ON cdb.class = 'DCDB' AND cdb.value = CASE WHEN (d.status2 & 0x80000)=0x80000 THEN 1 ELSE 0 END
	LEFT JOIN sys.syslanguages sld ON sld.lcid = p.default_language
	LEFT JOIN sys.fulltext_languages slft ON slft.lcid = p.default_fulltext_language
	LEFT JOIN sys.sysobjvalues coll ON coll.valclass = 102 AND coll.subobjid = 0 AND coll.objid = d.id	-- SVC_DATACOLLATION
	LEFT JOIN sys.sysobjvalues rda ON rda.valclass = 116 AND rda.objid = d.id AND rda.valnum = 0 -- SVC_STRETCH & STRETCH_DB_IS_STRETCHED
	WHERE d.id < 0x7fff AND repl_sys_db_visible(d.id) = 1
		AND (engineedition() <> 11 OR sys_db_visible_polaris(d.id) = 1)
		AND has_access('DB', (case when serverproperty('EngineEdition') in (5,12) then DB_ID() else d.id end)) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.databases AS
	SELECT d.database_logical_name as name,
		d.id_nonrepl AS database_id,
		r.indepid AS source_database_id,
		d.sid AS owner_sid,
		d.crdate AS create_date,
		d.cmptlevel AS compatibility_level,
		-- coll.value = null means that a collation was not specified for the DB and the server default is used instead
		convert(sysname, case when serverproperty('EngineEdition') = 5 AND d.id = 1 then serverproperty('collation')
                                 else CollationPropertyFromID(convert(int, isnull(coll.value, p.cid)), 'name') end) AS collation_name,
		iif ((serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x10000000) = 1), cast (3 as tinyint), p.user_access) AS user_access,
		iif ((serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x10000000) = 1), 'NO_ACCESS', ua.name) AS user_access_desc,
		sysconv(bit, d.status & 0x400) AS is_read_only,			-- DBR_RDONLY
		sysconv(bit, d.status & 1) AS is_auto_close_on,			-- DBR_CLOSE_ON_EXIT
		sysconv(bit, d.status & 0x400000) AS is_auto_shrink_on,		-- DBR_AUTOSHRINK
		case when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x00000020) = 1) then cast (1 as tinyint) -- RESTORING
			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x00000080) = 1) then cast (7 as tinyint) -- COPYING
			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x00000100) = 1) then cast (4 as tinyint) -- SUSPECT
			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x08000000) = 1) then cast (8 as tinyint) -- QUORUM_RECOVERY_PENDING
  			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x04000000) = 1) then cast (9 as tinyint) -- CREATING
			 else p.state 
			 end AS state, -- 7 is COPYING and 4 is SUSPECT state for database copy (UNDO: Need to have a clean way to set states in dbtable for a user db)
		case when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x00000020) = 1) then 'RESTORING' 
			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x00000080) = 1) then 'COPYING' 
			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x00000100) = 1) then 'SUSPECT'
			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x08000000) = 1) then CONVERT(nvarchar(60), N'QUORUM_RECOVERY_PENDING')
  			 when (serverproperty('EngineEdition') = 5) AND (sysconv(bit, d.status & 0x04000000) = 1) then 'CREATING'
			 else st.name 
			 end AS state_desc,
		sysconv(bit, d.status & 0x200000) AS is_in_standby,		-- DBR_STANDBY
		case when serverproperty('EngineEdition') = 5 then convert(bit, 0) else p.is_cleanly_shutdown end AS is_cleanly_shutdown,
		sysconv(bit, d.status & 0x80000000) AS is_supplemental_logging_enabled,	-- DBR_SUPPLEMENT_LOG
        case when (serverproperty('EngineEdition') = 5) then sysconv(tinyint, sysconv(bit,(d.status & 0x00100000)))
             else p.snapshot_isolation_state end AS snapshot_isolation_state,
        case when (serverproperty('EngineEdition') = 5) and (sysconv(bit, d.status & 0x00100000) = 1) then 'ON'
             when (serverproperty('EngineEdition') = 5) and (sysconv(bit, d.status & 0x00100000) = 0) then 'OFF'
             else si.name end AS snapshot_isolation_state_desc,		
		sysconv(bit, d.status & 0x800000) AS is_read_committed_snapshot_on,		-- DBR_READCOMMITTED_SNAPSHOT
        case when (serverproperty('EngineEdition') = 5) 
	         then case 
	         		when sysconv(bit,(d.status & 0x00000008)) = 1
	        	 		then cast(3 as tinyint)
	        	  	when sysconv(bit,(d.status & 0x00000004)) = 1
	        	 		then cast(2 as tinyint)
	        	 	else
	        	 		cast(1 as tinyint)
	          	   end	
              else p.recovery_model 
        end AS recovery_model,
		case when (serverproperty('EngineEdition') = 5) 
	         then case 
	         		when sysconv(bit,(d.status & 0x00000008)) = 1
	        	 		then CONVERT(nvarchar(60), N'SIMPLE')
	        	 	when sysconv(bit,(d.status & 0x00000004)) = 1
	        	 		then CONVERT(nvarchar(60), N'BULK_LOGGED')
	        	 	else
	        	 		CONVERT(nvarchar(60), N'FULL')
	        	  end	
			  else ro.name 
	    end AS recovery_model_desc,              
		p.page_verify_option, pv.name AS page_verify_option_desc,
		sysconv(bit, d.status2 & 0x1000000) AS is_auto_create_stats_on,			-- DBR_AUTOCRTSTATS
		sysconv(bit, d.status2 & 0x00400000) AS is_auto_create_stats_incremental_on,	-- DBR_AUTOCRTSTATSINC
		sysconv(bit, d.status2 & 0x40000000) AS is_auto_update_stats_on,		-- DBR_AUTOUPDSTATS
		sysconv(bit, d.status2 & 0x80000000) AS is_auto_update_stats_async_on,	-- DBR_AUTOUPDSTATSASYNC
		sysconv(bit, d.status2 & 0x4000) AS is_ansi_null_default_on,			-- DBR_ANSINULLDFLT
		sysconv(bit, d.status2 & 0x4000000) AS is_ansi_nulls_on,				-- DBR_ANSINULLS
		sysconv(bit, d.status2 & 0x2000) AS is_ansi_padding_on,					-- DBR_ANSIPADDING
		sysconv(bit, d.status2 & 0x10000000) AS is_ansi_warnings_on,			-- DBR_ANSIWARNINGS
		sysconv(bit, d.status2 & 0x1000) AS is_arithabort_on,					-- DBR_ARITHABORT
		sysconv(bit, d.status2 & 0x10000) AS is_concat_null_yields_null_on,		-- DBR_CATNULL
		sysconv(bit, d.status2 & 0x800) AS is_numeric_roundabort_on,			-- DBR_NUMEABORT
		sysconv(bit, d.status2 & 0x800000) AS is_quoted_identifier_on,			-- DBR_QUOTEDIDENT
		sysconv(bit, d.status2 & 0x20000) AS is_recursive_triggers_on,			-- DBR_RECURTRIG
		sysconv(bit, d.status2 & 0x2000000) AS is_cursor_close_on_commit_on,	-- DBR_CURSCLOSEONCOM
		sysconv(bit, d.status2 & 0x100000) AS is_local_cursor_default,			-- DBR_DEFLOCALCURS
		sysconv(bit, d.status2 & 0x20000000) AS is_fulltext_enabled,			-- DBR_FTENABLED
		sysconv(bit, d.status2 & 0x200) AS is_trustworthy_on,				-- DBR_TRUSTWORTHY
		sysconv(bit, d.status2 & 0x400) AS is_db_chaining_on,				-- DBR_DBCHAINING
		sysconv(bit, d.status2 & 0x08000000) AS is_parameterization_forced,	-- DBR_UNIVERSALAUTOPARAM
		sysconv(bit, d.status2 & 64) AS is_master_key_encrypted_by_server,	-- DBR_MASTKEY
		sysconv(bit, d.status2 & 0x00000010) AS is_query_store_on,			-- DBR_QDSENABLED
		sysconv(bit, d.category & 1) AS is_published,
		sysconv(bit, d.category & 2) AS is_subscribed,
		sysconv(bit, d.category & 4) AS is_merge_published,
		sysconv(bit, d.category & 16) AS is_distributor,
		sysconv(bit, d.category & 32) AS is_sync_with_backup,
		d.svcbrkrguid AS service_broker_guid,
		sysconv(bit, case when d.scope = 0 then 1 else 0 end) AS is_broker_enabled,
		p.log_reuse_wait, lr.name AS log_reuse_wait_desc,
		sysconv(bit, d.status2 & 4) AS is_date_correlation_on, 		-- DBR_DATECORRELATIONOPT
		sysconv(bit, d.category & 64) AS is_cdc_enabled,
		case 
			when (d.id = db_id('tempdb')) then sysconv(bit, p.is_db_encrypted)
			else sysconv(bit, d.status2 & 0x100)					-- DBR_ENCRYPTION
			end AS is_encrypted,
		convert(bit, d.status2 & 0x8) AS is_honor_broker_priority_on,				-- DBR_HONORBRKPRI
		sgr.guid AS replica_id,
		sgr2.guid AS group_database_id,
		ssr.indepid AS resource_pool_id,
		default_language_lcid = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(smallint, p.default_language) else null end,
		default_language_name = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(sysname, sld.name) else null end,
		default_fulltext_language_lcid = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(int, p.default_fulltext_language) else null end,
		default_fulltext_language_name = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(sysname, slft.name) else null end,
		is_nested_triggers_on = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(bit, p.allow_nested_triggers) else null end,
		is_transform_noise_words_on = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(bit, p.transform_noise_words) else null end,
		two_digit_year_cutoff = case when ((d.status2 & 0x80000)=0x80000 AND p.containment = 1) then convert(smallint, p.two_digit_year_cutoff) else null end,
		containment = sysconv(tinyint, (d.status2 & 0x80000)/0x80000), -- DBR_IS_CDB
		containment_desc = convert(nvarchar(60), cdb.name),
		p.recovery_seconds AS target_recovery_time_in_seconds,
		p.delayed_durability,
		case when (p.delayed_durability = 0) then CAST('DISABLED' AS nvarchar(60)) -- LCOPT_DISABLED
			 when (p.delayed_durability = 1) then CAST('ALLOWED' AS nvarchar(60)) -- LCOPT_ALLOWED
			 when (p.delayed_durability = 2) then CAST('FORCED' AS nvarchar(60)) -- LCOPT_FORCED
			 else NULL
			 end AS delayed_durability_desc,
		convert(bit, d.status2 & 0x80) AS 
		is_memory_optimized_elevate_to_snapshot_on,				-- DBR_HKELEVATETOSNAPSHOT
		sysconv(bit, d.category & 0x100) AS is_federation_member,
		convert(bit, isnull(rda.value, 0)) AS is_remote_data_archive_enabled,
		convert(bit, p.is_mixed_page_allocation_on) AS is_mixed_page_allocation_on,
		sysconv(bit, p.is_temporal_history_retention_on) AS is_temporal_history_retention_enabled,
		case 
			when ((d.category & 0x200)=0x200) then 2
			when ((d.status2 & 0x80000)=0x80000) then 1
			else 0
			end AS catalog_collation_type,
		case when ((d.category & 0x200)=0x200) then CAST('SQL_Latin1_General_CP1_CI_AS' AS nvarchar(60)) 
			when ((d.status2 & 0x80000)=0x80000) then CAST('Latin1_General_100_CI_AS_KS_WS_SC' AS nvarchar(60))
			else CAST('DATABASE_DEFAULT' AS nvarchar(60))
			end AS catalog_collation_type_desc,
		p.physical_database_name,
	        sysconv(bit, d.status & 0x00004000) AS is_result_set_caching_on,
		p.is_accelerated_database_recovery_on, 
		p.is_scalable_tempdb_on AS is_tempdb_spill_to_remote_store,
		p.is_stale_page_detection_on,
	    p.is_memory_optimized_enabled,
		sysconv(bit, p.is_data_retention_on) AS is_data_retention_enabled,
		p.is_ledger_on,
		sysconv(bit, d.category & 0x800) AS is_change_feed_enabled
	FROM sys.sysdbreg$ d OUTER APPLY OpenRowset(TABLE DBPROP, (case when serverproperty('EngineEdition') = 5 then DB_ID() else d.id end)) p
	LEFT JOIN sys.syssingleobjrefs r ON r.depid = d.id AND r.class = 96 AND r.depsubid = 0	-- SRC_VIEWPOINTDB
	LEFT JOIN sys.syspalvalues st ON st.class = 'DBST' AND st.value = p.state
	LEFT JOIN sys.syspalvalues ua ON ua.class = 'DBUA' AND ua.value = p.user_access
	LEFT JOIN sys.syspalvalues si ON si.class = 'DBSI' AND si.value = p.snapshot_isolation_state
	LEFT JOIN sys.syspalvalues ro ON ro.class = 'DBRO' AND ro.value = p.recovery_model
	LEFT JOIN sys.syspalvalues pv ON pv.class = 'DBPV' AND pv.value = p.page_verify_option
	LEFT JOIN sys.syspalvalues lr ON lr.class = 'LRWT' AND lr.value = p.log_reuse_wait
	LEFT JOIN sys.syssingleobjrefs agdb ON agdb.depid = d.id AND agdb.class = 104 AND agdb.depsubid = 0	-- SRC_AVAILABILITYGROUP 
	LEFT JOIN master.sys.syssingleobjrefs ssr ON ssr.class = 108 AND ssr.depid = d.id -- SRC_RG_DB_TO_POOL
	LEFT JOIN master.sys.sysclsobjs  ag ON ag.id = agdb.indepid AND ag.class = 67 -- SOC_AVAILABILITY_GROUP
	LEFT JOIN master.sys.sysguidrefs sgr ON sgr.class = 8 AND sgr.id = ag.id AND sgr.subid = 1 -- GRC_AGGUID / AGGUID_REPLICA_ID
	LEFT JOIN master.sys.sysguidrefs sgr2 ON sgr2.class = 9 AND sgr2.id = ag.id AND sgr2.subid = d.id -- GRC_AGDBGUID
	LEFT JOIN sys.syspalvalues cdb ON cdb.class = 'DCDB' AND cdb.value = CASE WHEN (d.status2 & 0x80000)=0x80000 THEN 1 ELSE 0 END
	LEFT JOIN sys.syslanguages sld ON sld.lcid = p.default_language
	LEFT JOIN sys.fulltext_languages slft ON slft.lcid = p.default_fulltext_language
	LEFT JOIN sys.sysobjvalues coll ON coll.valclass = 102 AND coll.subobjid = 0 AND coll.objid = d.id	-- SVC_DATACOLLATION
	LEFT JOIN sys.sysobjvalues rda ON rda.valclass = 116 AND rda.objid = d.id AND rda.valnum = 0 -- SVC_STRETCH & STRETCH_DB_IS_STRETCHED
	WHERE d.id < 0x7fff AND repl_sys_db_visible(d.id) = 1
		AND (engineedition() <> 11 OR sys_db_visible_polaris(d.id) = 1)
		AND has_access('DB', (case when serverproperty('EngineEdition') = 5 then DB_ID() else d.id end)) = 1

