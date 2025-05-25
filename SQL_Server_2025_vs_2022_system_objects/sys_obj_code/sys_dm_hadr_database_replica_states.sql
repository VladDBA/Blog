use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE view sys.dm_hadr_database_replica_states
as
	SELECT
		drs.database_id as database_id,
		drs.group_id as group_id,
		drs.replica_id as replica_id,
		drs.group_database_id as group_database_id,
		drs.is_local as is_local,
		drs.is_primary_replica as is_primary_replica,
		drs.synchronization_state as synchronization_state,
		drs.synchronization_state_desc as synchronization_state_desc,
		drs.is_commit_participant as is_commit_participant,
		drs.synchronization_health as synchronization_health,
		drs.synchronization_health_desc as synchronization_health_desc,
		drs.database_state as database_state,
		st.name as database_state_desc,
		drs.is_suspended as is_suspended,
		drs.suspend_reason as suspend_reason,
		drs.suspend_reason_desc as suspend_reason_desc,
		drs.recovery_lsn as recovery_lsn,
		drs.truncation_lsn as truncation_lsn,
		drs.last_sent_lsn as last_sent_lsn,
		drs.last_sent_time as last_sent_time,
		drs.last_received_lsn as last_received_lsn,
		drs.last_received_time as last_received_time,
		drs.last_hardened_lsn as last_hardened_lsn,
		drs.last_hardened_time as last_hardened_time,
		drs.last_redone_lsn as last_redone_lsn,
		drs.last_redone_time as last_redone_time,
		drs.log_send_queue_size as log_send_queue_size,
		drs.log_send_rate as log_send_rate,
		drs.redo_queue_size as redo_queue_size,
		drs.redo_rate as redo_rate,
		drs.filestream_send_rate as filestream_send_rate,
		drs.end_of_log_lsn as end_of_log_lsn,
		drs.last_commit_lsn as last_commit_lsn,
		drs.last_commit_time as last_commit_time,
		drs.ghost_cleanup_fence as low_water_mark_for_ghosts,
		drs.secondary_lag_seconds as secondary_lag_seconds,
		drs.quorum_commit_lsn as quorum_commit_lsn,
		drs.quorum_commit_time as quorum_commit_time,
		drs.is_internal as is_internal
	FROM OpenRowset(TABLE DM_HADR_DATABASE_REPLICA_STATES) drs
		LEFT JOIN sys.syspalvalues st ON st.class = 'DBST' AND st.value = drs.database_state


/*====  SQL Server 2022 version  ====*/
CREATE view sys.dm_hadr_database_replica_states
as
	SELECT
		drs.database_id as database_id,
		drs.group_id as group_id,
		drs.replica_id as replica_id,
		drs.group_database_id as group_database_id,
		drs.is_local as is_local,
		drs.is_primary_replica as is_primary_replica,
		drs.synchronization_state as synchronization_state,
		drs.synchronization_state_desc as synchronization_state_desc,
		drs.is_commit_participant as is_commit_participant,
		drs.synchronization_health as synchronization_health,
		drs.synchronization_health_desc as synchronization_health_desc,
		drs.database_state as database_state,
		st.name as database_state_desc,
		drs.is_suspended as is_suspended,
		drs.suspend_reason as suspend_reason,
		drs.suspend_reason_desc as suspend_reason_desc,
		drs.recovery_lsn as recovery_lsn,
		drs.truncation_lsn as truncation_lsn,
		drs.last_sent_lsn as last_sent_lsn,
		drs.last_sent_time as last_sent_time,
		drs.last_received_lsn as last_received_lsn,
		drs.last_received_time as last_received_time,
		drs.last_hardened_lsn as last_hardened_lsn,
		drs.last_hardened_time as last_hardened_time,
		drs.last_redone_lsn as last_redone_lsn,
		drs.last_redone_time as last_redone_time,
		drs.log_send_queue_size as log_send_queue_size,
		drs.log_send_rate as log_send_rate,
		drs.redo_queue_size as redo_queue_size,
		drs.redo_rate as redo_rate,
		drs.filestream_send_rate as filestream_send_rate,
		drs.end_of_log_lsn as end_of_log_lsn,
		drs.last_commit_lsn as last_commit_lsn,
		drs.last_commit_time as last_commit_time,
		drs.ghost_cleanup_fence as low_water_mark_for_ghosts,
		drs.secondary_lag_seconds as secondary_lag_seconds,
		drs.quorum_commit_lsn as quorum_commit_lsn,
		drs.quorum_commit_time as quorum_commit_time
	FROM OpenRowset(TABLE DM_HADR_DATABASE_REPLICA_STATES) drs
		LEFT JOIN sys.syspalvalues st ON st.class = 'DBST' AND st.value = drs.database_state

