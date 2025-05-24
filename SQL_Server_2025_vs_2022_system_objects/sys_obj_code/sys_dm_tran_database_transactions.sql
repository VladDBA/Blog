SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_tran_database_transactions AS
	SELECT
		transaction_id,
		database_id,
		database_transaction_begin_time,
		database_transaction_type,
		database_transaction_state,
		database_transaction_status,
		database_transaction_status2,
		database_transaction_log_record_count,
		database_transaction_replicate_record_count,
		database_transaction_log_bytes_used,
		database_transaction_log_bytes_reserved,
		database_transaction_log_bytes_used_system,
		database_transaction_log_bytes_reserved_system,
		database_transaction_begin_lsn,
		database_transaction_last_lsn,
		database_transaction_most_recent_savepoint_lsn,
		database_transaction_commit_lsn,
		database_transaction_last_rollback_lsn,
		database_transaction_next_undo_lsn,
		database_transaction_first_repl_lsn
	FROM OpenRowset(TABLE DATABASE_TRANSACTIONS)


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_tran_database_transactions AS
	SELECT
		transaction_id,
		database_id,
		database_transaction_begin_time,
		database_transaction_type,
		database_transaction_state,
		database_transaction_status,
		database_transaction_status2,
		database_transaction_log_record_count,
		database_transaction_replicate_record_count,
		database_transaction_log_bytes_used,
		database_transaction_log_bytes_reserved,
		database_transaction_log_bytes_used_system,
		database_transaction_log_bytes_reserved_system,
		database_transaction_begin_lsn,
		database_transaction_last_lsn,
		database_transaction_most_recent_savepoint_lsn,
		database_transaction_commit_lsn,
		database_transaction_last_rollback_lsn,
		database_transaction_next_undo_lsn
	FROM OpenRowset(TABLE DATABASE_TRANSACTIONS)

