SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

CREATE FUNCTION sys.fn_get_audit_file (
	@file_pattern nvarchar(260),
	@initial_file_name nvarchar(260) = NULL,
	@audit_record_offset bigint = NULL)
RETURNS table
AS
RETURN
(
	SELECT *
	FROM sys.fn_get_audit_file_v2(
		@file_pattern,
		@initial_file_name,
		@audit_record_offset,
		default,
		default)
)


/*====  SQL Server 2022 version  ====*/
CREATE FUNCTION sys.fn_get_audit_file (
	@file_pattern nvarchar(260),
	@initial_file_name nvarchar(260) = NULL,
	@audit_record_offset bigint = NULL)
RETURNS table
AS
	RETURN SELECT
		event_time,
		sequence_number,
		action_id,
		succeeded,
		permission_bitmask,
		is_column_permission,
		session_id,
		server_principal_id,
		database_principal_id,
		target_server_principal_id,
		target_database_principal_id,
		object_id,
		class_type,
		session_server_principal_name,
		server_principal_name,
		server_principal_sid,
		database_principal_name,
		target_server_principal_name,
		target_server_principal_sid,
		target_database_principal_name,
		server_instance_name,
		database_name,
		schema_name,
		object_name,
		statement,
		additional_information,
		file_name,
		audit_file_offset,
		user_defined_event_id,
		user_defined_information,
		audit_schema_version,
		sequence_group_id,
		transaction_id,
		client_ip,
		application_name,
		duration_milliseconds,
		response_rows,
		affected_rows,
		connection_id,
		data_sensitivity_information,
		host_name,
		session_context,
		client_tls_version,
		client_tls_version_name,
		database_transaction_id,
		ledger_start_sequence_number,
		external_policy_permissions_checked,
		obo_middle_tier_app_id,
		is_local_secondary_replica
	FROM OpenRowSet(TABLE FN_GET_AUDIT_FILE, @file_pattern, @initial_file_name, @audit_record_offset)

