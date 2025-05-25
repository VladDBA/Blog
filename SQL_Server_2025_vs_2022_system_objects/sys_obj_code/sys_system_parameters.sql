SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.system_parameters AS
	SELECT object_id, name collate catalog_default AS name,
		parameter_id, system_type_id,
		user_type_id, max_length,
		precision, scale,
		is_output, is_cursor_ref,
		has_default_value, is_xml_document,
		default_value, xml_collection_id,
		is_readonly, is_nullable,
		encryption_type, encryption_type_desc, 
		encryption_algorithm_name, column_encryption_key_id,
		column_encryption_key_database_name collate catalog_default as column_encryption_key_database_name,
		vector_dimensions,
		vector_base_type,
		vector_base_type_desc
	FROM sys.system_parameters$
	WHERE number = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.system_parameters AS
	SELECT object_id, name collate catalog_default AS name,
		parameter_id, system_type_id,
		user_type_id, max_length,
		precision, scale,
		is_output, is_cursor_ref,
		has_default_value, is_xml_document,
		default_value, xml_collection_id,
		is_readonly, is_nullable,
		encryption_type, encryption_type_desc, 
		encryption_algorithm_name, column_encryption_key_id,
		column_encryption_key_database_name collate catalog_default as column_encryption_key_database_name
	FROM sys.system_parameters$
	WHERE number = 1

