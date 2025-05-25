SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.sysexternal_file_formats_2016 AS
	SELECT
		seff.file_format_id AS file_format_id,
		seff.name AS name,
		seff.format_type AS format_type,
		seff.field_terminator AS field_terminator,
		seff.string_delimiter AS string_delimiter,
		seff.date_format AS date_format,
		sysconv(bit, seff.use_type_default & 0x1) AS use_type_default,
		seff.serde_method AS serde_method,
		seff.row_terminator AS row_terminator,
		seff.encoding AS encoding,
		seff.data_compression AS data_compression
	FROM sys.sysextfileformats seff
	WHERE has_access('EF', DB_ID()) = 1 -- catalog security check

