SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.system_internals_partition_columns AS
	SELECT c.rsid AS partition_id,
		c.rscolid AS partition_column_id,
		c.rcmodified AS modified_count,
		case c.maxinrowlen when 0 then p.length
			else c.maxinrowlen end AS max_inrow_length,
		convert(bit, c.status & 1) AS is_replicated,				--RSC_REPLICATED
		convert(bit, c.status & 4) AS is_logged_for_replication,	--RSC_LOG_FOR_REPL
		convert(bit, c.status & 2) AS is_dropped,				--RSC_DROPPED
		p.xtype AS system_type_id,
		p.length AS max_length,
		p.prec AS precision,
		p.scale AS scale,
		convert(sysname, CollationPropertyFromId(c.cid, 'name')) AS collation_name,
		convert(bit, c.status & 32) AS is_filestream,			--RSC_FILESTREAM
		c.ordkey AS key_ordinal,
		convert(bit, 1 - (c.status & 128)/128) AS is_nullable,		-- RSC_NOTNULL		
		convert(bit, c.status & 8) AS is_descending_key,		--RSC_DESC_KEY
		convert(bit, c.status & 16) AS is_uniqueifier,			--RSC_UNIQUIFIER
		convert(smallint, convert(binary(2), c.offset & 0xffff)) AS leaf_offset,
		convert(smallint, substring(convert(binary(4), c.offset), 1, 2)) AS internal_offset,
		convert(tinyint, c.bitpos & 0xff) AS leaf_bit_position,
		convert(tinyint, c.bitpos/0x100) AS internal_bit_position,
		convert(smallint, convert(binary(2), c.nullbit & 0xffff)) AS leaf_null_bit,
		convert(smallint, substring(convert(binary(4), c.nullbit), 1, 2)) AS internal_null_bit,
		convert(bit, c.status & 64) AS is_anti_matter,			--RSC_ANTIMATTER
		convert(uniqueidentifier, c.colguid) AS partition_column_guid,
		sysconv(bit, c.status & 0x00000100) AS is_sparse,	--RSC_SPARSE
		sysconv(bit, case when ov.value is NULL then 0 else 1 end) AS has_default,
		ov.value AS default_value,
		c.hbcolid as hobt_column_id,
		sysconv(bit, c.status & 0x00000400) AS is_csilocator,	--RSC_CSILOCATOR
		sysconv(bit, c.status & 0x00001000) AS is_added_with_skip_segments	--RSC_ADDED_COL_SKIP_SEG_GEN
	FROM
		sys.sysrscols c OUTER APPLY
		OpenRowset(TABLE RSCPROP, c.ti) p LEFT OUTER JOIN -- It simply decodes ti into individual properties
		sys.sysseobjvalues ov ON
		(
			ov.valclass = 1 AND -- SEVC_OUT_OF_ROW_DEFAULT
			ov.id = c.rsid AND
			ov.subid = c.hbcolid AND
			ov.valnum = 0 -- SE_VALNUM_DEFAULT 
		)
	JOIN sys.sysrowsets rs ON c.rsid = rs.rowsetid


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.system_internals_partition_columns AS
	SELECT c.rsid AS partition_id,
		c.rscolid AS partition_column_id,
		c.rcmodified AS modified_count,
		case c.maxinrowlen when 0 then p.length
			else c.maxinrowlen end AS max_inrow_length,
		convert(bit, c.status & 1) AS is_replicated,				--RSC_REPLICATED
		convert(bit, c.status & 4) AS is_logged_for_replication,	--RSC_LOG_FOR_REPL
		convert(bit, c.status & 2) AS is_dropped,				--RSC_DROPPED
		p.xtype AS system_type_id,
		p.length AS max_length,
		p.prec AS precision,
		p.scale AS scale,
		convert(sysname, CollationPropertyFromId(c.cid, 'name')) AS collation_name,
		convert(bit, c.status & 32) AS is_filestream,			--RSC_FILESTREAM
		c.ordkey AS key_ordinal,
		convert(bit, 1 - (c.status & 128)/128) AS is_nullable,		-- RSC_NOTNULL		
		convert(bit, c.status & 8) AS is_descending_key,		--RSC_DESC_KEY
		convert(bit, c.status & 16) AS is_uniqueifier,			--RSC_UNIQUIFIER
		convert(smallint, convert(binary(2), c.offset & 0xffff)) AS leaf_offset,
		convert(smallint, substring(convert(binary(4), c.offset), 1, 2)) AS internal_offset,
		convert(tinyint, c.bitpos & 0xff) AS leaf_bit_position,
		convert(tinyint, c.bitpos/0x100) AS internal_bit_position,
		convert(smallint, convert(binary(2), c.nullbit & 0xffff)) AS leaf_null_bit,
		convert(smallint, substring(convert(binary(4), c.nullbit), 1, 2)) AS internal_null_bit,
		convert(bit, c.status & 64) AS is_anti_matter,			--RSC_ANTIMATTER
		convert(uniqueidentifier, c.colguid) AS partition_column_guid,
		sysconv(bit, c.status & 0x00000100) AS is_sparse,	--RSC_SPARSE
		sysconv(bit, case when ov.value is NULL then 0 else 1 end) AS has_default,
		ov.value AS default_value
	FROM
		sys.sysrscols c OUTER APPLY
		OpenRowset(TABLE RSCPROP, c.ti) p LEFT OUTER JOIN -- It simply decodes ti into individual properties
		sys.sysseobjvalues ov ON
		(
			ov.valclass = 1 AND -- SEVC_OUT_OF_ROW_DEFAULT
			ov.id = c.rsid AND
			ov.subid = c.hbcolid AND
			ov.valnum = 0 -- SE_VALNUM_DEFAULT 
		)
	JOIN sys.sysrowsets rs ON c.rsid = rs.rowsetid

