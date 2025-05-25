use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

CREATE PROC sys.sp_flush_commit_table (@flush_ts BIGINT, @cleanup_version BIGINT = NULL) AS
BEGIN
	DECLARE @cleanup_ts BIGINT
	SET		@cleanup_ts = change_tracking_hardened_cleanup_version ()

	IF @cleanup_ts > @cleanup_version
		SET @cleanup_ts = @cleanup_version

	DECLARE @start_ts BIGINT
	SET		@start_ts = (SELECT MAX(commit_ts) FROM sys.syscommittab)

	IF @start_ts IS NULL OR @start_ts < @cleanup_ts
		SET @start_ts = @cleanup_ts

	BEGIN TRY

		INSERT	sys.syscommittab
		SELECT	*
		FROM	OpenRowset (table SYSCOMMITTABLE, db_id (), @start_ts, 1)
		WHERE	commit_ts <= @flush_ts

	END TRY
	BEGIN CATCH
		DECLARE @error INT
		DECLARE @errorText NVARCHAR(4000)
		SELECT @error = ERROR_NUMBER()
		SELECT @errorText = 'Syscommittable flush error: ' + ERROR_MESSAGE()

		-- fire cleanup event
		DECLARE @objId INT = object_id('sys.syscommittab')
		EXEC sys.sp_ct_fire_cleanup_event @objId, 32 /* state : Error */, 24 /* FailedToFlushDuplicateRecords */, @errorText

		-- bug 1841405: In case the insert operation fails with error code 2601 due to the duplicate records issue,
		-- we try to insert only the records which don't already exist in the syscommittab table from the SYSCOMMITTABLE
		DECLARE @is_ct_retry_insert_during_flush_enabled INT
		EXEC @is_ct_retry_insert_during_flush_enabled = sys.sp_is_featureswitch_enabled N'ChangeTrackingRetryUniqueRowInsertDuringFlush';
		IF (@error = 2601 and @is_ct_retry_insert_during_flush_enabled = 1)
		BEGIN
			--RANK ensures that duplicates already present in in-memory syscommittab are not
			--attempted to insert into the on-disk syscommittab
			DECLARE @StartTime DATETIME2, @EndTime DATETIME2;
			DECLARE @Duration NVARCHAR(4000);

			SET @StartTime = SYSDATETIME();

			INSERT INTO sys.syscommittab(commit_ts, xdes_id, commit_lbn, commit_csn, commit_time, dbfragid)
			SELECT commit_ts, xdes_id, commit_lbn, commit_csn, commit_time, dbfragid
			FROM (
				SELECT *, RANK() OVER (PARTITION BY xdes_id ORDER BY commit_ts DESC) AS commit_ts_rank
				FROM OpenRowset (table SYSCOMMITTABLE, db_id(), @start_ts, 1)
				WHERE commit_ts <= @flush_ts
			) AS subquery
			WHERE commit_ts_rank = 1 AND xdes_id NOT IN (SELECT xdes_id FROM sys.syscommittab)

			SET @EndTime = SYSDATETIME();

			-- Calculate the duration in seconds for the insert query
			SET @Duration = 'Syscommittab Insert Operation in the CATCH block took ' + 
				CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS NVARCHAR(20)) + 
				' seconds.'
			-- fire cleanup event to emit time
			EXEC sys.sp_ct_fire_cleanup_event @objId, 0 /* state : none */, 26 /* DuplicateCommitTableCheckpointLatency */, @Duration

		END
		ELSE
			THROW
	END CATCH

END


/*====  SQL Server 2022 version  ====*/

CREATE PROC sys.sp_flush_commit_table (@flush_ts BIGINT, @cleanup_version BIGINT = NULL) AS
BEGIN
	DECLARE @cleanup_ts BIGINT
	SET		@cleanup_ts = change_tracking_hardened_cleanup_version ()

	IF @cleanup_ts > @cleanup_version
		SET @cleanup_ts = @cleanup_version

	DECLARE @start_ts BIGINT
	SET		@start_ts = (SELECT MAX(commit_ts) FROM sys.syscommittab)

	IF @start_ts IS NULL OR @start_ts < @cleanup_ts
		SET @start_ts = @cleanup_ts

	BEGIN TRY

		INSERT	sys.syscommittab
		SELECT	*
		FROM	OpenRowset (table SYSCOMMITTABLE, db_id (), @start_ts, 1)
		WHERE	commit_ts <= @flush_ts

	END TRY
	BEGIN CATCH
		DECLARE @error INT
		DECLARE @errorText NVARCHAR(MAX)
		SELECT @error = ERROR_NUMBER()
		SELECT @errorText = 'Syscommittable flush error: ' + ERROR_MESSAGE()

		-- fire cleanup event
		DECLARE @objId INT = object_id('sys.syscommittab')
		EXEC sys.sp_ct_fire_cleanup_event @objId, 32 /* state : Error */, 24 /* FailedToFlushDuplicateRecords */, @errorText

		-- bug 1841405: In case the insert operation fails with error code 2601 due to the duplicate records issue,
		-- we try to insert only the records which don't already exist in the syscommittab table from the SYSCOMMITTABLE
		DECLARE @is_ct_retry_insert_during_flush_enabled INT
		EXEC @is_ct_retry_insert_during_flush_enabled = sys.sp_is_featureswitch_enabled N'ChangeTrackingRetryUniqueRowInsertDuringFlush';
		IF (@error = 2601 and @is_ct_retry_insert_during_flush_enabled = 1)
		BEGIN
			INSERT	sys.syscommittab
			SELECT	*
			FROM	OpenRowset (table SYSCOMMITTABLE, db_id (), @start_ts, 1)
			WHERE	commit_ts <= @flush_ts AND xdes_id NOT IN (SELECT xdes_id FROM sys.syscommittab)
		END
		ELSE
			THROW
	END CATCH

END

