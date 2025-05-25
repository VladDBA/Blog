use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_auto_tuning_create_workflow
	@execution_id uniqueidentifier,
	@workflow_type_id int,
	@recommendation_id bigint,
	@properties nvarchar(max),
	@derived_from_id uniqueidentifier = NULL,
	@initial_state int = 0
AS
BEGIN
	exec sys.sp_auto_tuning_validate_executable;

	BEGIN TRAN tran_create_workflow;
	BEGIN TRY
	IF @execution_id IS NULL
		RAISERROR(15725, -1, 1, N'execution_id');

	IF @workflow_type_id IS NULL
		RAISERROR(15725, -1, 2, N'workflow_type_id');

	IF @properties IS NULL
		RAISERROR(15725, -1, 3, N'properties');

	IF @initial_state IS NULL
		RAISERROR(15725, -1, 4, N'initial_state');

	DECLARE @return_status int;
	DECLARE @sql_string nvarchar(max);
	SET @sql_string = N'
		DECLARE @cur_time_utc DATETIME = GETUTCDATE();
		IF @recommendation_id_in IS NOT NULL
		BEGIN
			IF EXISTS(SELECT id FROM sys.ats_recommendations WHERE id=@recommendation_id_in)
			BEGIN
				INSERT INTO sys.ats_workflow_recommendation_relation(execution_id, recommendation_id)
				VALUES(@execution_id_in, @recommendation_id_in);
			END
			ELSE
			BEGIN
				RAISERROR(15726, -1, 1, N''recommendation_id does not exist!'');
			END
		END

		INSERT INTO sys.ats_workflow_fsm(execution_id, current_state, create_date_utc, last_update_date_utc, workflow_type_id, retry_count, derived_from_id, properties)
		VALUES(@execution_id_in, @initial_state_in, @cur_time_utc, @cur_time_utc, @workflow_type_id_in, 0, @derived_from_id_in, @properties_in);
	';

	DECLARE @params_def nvarchar(4000);
	SET @params_def = N'
	@execution_id_in uniqueidentifier,
	@workflow_type_id_in int,
	@recommendation_id_in bigint,
	@properties_in nvarchar(max),
	@derived_from_id_in uniqueidentifier,
	@initial_state_in int
	';
	EXEC @return_status = sp_executesql @sql_string,
		@params_def,
		@execution_id_in=@execution_id,
		@workflow_type_id_in=@workflow_type_id,
		@recommendation_id_in=@recommendation_id,
		@properties_in=@properties,
		@derived_from_id_in=@derived_from_id,
		@initial_state_in=@initial_state;
	IF @return_status != 0
	BEGIN
		ROLLBACK TRAN tran_create_workflow;
		RAISERROR(15723, -1, 4, @return_status);
	END
	ELSE
		COMMIT TRAN tran_create_workflow;
	RETURN 0
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN tran_create_workflow;
		THROW;
	END CATCH
END

