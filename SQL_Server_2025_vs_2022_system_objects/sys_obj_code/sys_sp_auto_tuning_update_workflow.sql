SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

-- null value means we do not want to update the corresponding column in internal table
CREATE PROCEDURE sys.sp_auto_tuning_update_workflow
	@execution_id uniqueidentifier,
	@expected_state int,
	@new_state int=NULL,
	@retry_count int=NULL,
	@properties nvarchar(max)=NULL
AS
BEGIN
	exec sys.sp_auto_tuning_validate_executable;
	BEGIN TRAN tran_update_workflow;
	BEGIN TRY
	IF @execution_id IS NULL
		RAISERROR(15725, -1, 5, N'execution_id');

	IF @expected_state IS NULL
		RAISERROR(15725, -1, 6, N'wf_expected_state');

	DECLARE @return_status int;
	DECLARE @sql_string nvarchar(max);
	SET @sql_string = N'
	IF EXISTS(SELECT current_state FROM sys.ats_workflow_fsm WHERE execution_id=@execution_id_in AND current_state=@expected_state_in)
	BEGIN
		UPDATE sys.ats_workflow_fsm
		SET current_state=ISNULL(@new_state_in, current_state),
			last_update_date_utc=GETUTCDATE(),
			retry_count=ISNULL(@retry_count_in, retry_count),
			properties=ISNULL(@properties_in, properties)
		WHERE execution_id=@execution_id_in;
	END
	ELSE
	BEGIN
		DECLARE @message_error nvarchar(4000) = ''Workflow ID:''+ISNULL(CONVERT(nvarchar(128), @execution_id_in), ''null'')+'' does not exist or state: ''+ISNULL(CONVERT(nvarchar(128), @expected_state_in), ''null'') + '' does not exists!'';
		RAISERROR(15726, -1, 3, @message_error);
	END
	';
	DECLARE @params_def nvarchar(4000);
	SET @params_def = N'@execution_id_in uniqueidentifier,
	@expected_state_in int,
	@new_state_in int,
	@retry_count_in int,
	@properties_in nvarchar(max)
	';
	EXEC @return_status = sp_executesql @sql_string,
		@params_def,
		@execution_id_in=@execution_id,
		@expected_state_in=@expected_state,
		@new_state_in=@new_state,
		@retry_count_in=@retry_count,
		@properties_in=@properties
		;
	IF @return_status != 0
	BEGIN
		ROLLBACK TRAN tran_update_workflow;
		RAISERROR(15723, -1, 6, @return_status);
	END
	ELSE
		COMMIT TRAN tran_update_workflow;
	RETURN 0
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN tran_update_workflow;
		THROW;
	END CATCH
END

