SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_predict_next_activity (
    @retention_interval_days INT,
	@probability_threshold INT,
	@window_size_sec INT,
	@window_slide_sec INT,
	@now BIGINT,
	@start_of_predicted_activity BIGINT OUTPUT,
	@end_of_predicted_activity BIGINT OUTPUT,
	@confidence BIGINT OUTPUT,
	@is_db_old BIGINT OUTPUT)
AS
BEGIN
	DECLARE @min_timestamp BIGINT = 0
	DECLARE @history_start BIGINT = @now - @retention_interval_days * 24 * 60 * 60
	DECLARE @prediction_horizon BIGINT = @now + 24 * 60 * 60 
	DECLARE @windows_with_resumes FLOAT = 0
	DECLARE @probability_of_resume INT = 0
	DECLARE @previous_day INT = 0
	DECLARE @window_start BIGINT = @now
	DECLARE @window_start_previous_day BIGINT = 0
	DECLARE @window_end_previous_day BIGINT = 0
	DECLARE @first_resume BIGINT = 0
	DECLARE @last_resume BIGINT = 0
	DECLARE @earliest_resume_per_window BIGINT = 0
	DECLARE @latest_resume_per_window BIGINT = 0
	DECLARE @previous_start BIGINT = 0
	SET @start_of_predicted_activity = 0
	SET @end_of_predicted_activity = 0
	SET @confidence = 0
	SET @is_db_old = 0
	IF (@retention_interval_days = 0)
		RETURN 0
	SELECT @min_timestamp = MIN(time_snapshot)
	FROM sys.pause_resume_history
	IF (@min_timestamp < @history_start)
	BEGIN
		SET @is_db_old = 1
		DELETE FROM sys.pause_resume_history
		WHERE @min_timestamp < time_snapshot AND time_snapshot < @history_start		
	END
	WHILE (@window_start + @window_size_sec <= @prediction_horizon)
	BEGIN
		SET @windows_with_resumes = 0
		SET @earliest_resume_per_window = @window_size_sec
		SET @latest_resume_per_window = 0
		SET @previous_day = 1
		WHILE (@previous_day <= @retention_interval_days)
		BEGIN
			SET @window_start_previous_day = @window_start - @previous_day * 24 * 60 * 60
			SET @window_end_previous_day = @window_start_previous_day + @window_size_sec
			SELECT @first_resume = MIN(time_snapshot), @last_resume = MAX(time_snapshot)
			FROM sys.pause_resume_history
			WHERE event_type = 2 AND @window_start_previous_day <= time_snapshot AND time_snapshot <= @window_end_previous_day
			IF (@first_resume is NOT NULL)
			BEGIN
				IF (@earliest_resume_per_window > (@first_resume - @window_start_previous_day))
					SET @earliest_resume_per_window = (@first_resume - @window_start_previous_day)
				IF (@latest_resume_per_window < (@last_resume - @window_start_previous_day))
					SET @latest_resume_per_window = (@last_resume - @window_start_previous_day)
				SET @windows_with_resumes = @windows_with_resumes + 1
			END
			SET @previous_day = @previous_day + 1
		END
		SET @probability_of_resume = (@windows_with_resumes / @retention_interval_days) * 100
		IF (@probability_threshold <= @probability_of_resume AND @confidence < @probability_of_resume)
		BEGIN
			IF (@previous_start = 0 OR @previous_start = @start_of_predicted_activity)
			BEGIN
				SET @start_of_predicted_activity = @window_start + @earliest_resume_per_window
				SET @previous_start = @start_of_predicted_activity
				SET @end_of_predicted_activity = @window_start + @latest_resume_per_window
				SET @confidence = @probability_of_resume
			END
			ELSE 
				BREAK
		END
		SET @window_start = @window_start + @window_slide_sec
	END	
END

