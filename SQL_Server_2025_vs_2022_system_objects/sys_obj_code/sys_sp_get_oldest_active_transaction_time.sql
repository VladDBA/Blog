use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE PROCEDURE sys.sp_get_oldest_active_transaction_time
AS
BEGIN
	SELECT min(transaction_begin_time) as min_transaction_begin_time, CURRENT_TIMESTAMP as sql_current_time FROM sys.dm_tran_active_transactions
	WHERE transaction_state=2 AND transaction_id in (SELECT transaction_id FROM sys.dm_tran_session_transactions)
END

