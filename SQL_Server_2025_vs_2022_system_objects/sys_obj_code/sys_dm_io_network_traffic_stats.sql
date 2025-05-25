use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_io_network_traffic_stats AS
 	SELECT	snapshot_time,
			network_protocol,
			network_protocol_desc,
			count_sends,
			count_receives,
			send_bytes,
			receive_bytes,
			max_send_bytes,
			max_receive_bytes,
			min_send_bytes,
			min_receive_bytes
	FROM OpenRowSet(TABLE DM_IO_NETWORK_TRAFFIC_STATS)

