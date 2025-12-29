/*create database*/
USE [master]
GO
IF EXISTS (SELECT 1
           FROM   sys.[databases]
           WHERE  name = N'oh_my_heap')
  DROP DATABASE [oh_my_heap];
GO
CREATE DATABASE [oh_my_heap];
GO
ALTER DATABASE [oh_my_heap] SET RECOVERY SIMPLE;
GO
/*pre-grow the data and t-log files to avoid wasting time with this further down the line*/
ALTER DATABASE [oh_my_heap] MODIFY FILE ( NAME = N'oh_my_heap', SIZE = 5242880KB );
GO
ALTER DATABASE [oh_my_heap] MODIFY FILE ( NAME = N'oh_my_heap_log', SIZE = 2097152KB );
GO
USE [oh_my_heap];
GO

/*create tables*/
IF OBJECT_ID(N'active_cluster', N'U') IS NOT NULL
  BEGIN
      DROP TABLE [active_cluster];
  END;
GO
CREATE TABLE [active_cluster]
  (
     [id]              INT NOT NULL IDENTITY(1, 1),
     [short_nvarchar]  NVARCHAR(50),
     [medium_nvarchar] NVARCHAR(240),
     [long_nvarchar]   NVARCHAR(400),
     [time_stamp]      DATETIME DEFAULT GETDATE(),
     [is_full]         BIT NOT NULL DEFAULT 0
  );
GO

IF OBJECT_ID(N'active_heap', N'U') IS NOT NULL
  BEGIN
      DROP TABLE [active_heap];
  END;
GO
CREATE TABLE [active_heap]
  (
     [id]              INT NOT NULL IDENTITY(1, 1),
     [short_nvarchar]  NVARCHAR(50),
     [medium_nvarchar] NVARCHAR(240),
     [long_nvarchar]   NVARCHAR(400),
     [time_stamp]      DATETIME DEFAULT GETDATE(),
     [is_full]         BIT NOT NULL DEFAULT 0
  );
GO

/*populate heap*/
INSERT INTO [active_heap] WITH(TABLOCK) ([short_nvarchar])
SELECT TOP(1000000) NULL
FROM sys.all_columns c
CROSS APPLY sys.all_columns c1
CROSS APPLY sys.all_columns c2
OPTION (MAXDOP 0 );
GO
/*create index*/
CREATE INDEX [ix_active_heap] ON [active_heap]([id]) 
WITH (ONLINE=OFF, MAXDOP=0);
GO

/*populate clustered index*/
INSERT INTO [active_cluster] WITH(TABLOCK) ([short_nvarchar])
SELECT TOP(1000000) NULL
FROM sys.all_columns c
CROSS APPLY sys.all_columns c1
CROSS APPLY sys.all_columns c2
OPTION (MAXDOP 0);
GO
/*create clustered index*/
CREATE UNIQUE CLUSTERED INDEX [cix_active_cluster] ON [active_cluster]([id]) 
WITH (ONLINE=OFF, MAXDOP=0);
GO