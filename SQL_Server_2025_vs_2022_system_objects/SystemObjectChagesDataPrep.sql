/*
         Description:  This script system object data between two versions of SQL Server (2025 and 2022 in this case).
         It needs to be executed on the 2022 instance and it requires a linked server connection (currently [WINSRV2K25\SQL2025]) to the 2025 instance
         Ctrl+H to find and replace the [WINSRV2K25\SQL2025] linked server connection with your own and then run.
         Author: Vlad Drumea
         Create date: 2025-05-25         
         Website: https://vladdba.com
         From: https://github.com/VladDBA/Blog/tree/main/SQL_Server_2025_vs_2022_system_objects/
   
*/

IF DB_ID('data_mining_2025') IS NULL
  CREATE DATABASE [data_mining_2025];
GO
USE [data_mining_2025]
GO
IF OBJECT_ID('dbo.diff_system_objects') IS NOT NULL
  DROP TABLE [dbo].[diff_system_objects];
GO
CREATE TABLE [dbo].[diff_system_objects](
	[object_id] INT NOT NULL,
	[name] NVARCHAR(128) NOT NULL,
	[type] CHAR(2) NOT NULL,
	[schema_id] INT NOT NULL,
	[definition] NVARCHAR(max) NULL,
	[uses_ansi_nulls] BIT NULL,
	[uses_quoted_identifier] BIT NULL,
	[new] BIT NULL,
	[modified] BIT NULL,
    [just_obj_id_changed] BIT NULL,
	[2022_object_id] INT NULL,
	[2022_definition] NVARCHAR(MAX) NULL,
    [character_count_change] INT,
    [line_count_change] INT);
GO

IF OBJECT_ID('dbo.diff_system_columns') IS NOT NULL
  DROP TABLE [dbo].[diff_system_columns];
GO
CREATE TABLE [dbo].[diff_system_columns](
	[object_id] [int] NULL,
	[schema_id] [int] NOT NULL,
	[table_name] [sysname] NOT NULL,
	[column_id] [int] NOT NULL,
	[column_name] [nvarchar](128) NULL,
	[data_type] [sysname] NOT NULL,
	[object_type] [char](2) NULL,
    [2022_object_id] INT
);
GO

;WITH a
     AS (SELECT [o].[id],
                [o].[name],
                [o].[type],
                [o].[uid],
                [m].[definition],
                [m].[uses_ansi_nulls],
                [m].[uses_quoted_identifier]
         FROM   [WINSRV2K25\SQL2025].[master].[sys].[sysobjects] AS [o]
                LEFT JOIN [WINSRV2K25\SQL2025].[master].[sys].[system_sql_modules] AS [m]
                        ON [m].[object_id] = [o].[id]
                        WHERE [o].[type] NOT IN( 'U', 'PK' ,'K')
         EXCEPT
         SELECT [o].[id],
                [o].[name],
                [o].[type],
                [o].[uid],
                [m].[definition],
                [m].[uses_ansi_nulls],
                [m].[uses_quoted_identifier]
         FROM   [master].[sys].[sysobjects] AS [o]
                LEFT JOIN [master].[sys].[system_sql_modules] AS [m]
                        ON [m].[object_id] = [o].[id]
                        WHERE [o].[type] NOT IN( 'U', 'PK' ,'K'))
INSERT INTO [diff_system_objects] ([object_id],[name],[type],[schema_id],[definition],[uses_ansi_nulls],[uses_quoted_identifier])
SELECT [id],[name],[type],[uid],[definition],[uses_ansi_nulls],[uses_quoted_identifier] FROM   a;

/*mark the ones that are actually new and not just code and/or object_id changes*/
;
WITH b
     AS (SELECT [o].[name],
                [o].[type],
                [uid]
          FROM   [WINSRV2K25\SQL2025].[master].[sys].[sysobjects] AS [o]
                LEFT JOIN [master].[sys].[system_sql_modules] AS [m]
                        ON [m].[object_id] = [o].[id]
                        WHERE [o].[type] NOT IN( 'U', 'PK' ,'K')
         EXCEPT
         SELECT [o].[name],
                [o].[type],
                [uid]
          FROM   [master].[sys].[sysobjects] AS [o]
                LEFT JOIN [master].[sys].[system_sql_modules] AS [m]
                        ON [m].[object_id] = [o].[id]
                        WHERE [o].[type] NOT IN( 'U', 'PK' ,'K'))
UPDATE [dm]
SET    dm.[new] = 1,
[dm].[just_obj_id_changed] = 0,
[dm].[modified] = 0
FROM   [diff_system_objects] AS [dm]
       INNER JOIN b
               ON b.[name] = [dm].[name]
                  AND b.[type] = [dm].[type]
                  AND b.[uid] = [dm].[schema_id];

       /*get the 2022 object_id of anything that might have it changed*/
UPDATE [dm]
SET    [dm].[2022_object_id] = [o].[id]
FROM   [diff_system_objects] AS [dm]
       INNER JOIN [master].[sys].[sysobjects] AS [o]
               ON [o].[name] = [dm].[name]
                  AND [o].[type] = [dm].[type]
                  AND [o].[uid] = [dm].[schema_id]

/*add definitions for system_sql_modules that are in 2022*/
UPDATE [dm]
SET    [dm].[2022_definition] = [m].[definition]
FROM   [diff_system_objects] AS [dm]
       INNER JOIN [master].[sys].[system_sql_modules] AS [m]
               ON [m].[object_id] = [dm].[2022_object_id]
               WHERE  [dm].[2022_definition] IS NULL;

/*get definitions of remote objects that are in sys.sql_modules */
UPDATE [dm]
SET    [dm].[definition] = [rd].[definition],
       [dm].[uses_ansi_nulls] = [rd].[uses_ansi_nulls],
       [dm].[uses_quoted_identifier] = [rd].[uses_quoted_identifier]
FROM   [diff_system_objects] AS [dm]
       INNER JOIN [WINSRV2K25\SQL2025].[master].[sys].[sql_modules] AS [rd]
               ON [dm].[object_id] = [rd].[object_id]
WHERE  [dm].[definition] IS NULL;

/*update definitions of local objects that are in sys.sql_modules*/
UPDATE [dm]
SET    [dm].[2022_definition] = [m].[definition]
FROM   [diff_system_objects] AS [dm]
       INNER JOIN [master].[sys].[sql_modules] AS [m]
               ON [dm].[2022_object_id] = [m].[object_id]
WHERE  [dm].[2022_definition] IS NULL;

/*Get info about what the remote extended stored procedures are pointing to*/
IF OBJECT_ID('tempdb.dbo.#xsp_details') IS NOT NULL
  DROP TABLE #xsp_details;
GO
CREATE TABLE #xsp_details
  (
     [name] SYSNAME,
     [dll]    NVARCHAR(255)
  );
GO
DECLARE @RmtSQL NVARCHAR(500);
SET @RmtSQL = N'EXEC [master].[sys].[sp_helpextendedproc];';
INSERT INTO #xsp_details
            ([name],
             [dll])
EXEC (@RmtSQL) AT [WINSRV2K25\SQL2025];
GO
UPDATE [dm]
SET    [dm].[definition] = [xpd].[dll]
FROM   [diff_system_objects] AS [dm]
       INNER JOIN #xsp_details AS [xpd]
               ON [dm].[name] = [xpd].[name]
WHERE  [dm].[definition] IS NULL
       AND [dm].[type] = 'X'
GO
IF OBJECT_ID('tempdb.dbo.#xsp_details') IS NOT NULL
  DROP TABLE #xsp_details;
GO 

/*if we have extended stored procs, get info about the local ones too*/
IF OBJECT_ID('tempdb.dbo.#xsp_details') IS NOT NULL
  DROP TABLE #xsp_details;
GO
CREATE TABLE #xsp_details
  (
     [name] SYSNAME,
     [dll]    NVARCHAR(255)
  );
GO
INSERT INTO #xsp_details
            ([name],
             [dll])
EXEC [master].[sys].[sp_helpextendedproc];
GO
UPDATE [dm]
SET    [dm].[definition] = [xpd].[dll]
FROM   [diff_system_objects] AS [dm]
       INNER JOIN #xsp_details AS [xpd]
               ON [dm].[name] = [xpd].[name]
WHERE  [dm].[2022_definition] IS NULL
       AND [dm].[type] = 'X'
       AND [dm].[2022_object_id] IS NOT NULL;
GO
IF OBJECT_ID('tempdb.dbo.#xsp_details') IS NOT NULL
  DROP TABLE #xsp_details;
GO

/*at this point, anything left with a NULL 2022_object_id is new and not modified*/
UPDATE [diff_system_objects]
SET    [modified] = 0,
       [new] = 1
WHERE  [2022_object_id] IS NULL;

/*anything that has an object id in both versions, but still has NULL for new, is not new*/
UPDATE [diff_system_objects] 
SET    [new] = 0
WHERE [2022_object_id] IS NOT NULL
AND [new] IS NULL;

/*how much did existing objects change?*/
UPDATE [diff_system_objects]
SET    [character_count_change] = LEN([definition]) - LEN([2022_definition]),
       [line_count_change] = ( LEN([definition]) - LEN(REPLACE([definition], CHAR(13) + CHAR(10), N' ')) ) - ( LEN([2022_definition]) - LEN(REPLACE([2022_definition], CHAR(13) + CHAR(10), N' ')) )
WHERE  [definition] IS NOT NULL
       AND [2022_definition] IS NOT NULL;
GO
/*how much code do new objects have*/
UPDATE [diff_system_objects]
SET    [character_count_change] = LEN([definition]),
       [line_count_change] = ( LEN([definition]) - LEN(REPLACE([definition], CHAR(13) + CHAR(10), N' ')) )
WHERE  [definition] IS NOT NULL
       AND [2022_definition] IS NULL
       AND [new] = 1;
GO

/*check objects that have the same definitions, but just different object IDs*/
UPDATE [diff_system_objects]
SET    [modified] = 0,
       [new] = 0,[just_obj_id_changed]=1
WHERE  [object_id] <> [2022_object_id]
and [definition] is not null and [2022_definition] is not null
      AND HASHBYTES('MD5',[definition]) = HASHBYTES('MD5',[2022_definition]);
/*objects that have the same object ID and the same definition- should be 0*/
UPDATE [diff_system_objects]
SET    [modified] = 0,
       [new] = 0,[just_obj_id_changed]=0       
WHERE  [object_id] = [2022_object_id] and [definition] is not null and [2022_definition] is not null
      AND HASHBYTES('MD5',[definition]) = HASHBYTES('MD5',[2022_definition]);
/*objects with the same object_id but with code changes*/
UPDATE [diff_system_objects]
SET    [modified] = 1,
       [new] = 0,[just_obj_id_changed]=0
WHERE  [object_id] = [2022_object_id] and  [definition] is not null and [2022_definition] is not null
      AND HASHBYTES('MD5',[definition]) <> HASHBYTES('MD5',[2022_definition]);

/*different object_id with code changes*/
UPDATE [diff_system_objects]
SET    [modified] = 1,
       [new] = 0,[just_obj_id_changed]=0
WHERE  [object_id] <> [2022_object_id] and  [definition] is not null and [2022_definition] is not null
      AND HASHBYTES('MD5',[definition]) <> HASHBYTES('MD5',[2022_definition]);

/*get internal table and view column changes - aka new columns for internal tables that already exist in 2022*/
;WITH a
     AS (
     SELECT   [o].[uid] AS [schema_id],
              [o].id,
                [o].[name]           AS [table_name],
                [c].[column_id],
                [c].[name]                                AS [column_name],
                [dt].[name]                               AS [data_type],
                [o].[type]
         FROM   [WINSRV2K25\SQL2025].[master].[sys].[all_columns] AS [c]
                INNER JOIN [WINSRV2K25\SQL2025].[master].[sys].[sysobjects] AS [o]
                       ON [o].[id] = [c].[object_id]
                INNER JOIN [WINSRV2K25\SQL2025].[master].[sys].[types] AS [dt]
                        ON [c].[user_type_id] = [dt].[user_type_id]   
         EXCEPT 
              SELECT   [o].[uid] AS [schema_id],
              [o].id,
                [o].[name]           AS [table_name],
                [c].[column_id],
                [c].[name]                                AS [column_name],
                [dt].[name]                               AS [data_type],
                [o].[type]
         FROM   [master].[sys].[all_columns] AS [c]
                INNER JOIN [master].[sys].[sysobjects] AS [o]
                       ON [o].[id] = [c].[object_id]
                INNER JOIN [master].[sys].[types] AS [dt]
                        ON [c].[user_type_id] = [dt].[user_type_id]
                 )
INSERT INTO [diff_system_columns]
            ([object_id],
             [schema_id],
             [table_name],
             [column_id],
             [column_name],
             [data_type],
             [object_type],
             [2022_object_id])
SELECT [dm].[object_id],
       [a].[schema_id],
       [a].[table_name],
       [a].[column_id],
       [a].[column_name],
       [a].[data_type],
       [a].[type],
       [dm].[2022_object_id]
FROM   a
       INNER JOIN [diff_system_objects] AS [dm]
               ON [a].[table_name] = [dm].[name]
               AND [a].[id] = [dm].[object_id]
                  AND [a].[schema_id] = [dm].[schema_id]
                  AND [dm].[new] = 0
GROUP  BY [dm].[object_id],
          [a].[schema_id],
          [a].[table_name],
          [a].[column_id],
          [a].[column_name],
          [a].[data_type],
          [a].[type],
          [dm].[2022_object_id]
ORDER  BY [dm].[object_id],
          [a].[column_id];
GO 
/*at this point there are some records of columns in the table that shouldn't be there*/
DELETE [dc]
FROM   [diff_system_columns] [dc]
       INNER JOIN [master].[sys].[sysobjects] AS [o]
               ON [o].[id] = [dc].[2022_object_id]
       INNER JOIN [master].[sys].[all_columns] AS [c]
               ON [c].[object_id] = [o].[id]
                  AND [dc].[column_id] = [c].[column_id];
GO
/*update modified and just_obj_id_changed for internal tables based on the columns found above*/
UPDATE [diff_system_objects]
SET    [modified] = 0,
       [just_obj_id_changed] = 1
WHERE  [new] = 0
       AND [type] = 'IT'
       AND [modified] IS NULL
       AND [object_id] NOT IN (SELECT [object_id]
                               FROM   [diff_system_columns]); 
GO
UPDATE [diff_system_objects]
SET    [modified] = 1,
       [just_obj_id_changed] = 0
WHERE  [new] = 0
       AND [type] = 'IT'
       AND [modified] IS NULL
       AND [object_id] IN (SELECT [object_id]
                               FROM   [diff_system_columns]); 
GO

     /*return system object data*/

/*Overview*/
SELECT [type],
       SUM(ISNULL(CAST([new] AS INT), 0))                   AS [new],
       SUM(ISNULL(CAST([modified] AS INT), 0))              AS [modified],
       SUM(ISNULL(CAST([just_obj_id_changed] AS INT), 0)) AS [just_obj_id_changed],
       SUM(CASE
             WHEN [line_count_change] < 0 THEN 0
             ELSE [line_count_change]
           END)                                           AS [lines_added],
       SUM(CASE
             WHEN [line_count_change] > 0 THEN 0
             ELSE [line_count_change]
           END)                                           AS [lines_removed],
       COUNT(*)                                           AS [total_objects]
FROM   [diff_system_objects]
WHERE  [schema_id] <> 1
GROUP  BY [type];

/*get new data types*/
SELECT name,
       [system_type_id]
FROM   [WINSRV2K25\SQL2025].[master].[sys].[types]
EXCEPT
SELECT name,
       [system_type_id]
FROM   [master].[sys].[types]; 

/*get new columns*/

SELECT SCHEMA_NAME([schema_id]) + N'.'
       + [table_name] AS [Name],
       STRING_AGG([column_name]+' ('+UPPER([data_type])+')', ', ') AS [Columns],
       [object_type]
FROM   [diff_system_columns]
GROUP  BY [table_name],
          [schema_id],
          [object_type];

     
     /*Get code to dump in .sql files - these queries are used in DumpChangedSysObjectScripts.ps1*/

/*new objects*/
SELECT SCHEMA_NAME([schema_id]) + N'_' + [name]
       + N'.sql' AS [file_name],
       N'use [master]'+ CHAR(13) + CHAR(10)
       + N'GO'+ CHAR(13) + CHAR(10)
       + N'SET ANSI_NULLS '
       + CASE [uses_ansi_nulls]
           WHEN 1 THEN N'ON'
           ELSE N'OFF'
         END
       +N';'+ CHAR(13) + CHAR(10)
       + N'SET QUOTED_IDENTIFIER '
       + CASE [uses_quoted_identifier]
           WHEN 1 THEN N'ON'
           ELSE N'OFF;'
         END
       + N';'+ CHAR(13) + CHAR(10)
       + [definition] AS [definition]
FROM   [diff_system_objects]
WHERE  [new] = 1
       AND [type] <> 'X'
       AND [definition] IS NOT NULL
       AND [schema_id] <> 1;

/*modified objects*/
SELECT SCHEMA_NAME([schema_id]) + N'_' + [name]
       + N'.sql'                                 AS [file_name],
       + CASE
           WHEN [just_obj_id_changed] = 1 THEN N'    /*Only the object_id was changed between versions, the code is the same*/'
                                               + CHAR(13) + CHAR(10)
           ELSE N''
         END
       + N'use [master]'+ CHAR(13) + CHAR(10)
       + N'GO'+ CHAR(13) + CHAR(10)
       + N'SET ANSI_NULLS '
       + CASE [uses_ansi_nulls]
           WHEN 1 THEN N'ON'
           ELSE N'OFF'
         END
       + N';' + CHAR(13) + CHAR(10)
       + N'SET QUOTED_IDENTIFIER '
       + CASE [uses_quoted_identifier]
           WHEN 1 THEN N'ON'
           ELSE N'OFF;'
         END
       + N';' + CHAR(13) + CHAR(10)
       + N'/*====  SQL Server 2025 version  ====*/'
       + CHAR(13) + CHAR(10) + [definition] + CHAR(13)
       + CHAR(10) + CHAR(13) + CHAR(10)
       + N'/*====  SQL Server 2022 version  ====*/'
       + CHAR(13) + CHAR(10) + [2022_definition] AS [definition]
FROM   [diff_system_objects]
WHERE  [new] = 0
       AND [type] <> 'X'
       AND [definition] IS NOT NULL
       AND [2022_definition] IS NOT NULL
       AND [schema_id] <> 1;