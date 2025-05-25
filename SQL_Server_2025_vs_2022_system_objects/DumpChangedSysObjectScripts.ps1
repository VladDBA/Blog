#Connection parameters (yes, they're hardcoded)
$server = ""
$database = "data_mining_2025"
$login = ""
$password = ""
$OutPath = ""


$NewObjQuery = "SELECT SCHEMA_NAME([schema_id]) + N'_' + [name]
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
       AND [schema_id] <> 1;"

$ChangedObjQuery = "SELECT SCHEMA_NAME([schema_id]) + N'_' + [name]
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
       AND [schema_id] <> 1;"

Write-Host "Dumping object definitions from diff_system_objects"
Write-Host "-----------------------------------"
$connectionString = if (!([string]::IsNullOrEmpty($login))) {
    "Server=$server;Database=$database;User Id=$login;Password=$password;"
} else {
    "Server=$server;Database=$database;trusted_connection=true;"
}

function ExecuteQueryAndExportFiles {
    param($queryText)

    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection $connectionString
    $Query = New-Object System.Data.SqlClient.SqlCommand -ArgumentList $queryText, $SqlConnection
    $Query.CommandTimeout = 200
    $Adapter = New-Object System.Data.SqlClient.SqlDataAdapter $Query
    $Set = New-Object System.Data.DataSet
    $Adapter.Fill($Set) | Out-Null
    $SqlConnection.Dispose()

    $Tbl = $Set.Tables[0]
    foreach ($row in $Tbl.Rows) {
        $FileName = $row["file_name"]
        $FileContent = $row["definition"]
        $OutFilePath = Join-Path -Path $OutPath -ChildPath $FileName
        $FileContent | Out-File -Encoding ASCII -FilePath $OutFilePath -Force
    }
}
ExecuteQueryAndExportFiles -queryText $NewObjQuery
ExecuteQueryAndExportFiles -queryText $ChangedObjQuery
