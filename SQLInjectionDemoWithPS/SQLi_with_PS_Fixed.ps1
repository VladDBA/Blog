#Connection parameters (yes, they're hardcoded)
$server = "LOCALHOST\VSQL2019"
$database = "AppDB"
$login = ""
$password = ""

$TryAgain = "Y"
Write-Host "SQL Injection Demo with PowerShell - fixed"
Write-Host "-----------------------------------"
$SearchString = Read-Host -Prompt "Search for a product"


if (!([string]::IsNullOrEmpty($login))) {
    $connectionString = "Server=$server;Database=$database;User Id=$login;Password=$password;;"
}
else {
    $connectionString = "Server=$server;Database=$database;trusted_connection=true;"
}
function Get-Products {
    param (
        [Parameter(Position = 0, Mandatory = $true)]
        [string]$SearchString,
        [Parameter(Position = 1, Mandatory = $true)]
        [string]$connectionString
    )
    $Query = "SELECT [Name], [Manufacturer] FROM [Products] WHERE [Name] LIKE '%'+@SearchString+'%' AND [IsSecret] = 0;"
    $SearchSet = New-Object System.Data.DataSet
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $sqlConnection.ConnectionString = $connectionString
    $SearchCommand = $sqlConnection.CreateCommand()
    $SearchCommand.CommandText = $Query
    $SearchCommand.Parameters.Add("@SearchString", [Data.SQLDBType]::VarChar, 100) | Out-Null
	$SearchCommand.Parameters["@SearchString"].Value = $SearchString
    $SearchAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SearchAdapter.SelectCommand = $SearchCommand
    try {
        Write-Host "Executing query: "
        Write-Host "$Query" -ForegroundColor Yellow
        Write-Host ""
        $sqlConnection.Open()
        $SearchAdapter.Fill($SearchSet) | Out-Null -ErrorAction Stop
        Write-Host "Results:"
        $SearchSet.Tables[0] | Format-Table -AutoSize
    }
    catch {
        Write-Host " Error: $_" -ForegroundColor Red
    }
    finally {
        $sqlConnection.Close()
        $sqlConnection.Dispose()
    }
}
while ($TryAgain -eq "Y") {
    Get-Products -SearchString $SearchString -connectionString $connectionString
    $TryAgain = Read-Host -Prompt "Do you want to search again? (Y/N)"
    if ($TryAgain -eq "Y") {
        $SearchString = Read-Host -Prompt "Search for a product"
    }
    
}
Write-Host ""
Write-Host "End of the demo" -ForegroundColor Green