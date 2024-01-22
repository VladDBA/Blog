#Connection parameters (yes, they're hardcoded)
$server = "LOCALHOST\VSQL2019"
$database = "AdventureWorks2019"
$login = ""
$password = ""

#Create a SQL connection
$sqlConnection = New-Object System.Data.SqlClient.SqlConnection
if (!([string]::IsNullOrEmpty($login))) {
    $connectionString = "Server=$server;Database=$database;User Id=$login;Password=$password;;"
} else {
    $connectionString = "Server=$server;Database=$database;trusted_connection=true;"
}
$sqlConnection.ConnectionString = $connectionString

# Open the connection
$sqlConnection.Open()

# Define the query
$query = "SELECT SalesOrderID, CarrierTrackingNumber, rowguid FROM Sales.SalesOrderDetail;"

# Create a SQL command
$sqlCommand = $sqlConnection.CreateCommand()
$sqlCommand.CommandText = $query

# Execute the query and process the result set
$sqlReader = $sqlCommand.ExecuteReader()

while ($sqlReader.Read()) {
    # List individual row
    $column1Value = $sqlReader["SalesOrderID"]
    $column2Value = $sqlReader["CarrierTrackingNumber"]
    $column3Value = $sqlReader["rowguid"]

    Write-Host "Sales Order ID: $column1Value, Carrier Tracking Number: $column2Value, row GUID: $column3Value"
    # wait for 1 second before going to next row
    Start-Sleep -Milliseconds 1000
}

# Close reader
$sqlReader.Close()

# Close the connection when done
$sqlConnection.Close()