This is the Java example for causing ASYNC_NETWORK_IO waits.

See the related blog post [here](https://vladdba.com/2024/01/22/how-applications-cause-excessive-async_network_io-waits-in-sql-server/)

### Requirments:

 - Your instance needs to have the AdventureWorks2019 database

### Usage example:
```PowerShell 
java -jar --enable-preview .\ASYNC_NW_IO.jar "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2019;encrypt=false" "NameOfYourSQLLogin" "SuperSecurePassword"
```