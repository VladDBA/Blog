# SQL Server 2025 PBKDF2 pure T-SQL implementation

See the related blog post [here](https://vladdba.com/2025/11/02/replicating-sql-server-2025-pbkdf2-hashing-algorithm-using-t-sql/)

Execution example:

```sql
/* Generate a hash for a sample password */
DECLARE @Hash VARBINARY(70);

EXEC [dbo].[sp_Pbkdf2HashMssql2025]
  @ClearTextPassword = N'$up3R-S3Cur3P@22',
  @Iterations        = 100000,
  @OutHash           = @Hash OUTPUT;
/* compare using PWDCOMPARE */
SELECT PWDCOMPARE(N'$up3R-S3Cur3P@22', @Hash) AS [password_match];
```
