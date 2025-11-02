SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
IF ( OBJECT_ID(N'dbo.sp_Pbkdf2HashMssql2025', N'P') IS NOT NULL )
  BEGIN
      DROP PROCEDURE [dbo].[sp_Pbkdf2HashMssql2025];
  END;
GO
CREATE PROCEDURE [dbo].[sp_Pbkdf2HashMssql2025] (@ClearTextPassword NVARCHAR(128),          /* clear text password */
                                                 @Iterations        INT           = 100000, /* number of hashing iterations */
                                                 @OutHash           VARBINARY(70) OUTPUT    /* password hash */
)
 /*  Returns the 70‑byte login hash used by SQL Server 2025:
     0x0300 | 4‑byte salt | 64‑byte PBKDF2‑HMAC‑SHA‑512 derived key 
     More info: 
     https://vladdba.com/2025/11/02/replicating-sql-server-2025-pbkdf2-hashing-algorithm-using-t-sql/ */
AS
    SET ANSI_PADDING ON;
    SET ANSI_WARNINGS ON;
    SET ARITHABORT ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET NUMERIC_ROUNDABORT OFF;
    SET NOCOUNT ON;

  BEGIN
      DECLARE @BlockIdx    BINARY(4)       = 0x00000001,          /* block index = 1 (big‑endian) */
              @U           VARBINARY(64),                         /* holds the latest HMAC output */
              @T           VARBINARY(64)   = 0x,                  /* accumulator (the result of the XOR operation)*/
              @i           INT             = 1,                   /* starts with i ends in teration */
              @PasswordBin VARBINARY(256),
              @Salt        VARBINARY(4)    = CRYPT_GEN_RANDOM(4); /* 4 byte random salt just like in the 
                                                                    pre-2025 versions of SQL Server*/
      /* first we need the password to be converted to varbinary(256) 
       (max 256 bytes because NVARCHAR(128) = 256 bytes) */
      SET @PasswordBin = CAST(@ClearTextPassword AS VARBINARY(256));
      /* initial HMAC (U1)
      copy-pasta from Wikipedia: 
       "U1 = PRF(Password, Salt + INT_32_BE(i))
       The first iteration of PRF uses Password as the PRF key and Salt 
       concatenated with i encoded as a big-endian 32-bit integer as the input. 
       (Note that i is a 1-based index.) "*/
      SET @U = [dbo].[fn_HmacSha512](@PasswordBin, @Salt + @BlockIdx);
      SET @T = @U;

      /* subsequent HMACs
       U2 = PRF(Password, U1) ...... Uc = PRF(Password, Uc-1) where c = @Iterations */
      WHILE @i < @Iterations
        BEGIN
            SET @U = [dbo].[fn_HmacSha512](@PasswordBin, @U);
            SET @T = [dbo].[fn_XorVarbinary](@T, @U);
            SET @i += 1;
        END;

      /* when done iterating, build the resulting hash:
       hash version + salt + derived key */
      SET @OutHash = 0x0300 + @Salt + @T;
  END;
GO