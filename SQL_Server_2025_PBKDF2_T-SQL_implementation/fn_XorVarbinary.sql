SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
IF ( OBJECT_ID(N'dbo.fn_XorVarbinary', N'FN') IS NOT NULL )
  BEGIN
      DROP FUNCTION [dbo].[fn_XorVarbinary];
  END;
GO

CREATE FUNCTION [dbo].[fn_XorVarbinary] (@T VARBINARY(64),
                                         @U VARBINARY(64))
RETURNS VARBINARY(64)
/* byte‑wise XOR of two varbinary values 
   More info: 
   https://vladdba.com/2025/11/02/replicating-sql-server-2025-pbkdf2-hashing-algorithm-using-t-sql/ */
AS
  BEGIN;
      DECLARE @LenT   INT            = DATALENGTH(@T),
              @LenU   INT            = DATALENGTH(@U),
              @MaxLen INT,           
              @i      INT            = 1,
              @Result VARBINARY(64) = 0x,
              @ByteT  TINYINT,
              @ByteU  TINYINT;

      SET @MaxLen = CASE
                      WHEN @LenT > @LenU THEN @LenT
                      ELSE @LenU
                    END;

      WHILE @i <= @MaxLen
       /* pull the @i‑th byte from each operand, if the operand is shorter,
         treat the missing byte as 0 (zero‑padding on the right) */
        BEGIN
            SET @ByteT = CASE
                WHEN @i <= @LenT THEN CAST(SUBSTRING(@T, @i, 1) AS TINYINT)
                ELSE 0
              END;
            SET @ByteU = CASE
                WHEN @i <= @LenU THEN CAST(SUBSTRING(@U, @i, 1) AS TINYINT)
                ELSE 0
              END;
              /* XOR the two bytes and append to @result. */
            SET @Result += CAST(@ByteT ^ @ByteU AS BINARY(1));
            SET @i += 1;
        END;
      RETURN @Result;
  END;
GO