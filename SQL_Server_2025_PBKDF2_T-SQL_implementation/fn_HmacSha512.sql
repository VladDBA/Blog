SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF ( OBJECT_ID(N'dbo.fn_HmacSha512', N'FN') IS NOT NULL )
  BEGIN
      DROP FUNCTION [dbo].[fn_HmacSha512];
  END;
GO

CREATE FUNCTION [dbo].[fn_HmacSha512] (@Key VARBINARY(256), /* password (max 256 bytes after conversion) */
                                       @Msg VARBINARY(256)  /* salt+counter or previous @U value */
)
RETURNS VARBINARY(64)
/* returns VARBINARY(64) = HMAC‑SHA‑512(key, message) 
   More info: 
   https://vladdba.com/2025/11/02/replicating-sql-server-2025-pbkdf2-hashing-algorithm-using-t-sql/ */
AS
  BEGIN
      DECLARE @BlockSize    INT             = 128, /* SHA‑512 block size */
              @KeyPadded    VARBINARY(128), 
              @InnerPadding VARBINARY(128)  = 0x,
              @OuterPadding VARBINARY(128)  = 0x,
              @i            INT             = 1;

      /* if the key is longer than a block, hash it first (as per RFC 2104) */
      IF ( DATALENGTH(@Key) > @BlockSize )
        BEGIN
            SET @Key = HASHBYTES(N'SHA2_512', @Key);
        END;

      /* pad the key to exactly one block (append zeros) */
      SET @KeyPadded = @Key
                       + CAST(REPLICATE(0x00, @BlockSize - DATALENGTH(@Key)) AS VARBINARY);

      WHILE @i <= @BlockSize
        /* build @InnerPadding and @OuterPadding by XOR‑ing each byte of the padded key
          with 0x36 (@InnerPadding) and 0x5C (@OuterPadding) */
        BEGIN
            SET @InnerPadding += CAST(CAST(SUBSTRING(@KeyPadded, @i, 1) AS TINYINT) ^ 0x36 AS BINARY(1));
            SET @OuterPadding += CAST(CAST(SUBSTRING(@KeyPadded, @i, 1) AS TINYINT) ^ 0x5C AS BINARY(1));
            SET @i += 1;
        END;

      /* HMAC = H( outer padding + H( inner padding + message ) ) */
      RETURN HASHBYTES(N'SHA2_512', @OuterPadding
                                    + HASHBYTES(N'SHA2_512', @InnerPadding + @Msg));
  END;
GO