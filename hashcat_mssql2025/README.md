# Hashcat Module for MSSQL 2025 Login Password Hashes (module 1732)

This module adds support for cracking SQL Server 2025's login hashes that use the new PBKDF2-HMAC-SHA512 algorithm.

> Related blog post coming soon on [vladdba.com](https://vladdba.com/)

The [modules](modules) directory contains compiled module binaries for both Windows (.dll) and Linux (.so) for the current stable release of hashcat ([7.1.2](https://github.com/hashcat/hashcat/releases/tag/v7.1.2)).

## Using the precompiled modules with hashcat 7.1.2

1. Copy the contents of the [modules](modules) directory in your hashcat 7.1.2 modules director.
2. Copy [OpenCL/m01732-pure.cl](OpenCL/m01732-pure.cl) in your hashcat 7.1.2 OpenCL directory.
3. Run hashcat with `-m 1732`.

## Compiling from source

1. Copy the contents of the [src/modules](src/modules) directory to your `hashcat/src/modules` directory.
2. Copy [OpenCL/m01732-pure.cl](OpenCL/m01732-pure.cl) to your `hashcat/OpenCL` directory.
3. Copy the contents of [tools/test_modules/m01732.pm](tools/test_modules/m01732.pm) to your `hashcat/tools/test_modules` directory.
This is only needed if you want to run the built-in unit tests afterwards.
4. Run `make` or `make win`.

This is just a stand-in until [my PR to hashcat](https://github.com/hashcat/hashcat/pull/4667) gets approved and merged.
