SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Employee](
	[BusinessEntityID] [int] NOT NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [dbo].[Name] NOT NULL,
	[MiddleName] [dbo].[Name] NULL,
	[LastName] [dbo].[Name] NOT NULL,
	[Suffix] [nvarchar](10) NULL,
	[JobTitle] [nvarchar](50) NOT NULL,
	[PhoneNumber] [dbo].[Phone] NULL,
	[PhoneNumberType] [dbo].[Name] NULL,
	[EmailAddress] [nvarchar](50) NULL,
	[EmailPromotion] [int] NOT NULL,
	[AddressLine1] [nvarchar](60) NOT NULL,
	[AddressLine2] [nvarchar](60) NULL,
	[City] [nvarchar](30) NOT NULL,
	[StateProvinceName] [dbo].[Name] NOT NULL,
	[PostalCode] [nvarchar](15) NOT NULL,
	[CountryRegionName] [dbo].[Name] NOT NULL,
	[AdditionalContactInfo] [xml] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE [dbo].[EmployeeDepartment](
	[BusinessEntityID] [int] NOT NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [dbo].[Name] NOT NULL,
	[MiddleName] [dbo].[Name] NULL,
	[LastName] [dbo].[Name] NOT NULL,
	[Suffix] [nvarchar](10) NULL,
	[JobTitle] [nvarchar](50) NOT NULL,
	[Department] [dbo].[Name] NOT NULL,
	[GroupName] [dbo].[Name] NOT NULL,
	[StartDate] [date] NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[EmployeeDepartmentHistory](
	[BusinessEntityID] [int] NOT NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [dbo].[Name] NOT NULL,
	[MiddleName] [dbo].[Name] NULL,
	[LastName] [dbo].[Name] NOT NULL,
	[Suffix] [nvarchar](10) NULL,
	[Shift] [dbo].[Name] NOT NULL,
	[Department] [dbo].[Name] NOT NULL,
	[GroupName] [dbo].[Name] NOT NULL,
	[StartDate] [date] NOT NULL,
	[EndDate] [date] NULL
) ON [PRIMARY]
GO