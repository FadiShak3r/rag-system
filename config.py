"""
Configuration module for RAG system
"""
import os
from dotenv import load_dotenv

load_dotenv()

SQL_SERVER_CONFIG = {
    'driver': os.getenv('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server'),
    'server': os.getenv('MSSQL_SERVER', 'localhost'),
    'port': os.getenv('MSSQL_PORT', '1433'),
    'database': os.getenv('MSSQL_DATABASE'),
    'user': os.getenv('MSSQL_UID'),
    'password': os.getenv('MSSQL_PWD'),
    'trust_server_certificate': os.getenv('MSSQL_TRUST_SERVER_CERTIFICATE', 'yes').lower() == 'yes',
}

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_EMBEDDING_MODEL = 'text-embedding-3-small'
OPENAI_CHAT_MODEL = 'gpt-4o-mini'
CHROMA_DB_PATH = os.getenv('CHROMA_DB_PATH', './chroma_db')

# Metabase configuration
META_BASE_URL = os.getenv('META_BASE_URL', 'http://localhost:3000')
META_BASE_API_KEY = os.getenv('META_BASE_API_KEY')
META_BASE_DATABASE_ID = int(os.getenv('META_BASE_DATABASE_ID', '2'))

TABLES = [
    'DimProduct',
    'DimCustomer',
    'FactProductInventory',
    'CompetitorDimProduct',
    'CompetitorFactInternetSales',
    'DimAccount',
    'DimCurrency',
    'DimDate',
    'DimDepartmentGroup',
    'DimEmployee',
    'DimGeography',
    'DimOrganization',
    'DimProductCategory',
    'DimProductSubcategory',
    'DimProfile',
    'DimPromotion',
    'DimReseller',
    'DimSalesReason',
    'DimSalesTerritory',
    'DimScenario',
    'DimScrapeRun',
    'FactAdditionalInternationalProductDescription',
    'FactCallCenter',
    'FactCurrencyRate',
    'FactFinance',
    'FactInternetSales',
]

# Database schema for AI reference
DB_SCHEMA = """
Tables:
1. DimProduct - Product information
   - ProductKey (int, primary key)
   - ProductName (nvarchar)
   - ProductDescription (nvarchar)
   - ProductSubcategoryKey (int)
   - Color (nvarchar)
   - Size (nvarchar)
   - SizeRange (nvarchar)
   - ModelName (nvarchar)
   - DealerPrice (money)
   - ListPrice (money)
   - Weight (float)
   - Class (nvarchar)
   - Style (nvarchar)
   - Status (nvarchar)

2. DimCustomer - Customer information
   - CustomerKey (int, primary key)
   - FirstName (nvarchar)
   - LastName (nvarchar)
   - FullName (nvarchar)
   - BirthDate (date)
   - MaritalStatus (nchar) - 'M' or 'S'
   - Gender (nvarchar) - 'M' or 'F'
   - EmailAddress (nvarchar)
   - YearlyIncome (money)
   - TotalChildren (tinyint)
   - NumberChildrenAtHome (tinyint)
   - EnglishEducation (nvarchar)
   - EnglishOccupation (nvarchar)
   - HouseOwnerFlag (nchar)
   - NumberCarsOwned (tinyint)
   - AddressLine1 (nvarchar)
   - Phone (nvarchar)
   - DateFirstPurchase (date)
   - CommuteDistance (nvarchar)

3. FactProductInventory - Inventory data
   - ProductKey (int, foreign key to DimProduct)
   - DateKey (int)
   - MovementDate (date)
   - UnitCost (money)
   - UnitsIn (int)
   - UnitsOut (int)
   - UnitsBalance (int)

4. CompetitorDimProduct - Competitor product information
   - ProductKey (int, primary key)
   - ProductAlternateKey (nvarchar(50))
   - ProductSubcategoryKey (int)
   - WeightUnitMeasureCode (nvarchar(10))
   - SizeUnitMeasureCode (nvarchar(10))
   - EnglishProductName (nvarchar(200))
   - SpanishProductName (nvarchar(200))
   - FrenchProductName (nvarchar(200))
   - StandardCost (decimal(18,2))
   - FinishedGoodsFlag (bit)
   - Color (nvarchar(50))
   - SafetyStockLevel (int)
   - ReorderPoint (int)
   - ListPrice (decimal(18,2))
   - Size (nvarchar(50))
   - SizeRange (nvarchar(50))
   - Weight (decimal(18,2))
   - DaysToManufacture (int)
   - ProductLine (nvarchar(10))
   - DealerPrice (decimal(18,2))
   - Class (nvarchar(10))
   - Style (nvarchar(10))
   - ModelName (nvarchar(100))
   - LargePhoto (nvarchar(max))
   - EnglishDescription (nvarchar(max))
   - FrenchDescription (nvarchar(max))
   - ChineseDescription (nvarchar(max))
   - ArabicDescription (nvarchar(max))
   - HebrewDescription (nvarchar(max))
   - ThaiDescription (nvarchar(max))
   - GermanDescription (nvarchar(max))
   - JapaneseDescription (nvarchar(max))
   - TurkishDescription (nvarchar(max))
   - StartDate (nvarchar(50))
   - EndDate (nvarchar(50))
   - Status (nvarchar(20))
   - Company (nvarchar(50), required)

5. CompetitorFactInternetSales - Competitor internet sales data
   - ProductKey (int, foreign key to CompetitorDimProduct)
   - OrderDateKey (int)
   - SalesTerritoryKey (int)
   - OrderQuantity (decimal(18,3))
   - UnitPrice (decimal(18,2))
   - StandardCost (decimal(18,2))
   - TotalProductCost (decimal(18,2))
   - SalesAmount (decimal(18,2))
   - Company (nvarchar(50), required)

6. DimAccount - Account dimension with hierarchical structure
   - AccountKey (int, primary key)
   - ParentAccountKey (int, foreign key to DimAccount)
   - AccountCodeAlternateKey (int)
   - ParentAccountCodeAlternateKey (int)
   - AccountDescription (nvarchar(50))
   - AccountType (nvarchar(50))
   - Operator (nvarchar(50))
   - CustomMembers (nvarchar(300))
   - ValueType (nvarchar(50))
   - CustomMemberOptions (nvarchar(200))

7. DimCurrency - Currency dimension
   - CurrencyKey (int, primary key)
   - CurrencyAlternateKey (nchar(3), required)
   - CurrencyName (nvarchar(50), required)

8. DimDate - Date dimension with calendar and fiscal information
   - DateKey (int, primary key)
   - FullDateAlternateKey (date, required)
   - DayNumberOfWeek (tinyint)
   - EnglishDayNameOfWeek (nvarchar(10))
   - SpanishDayNameOfWeek (nvarchar(10))
   - FrenchDayNameOfWeek (nvarchar(10))
   - DayNumberOfMonth (tinyint)
   - DayNumberOfYear (smallint)
   - WeekNumberOfYear (tinyint)
   - EnglishMonthName (nvarchar(10))
   - SpanishMonthName (nvarchar(10))
   - FrenchMonthName (nvarchar(10))
   - MonthNumberOfYear (tinyint)
   - CalendarQuarter (tinyint)
   - CalendarYear (smallint)
   - CalendarSemester (tinyint)
   - FiscalQuarter (tinyint)
   - FiscalYear (smallint)
   - FiscalSemester (tinyint)

9. DimDepartmentGroup - Department group dimension with hierarchical structure
   - DepartmentGroupKey (int, primary key)
   - ParentDepartmentGroupKey (int, foreign key to DimDepartmentGroup)
   - DepartmentGroupName (nvarchar(50))

10. DimEmployee - Employee dimension with hierarchical structure
   - EmployeeKey (int, primary key)
   - ParentEmployeeKey (int, foreign key to DimEmployee)
   - EmployeeNationalIDAlternateKey (nvarchar(15))
   - ParentEmployeeNationalIDAlternateKey (nvarchar(15))
   - SalesTerritoryKey (int)
   - FirstName (nvarchar(50), required)
   - LastName (nvarchar(50), required)
   - MiddleName (nvarchar(50))
   - NameStyle (bit, required)
   - Title (nvarchar(50))
   - HireDate (date)
   - BirthDate (date)
   - LoginID (nvarchar(256))
   - EmailAddress (nvarchar(50))
   - Phone (nvarchar(25))
   - MaritalStatus (nchar(1)) - 'M' or 'S'
   - EmergencyContactName (nvarchar(50))
   - EmergencyContactPhone (nvarchar(25))
   - SalariedFlag (bit)
   - Gender (nchar(1)) - 'M' or 'F'
   - PayFrequency (tinyint)
   - BaseRate (money)
   - VacationHours (smallint)
   - SickLeaveHours (smallint)
   - CurrentFlag (bit, required)
   - SalesPersonFlag (bit, required)
   - DepartmentName (nvarchar(50))
   - StartDate (date)
   - EndDate (date)
   - Status (nvarchar(50))
   - EmployeePhoto (varbinary(max))

11. DimGeography - Geography dimension
   - GeographyKey (int, primary key)
   - City (nvarchar(30))
   - StateProvinceCode (nvarchar(3))
   - StateProvinceName (nvarchar(50))
   - CountryRegionCode (nvarchar(3))
   - EnglishCountryRegionName (nvarchar(50))
   - SpanishCountryRegionName (nvarchar(50))
   - FrenchCountryRegionName (nvarchar(50))
   - PostalCode (nvarchar(15))
   - SalesTerritoryKey (int)
   - IpAddressLocator (nvarchar(15))

12. DimOrganization - Organization dimension with hierarchical structure
   - OrganizationKey (int, primary key)
   - ParentOrganizationKey (int, foreign key to DimOrganization)
   - PercentageOfOwnership (nvarchar(16))
   - OrganizationName (nvarchar(50))
   - CurrencyKey (int, foreign key to DimCurrency)

13. DimProductCategory - Product category dimension
   - ProductCategoryKey (int, primary key)
   - ProductCategoryAlternateKey (int)
   - EnglishProductCategoryName (nvarchar(50), required)
   - SpanishProductCategoryName (nvarchar(50), required)
   - FrenchProductCategoryName (nvarchar(50), required)

14. DimProductSubcategory - Product subcategory dimension
   - ProductSubcategoryKey (int, primary key)
   - ProductSubcategoryAlternateKey (int)
   - EnglishProductSubcategoryName (nvarchar(50), required)
   - SpanishProductSubcategoryName (nvarchar(50), required)
   - FrenchProductSubcategoryName (nvarchar(50), required)
   - ProductCategoryKey (int, foreign key to DimProductCategory)

15. DimProfile - Profile dimension
   - ProfileKey (int, primary key)
   - ProfileName (nvarchar(255), required)
   - Platform (nvarchar(50), required)
   - IsOwnBrand (bit, required)

16. DimPromotion - Promotion dimension
   - PromotionKey (int, primary key)
   - PromotionAlternateKey (int)
   - EnglishPromotionName (nvarchar(255))
   - SpanishPromotionName (nvarchar(255))
   - FrenchPromotionName (nvarchar(255))
   - DiscountPct (float)
   - EnglishPromotionType (nvarchar(50))
   - SpanishPromotionType (nvarchar(50))
   - FrenchPromotionType (nvarchar(50))
   - EnglishPromotionCategory (nvarchar(50))
   - SpanishPromotionCategory (nvarchar(50))
   - FrenchPromotionCategory (nvarchar(50))
   - StartDate (datetime, required)
   - EndDate (datetime)
   - MinQty (int)
   - MaxQty (int)

17. DimReseller - Reseller dimension
   - ResellerKey (int, primary key)
   - GeographyKey (int, foreign key to DimGeography)
   - ResellerAlternateKey (nvarchar(15))
   - Phone (nvarchar(25))
   - BusinessType (varchar(20), required)
   - ResellerName (nvarchar(50), required)
   - NumberEmployees (int)
   - OrderFrequency (char(1))
   - OrderMonth (tinyint)
   - FirstOrderYear (int)
   - LastOrderYear (int)
   - ProductLine (nvarchar(50))
   - AddressLine1 (nvarchar(60))
   - AddressLine2 (nvarchar(60))
   - AnnualSales (money)
   - BankName (nvarchar(50))
   - MinPaymentType (tinyint)
   - MinPaymentAmount (money)
   - AnnualRevenue (money)
   - YearOpened (int)

18. DimSalesReason - Sales reason dimension
   - SalesReasonKey (int, primary key)
   - SalesReasonAlternateKey (int, required)
   - SalesReasonName (nvarchar(50), required)
   - SalesReasonReasonType (nvarchar(50), required)

19. DimSalesTerritory - Sales territory dimension
   - SalesTerritoryKey (int, primary key)
   - SalesTerritoryAlternateKey (int)
   - SalesTerritoryRegion (nvarchar(50), required)
   - SalesTerritoryCountry (nvarchar(50), required)
   - SalesTerritoryGroup (nvarchar(50))
   - SalesTerritoryImage (varbinary(max))

20. DimScenario - Scenario dimension
   - ScenarioKey (int, primary key)
   - ScenarioName (nvarchar(50))

21. DimScrapeRun - Scrape run dimension
   - ScrapeRunID (int, primary key)
   - ScrapeTimestamp (datetime, required)

22. FactAdditionalInternationalProductDescription - Additional product descriptions
   - ProductKey (int, foreign key to DimProduct, required)
   - CultureName (nvarchar(50), required)
   - ProductDescription (nvarchar(max), required)

23. FactCallCenter - Call center fact data
   - FactCallCenterID (int, primary key)
   - DateKey (int, required)
   - WageType (nvarchar(15), required)
   - Shift (nvarchar(20), required)
   - LevelOneOperators (smallint, required)
   - LevelTwoOperators (smallint, required)
   - TotalOperators (smallint, required)
   - Calls (int, required)
   - AutomaticResponses (int, required)
   - Orders (int, required)
   - IssuesRaised (smallint, required)
   - AverageTimePerIssue (smallint, required)
   - ServiceGrade (float, required)
   - Date (datetime)

24. FactCurrencyRate - Currency rate fact data
   - CurrencyKey (int, foreign key to DimCurrency, required)
   - DateKey (int, required)
   - AverageRate (float, required)
   - EndOfDayRate (float, required)
   - Date (datetime)

25. FactFinance - Finance fact data
   - FinanceKey (int, primary key)
   - DateKey (int, required)
   - OrganizationKey (int, foreign key to DimOrganization, required)
   - DepartmentGroupKey (int, foreign key to DimDepartmentGroup, required)
   - ScenarioKey (int, foreign key to DimScenario, required)
   - AccountKey (int, foreign key to DimAccount, required)
   - Amount (float, required)
   - Date (datetime)

26. FactInternetSales - Internet sales fact data
   - ProductKey (int, foreign key to DimProduct, required)
   - OrderDateKey (int, required)
   - DueDateKey (int, required)
   - ShipDateKey (int, required)
   - CustomerKey (int, foreign key to DimCustomer, required)
   - PromotionKey (int, foreign key to DimPromotion, required)
   - CurrencyKey (int, foreign key to DimCurrency, required)
   - SalesTerritoryKey (int, foreign key to DimSalesTerritory, required)
   - SalesOrderNumber (nvarchar(20), required)
   - SalesOrderLineNumber (tinyint, required)
   - RevisionNumber (tinyint, required)
   - OrderQuantity (smallint, required)
   - UnitPrice (money, required)
   - ExtendedAmount (money, required)
   - UnitPriceDiscountPct (float, required)
   - DiscountAmount (float, required)
   - ProductStandardCost (money, required)
   - TotalProductCost (money, required)
   - SalesAmount (money, required)
   - TaxAmt (money, required)
   - Freight (money, required)
   - CarrierTrackingNumber (nvarchar(25))
   - CustomerPONumber (nvarchar(25))
   - OrderDate (datetime)
   - DueDate (datetime)
   - ShipDate (datetime)
"""

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

