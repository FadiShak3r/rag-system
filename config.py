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
"""

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

