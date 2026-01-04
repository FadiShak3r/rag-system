"""
Database connection and data extraction module
"""
import pyodbc
import pandas as pd
from typing import List, Dict, Any
from config import SQL_SERVER_CONFIG


class DatabaseConnector:
    """Handles SQL Server database connections and queries"""
    
    def __init__(self):
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish connection to SQL Server database"""
        # Check available drivers first
        available_drivers = pyodbc.drivers()
        driver_name = SQL_SERVER_CONFIG.get('driver', 'ODBC Driver 18 for SQL Server')
        
        # Try to find a suitable SQL Server driver
        sql_server_drivers = [
            'ODBC Driver 18 for SQL Server',
            'ODBC Driver 17 for SQL Server',
            'ODBC Driver 13 for SQL Server',
            'FreeTDS',
        ]
        
        selected_driver = None
        for driver in sql_server_drivers:
            if driver in available_drivers:
                selected_driver = driver
                break
        
        if not selected_driver:
            print("\n❌ No SQL Server ODBC driver found!")
            print(f"\nAvailable drivers: {', '.join(available_drivers) if available_drivers else 'None'}")
            print("\n📦 To install the Microsoft ODBC Driver for SQL Server on macOS:")
            print("   1. Run: brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release")
            print("   2. Run: brew install msodbcsql18")
            print("\n   Or download from: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
            print("\n   After installation, verify with: python3 -c \"import pyodbc; print(pyodbc.drivers())\"")
            raise pyodbc.Error("ODBC Driver for SQL Server not found. Please install it first.")
        
        if selected_driver != driver_name:
            print(f"⚠️  Using driver: {selected_driver} (instead of {driver_name})")
        
        try:
            conn_str = (
                f"DRIVER={{{selected_driver}}};"
                f"SERVER={SQL_SERVER_CONFIG['server']};"
                f"DATABASE={SQL_SERVER_CONFIG['database']};"
                f"UID={SQL_SERVER_CONFIG['user']};"
                f"PWD={SQL_SERVER_CONFIG['password']};"
                "Encrypt=no;"
            )
            self.connection = pyodbc.connect(conn_str)
            print(f"Connected to SQL Server database: {SQL_SERVER_CONFIG['database']}")
        except pyodbc.Error as e:
            print(f"Error connecting to SQL Server: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("SQL Server connection closed")
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results as list of dictionaries"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [column[0] for column in cursor.description]
            
            # Fetch all rows and convert to list of dictionaries
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            
            cursor.close()
            return results
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")
            raise
    
    def get_table_data(self, table_name: str) -> pd.DataFrame:
        """Get all data from a table as pandas DataFrame"""
        query = f"SELECT * FROM {table_name}"
        results = self.execute_query(query)
        return pd.DataFrame(results)
    
    def get_all_tables_data(self, table_names: List[str]) -> Dict[str, pd.DataFrame]:
        """Get data from multiple tables"""
        tables_data = {}
        for table_name in table_names:
            try:
                tables_data[table_name] = self.get_table_data(table_name)
                print(f"Retrieved {len(tables_data[table_name])} rows from {table_name}")
            except pyodbc.Error as e:
                print(f"Error retrieving data from {table_name}: {e}")
        return tables_data
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

