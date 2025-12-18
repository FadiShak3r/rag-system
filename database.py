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
        try:
            # Build server string with port if specified
            server = SQL_SERVER_CONFIG['server']
            port = SQL_SERVER_CONFIG.get('port')
            if port:
                server = f"{server},{port}"
            
            # Build connection string
            conn_str_parts = [
                f"DRIVER={{{SQL_SERVER_CONFIG['driver']}}};",
                f"SERVER={server};",
                f"DATABASE={SQL_SERVER_CONFIG['database']};",
                f"UID={SQL_SERVER_CONFIG['user']};",
                f"PWD={SQL_SERVER_CONFIG['password']};",
            ]
            
            # Add TrustServerCertificate if enabled
            if SQL_SERVER_CONFIG.get('trust_server_certificate', False):
                conn_str_parts.append("TrustServerCertificate=yes;")
            else:
                conn_str_parts.append("Encrypt=no;")
            
            conn_str = "".join(conn_str_parts)
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

