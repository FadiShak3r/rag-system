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

TABLES = [
    'dbo.dimProduct',
]

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

