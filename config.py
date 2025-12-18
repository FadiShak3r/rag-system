"""
Configuration module for RAG system
"""
import os
from dotenv import load_dotenv

load_dotenv()

SQL_SERVER_CONFIG = {
    'server': os.getenv('SQL_SERVER', 'localhost'),
    'database': os.getenv('SQL_DATABASE'),
    'user': os.getenv('SQL_USER'),
    'password': os.getenv('SQL_PASSWORD'),
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

