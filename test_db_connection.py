"""
Test script to verify SQL Server database connection
"""
import sys
from database import DatabaseConnector
from config import SQL_SERVER_CONFIG, TABLES


def test_connection():
    """Test basic database connection"""
    print("=" * 60)
    print("SQL Server Connection Test")
    print("=" * 60)
    
    # Display configuration (without password)
    print("\n1. Configuration Check:")
    print(f"   Driver: {SQL_SERVER_CONFIG.get('driver', 'NOT SET')}")
    print(f"   Server: {SQL_SERVER_CONFIG.get('server', 'NOT SET')}")
    print(f"   Port: {SQL_SERVER_CONFIG.get('port', 'NOT SET')}")
    print(f"   Database: {SQL_SERVER_CONFIG.get('database', 'NOT SET')}")
    print(f"   User: {SQL_SERVER_CONFIG.get('user', 'NOT SET')}")
    print(f"   Password: {'*' * len(SQL_SERVER_CONFIG['password']) if SQL_SERVER_CONFIG.get('password') else 'NOT SET'}")
    print(f"   Trust Server Certificate: {SQL_SERVER_CONFIG.get('trust_server_certificate', False)}")
    
    # Check if all required config values are set
    missing_config = []
    if not SQL_SERVER_CONFIG.get('server'):
        missing_config.append('MSSQL_SERVER')
    if not SQL_SERVER_CONFIG.get('database'):
        missing_config.append('MSSQL_DATABASE')
    if not SQL_SERVER_CONFIG.get('user'):
        missing_config.append('MSSQL_UID')
    if not SQL_SERVER_CONFIG.get('password'):
        missing_config.append('MSSQL_PWD')
    
    if missing_config:
        print(f"\n❌ ERROR: Missing configuration values: {', '.join(missing_config)}")
        print("   Please set these in your .env file")
        return False
    
    print("   ✓ All configuration values are set")
    
    # Test connection
    print("\n2. Testing Connection...")
    try:
        with DatabaseConnector() as db:
            print("   ✓ Connection established successfully!")
            
            # Test simple query
            print("\n3. Testing Simple Query...")
            try:
                cursor = db.connection.cursor()
                cursor.execute("SELECT @@VERSION AS Version")
                version = cursor.fetchone()[0]
                print(f"   ✓ Query executed successfully")
                print(f"   SQL Server Version: {version.split(chr(10))[0]}")
                cursor.close()
            except Exception as e:
                print(f"   ❌ Error executing test query: {e}")
                return False
            
            # Test table access
            print("\n4. Testing Table Access...")
            for table_name in TABLES:
                try:
                    print(f"   Testing table: {table_name}")
                    cursor = db.connection.cursor()
                    cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
                    row = cursor.fetchone()
                    if row:
                        columns = [column[0] for column in cursor.description]
                        print(f"   ✓ Successfully accessed {table_name}")
                        print(f"   Sample row columns: {', '.join(columns[:5])}...")
                        print(f"   First row sample: {dict(zip(columns[:3], row[:3]))}")
                    else:
                        print(f"   ⚠ Table {table_name} exists but is empty")
                    cursor.close()
                except Exception as e:
                    print(f"   ❌ Error accessing {table_name}: {e}")
                    return False
            
            # Test row count
            print("\n5. Testing Row Count...")
            for table_name in TABLES:
                try:
                    cursor = db.connection.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"   ✓ {table_name}: {count:,} rows")
                    cursor.close()
                except Exception as e:
                    print(f"   ❌ Error counting rows in {table_name}: {e}")
            
            print("\n" + "=" * 60)
            print("✓ All tests passed! Database connection is working.")
            print("=" * 60)
            return True
            
    except ImportError as e:
        print(f"\n❌ ERROR: Import failed: {e}")
        print("   Make sure pyodbc is installed: pip install pyodbc")
        return False
    except Exception as e:
        print(f"\n❌ ERROR: Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("  1. Check if ODBC Driver 18 for SQL Server is installed")
        print("  2. Verify SQL Server is accessible from your network")
        print("  3. Check firewall settings")
        print("  4. Verify credentials in .env file")
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)

