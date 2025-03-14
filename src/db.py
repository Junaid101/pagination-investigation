import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv('.env')

def get_db_connection():
    # Load environment variables from .env file
    load_dotenv()

    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=int(os.getenv('DB_PORT', '3306'))  # Added port with default
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: Could not load environment")
        print(f"Error: {e}")
        return None

def test_connection():
    """
    Test the database connection by connecting and executing a simple query.
    Returns True if successful, False otherwise.
    """
    connection = None
    try:
        # Get connection
        connection = get_db_connection()
        
        if connection is None:
            return False
            
        if connection.is_connected():
            # Get server info
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            
            # Get database name
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()[0]
            print(f"Connected to database: {db_name}")
            
            # Test query
            cursor.execute("SELECT 1 + 1 AS result;")
            result = cursor.fetchone()[0]
            print(f"Test query result: {result}")
            
            cursor.close()
            return True
    except Error as e:
        print(f"Error while testing connection: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed")

# Example usage
if __name__ == "__main__":
    if test_connection():
        print("Connection test successful!")
    else:
        print("Connection test failed!")