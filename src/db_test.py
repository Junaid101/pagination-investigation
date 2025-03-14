import mysql.connector
import os
from mysql.connector import Error

def test_mysql_connection():
    connection = None
    try:
        # Get connection parameters from environment variables
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT', '3306')  # Default to 3306 if not specified
        database = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        # Check if required environment variables are set
        if not all([host, port, user, password]):
            print("Error: Required environment variables (DB_HOST, DB_USER, DB_PASSWORD) are not set")
            return False
            
        # Create connection config with port and SSL enabled (important for Aiven)
        connection_config = {
            "host": host,
            "port": int(port),  # Convert port to integer
            "user": user,
            "password": password,
            "ssl_disabled": False,  # Make sure SSL is enabled
            "connect_timeout": 10   # Increase timeout for troubleshooting
        }
        
        # Add database if specified
        if database:
            connection_config["database"] = database
        
        print(f"Attempting to connect to {host}:{port} as {user}...")
        
        # Establish connection
        connection = mysql.connector.connect(**connection_config)
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            
            if database:
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print(f"You're connected to database: {record[0]}")
            else:
                print("Connected to MySQL server (no database selected)")
                
            # Run a simple test query
            cursor.execute("SELECT 1 + 1 AS solution")
            result = cursor.fetchone()
            print(f"Test query result: {result[0]}")
                
            return True

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        
        # Additional troubleshooting for common errors
        if "2003" in str(e):
            print("\nTroubleshooting tips:")
            print("1. Verify the hostname and port are correct")
            print("2. Check if the server is running and accepting connections")
            print("3. Ensure your network/firewall allows connections to this host and port")
            print("4. For Aiven, verify you've whitelisted your IP address in the Aiven console")
            print("5. Try connecting using the mysql command line client to test basic connectivity")
            
        return False
    finally:
        if connection and connection.is_connected():
            if 'cursor' in locals():
                cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    # Make sure to set DB_PORT environment variable if needed
    # If not set, it will default to 3306
    test_mysql_connection()