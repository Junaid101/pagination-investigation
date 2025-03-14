import mysql.connector
from db import get_db_connection, test_connection
from generate_data import generate_users, drop_users_table
from dotenv import load_dotenv
from db_test import test_mysql_connection

def main():
    load_dotenv()

    test_connection()
    
    # Establish database connection
    connection = get_db_connection()
    
    try:
        # Generate and insert 1 million users
        generate_users(connection, 1000000)
    finally:
        # Close the database connection
        connection.close()

def drop_table():
    load_dotenv()

    test_connection()
    
    # Establish database connection
    connection = get_db_connection()
    
    try:
        # Generate and insert 1 million users
        drop_users_table(connection)
    finally:
        # Close the database connection
        connection.close()


if __name__ == "__main__":
    # drop_table()
    main()