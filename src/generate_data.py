import mysql.connector
from faker import Faker
import time
from datetime import datetime, timedelta
import random
import string

fake = Faker()

def drop_users_table(db_connection: mysql.connector.CMySQLConnection):
    """
    Drop the users table.
    
    Args:
        db_connection: MySQL connection object
    """
    try:
        cursor = db_connection.cursor()
                # Create the users table
        cursor.execute("""
                DROP TABLE users 
            """)
        print(f"User table dropped successfully")
    except Exception as e:
        print(f"Error deleting table: {e}")
        db_connection.rollback()
        raise
    finally:
        cursor.close()


def ensure_users_table_exists(db_connection: mysql.connector.CMySQLConnection):
    """
    Check if the users table exists, and create it if it doesn't.
    
    Args:
        db_connection: MySQL connection object
    """
    cursor = db_connection.cursor()
    try:
        # Check if table exists
        cursor.execute("SHOW TABLES LIKE 'users'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            print("Table 'users' does not exist. Creating it now...")
            
            # Create the users table
            cursor.execute("""
                CREATE TABLE users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    gender ENUM('Male', 'Female') NOT NULL,
                    phone VARCHAR(30) NOT NULL,
                    address TEXT NOT NULL,
                    username VARCHAR(50) NOT NULL UNIQUE,
                    date_of_birth DATE NOT NULL,
                    signup_date DATE NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)            
            db_connection.commit()
            print("Table 'users' created successfully.")
        else:
            print("Table 'users' already exists.")
    except Exception as e:
        print(f"Error checking/creating table: {e}")
        db_connection.rollback()
        raise
    finally:
        cursor.close()

def generate_users(db_connection: mysql.connector.CMySQLConnection, num_users, batch_size=200):
    """
    Generate fake users with batched inserts to improve performance.
    
    Args:
        db_connection: MySQL connection object
        num_users: Total number of users to generate
        batch_size: Number of users to insert in each batch
    """

    # First ensure the table exists
    ensure_users_table_exists(db_connection)
    
    cursor = db_connection.cursor()
    start_time = time.time()
    total_batches = (num_users + batch_size - 1) // batch_size  # Ceiling division
    
    print(f"Starting generation of {num_users} users with batch size {batch_size}")
    print(f"Will process {total_batches} batches")
    
    try:
        # Process users in batches
        for batch_num in range(total_batches):
            batch_start = time.time()
            
            # Calculate how many users to generate in this batch
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, num_users)
            current_batch_size = end_idx - start_idx
            
            # Prepare batch of values for insertion
            values = []


            for _ in range(current_batch_size):
                while True:  # Keep retrying until a unique email & username is found
                    try:
                        name = fake.name()
                        email = fake.email()
                        gender = random.choice(['Male', 'Female'])
                        phone = fake.phone_number()
                        address = fake.address()
                        username = fake.user_name()

                        # Generate a random date of birth for an adult (18-90 years old)
                        today = datetime.now().date()
                        days_to_subtract = random.randint(18 * 365, 90 * 365)
                        date_of_birth = today - timedelta(days=days_to_subtract)
                        dob_string = date_of_birth.strftime('%Y-%m-%d')

                        # Generate a random signup date in the last 5 years
                        days_since_signup = random.randint(0, 5 * 365)
                        signup_date = today - timedelta(days=days_since_signup)
                        signup_date_string = signup_date.strftime('%Y-%m-%d')

                        # Randomly determine if the user is active (80% chance of being active)
                        is_active = random.choices([True, False], weights=[80, 20])[0]

                        # Insert directly into the database
                        cursor.execute(
                            """
                            INSERT INTO users (name, email, gender, phone, address, username, date_of_birth, signup_date, is_active)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (name, email, gender, phone, address, username, dob_string, signup_date_string, is_active)
                        )

                        # Commit immediately to catch duplicate key errors early
                        db_connection.commit()

                        break  # Exit the while loop if insert is successful

                    except mysql.connector.errors.IntegrityError as e:
                        # If the error is a duplicate key, retry with a new email & username
                        if "Duplicate entry" in str(e):
                            continue
                        else:
                            raise  # Raise other database errors
                        
            # Calculate and log batch statistics
            batch_end = time.time()
            batch_time = batch_end - batch_start
            total_time = batch_end - start_time
            users_inserted = end_idx
            percent_complete = (users_inserted / num_users) * 100
            
            print(f"Batch {batch_num+1}/{total_batches} completed: {current_batch_size} users inserted in {batch_time:.2f}s")
            print(f"Progress: {users_inserted}/{num_users} users ({percent_complete:.1f}%) - Elapsed time: {total_time:.2f}s")
            
            # Estimate remaining time
            if batch_num > 0:  # Skip estimation for first batch
                avg_time_per_user = total_time / users_inserted
                remaining_users = num_users - users_inserted
                est_remaining_time = avg_time_per_user * remaining_users
                print(f"Estimated time remaining: {est_remaining_time:.2f}s")
            
            print("-" * 40)
    
    except Exception as e:
        # Roll back in case of error
        db_connection.rollback()
        print(f"Error during batch insert: {str(e)}")
        raise
    finally:
        # Close cursor
        cursor.close()
    
    # Log completion statistics
    end_time = time.time()
    total_runtime = end_time - start_time
    insert_rate = num_users / total_runtime if total_runtime > 0 else 0
    
    print(f"Completed generating {num_users} users in {total_runtime:.2f}s")
    print(f"Average insert rate: {insert_rate:.2f} users/second")

# Example usage
if __name__ == "__main__":
    # This is just an example - replace with your actual connection
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    
    db_connection = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
        port=int(os.getenv('DB_PORT', '3306'))
    )
    
    # Generate 1000 users with batch size of 100
    generate_users(db_connection, 1000, 100)
    
    db_connection.close()