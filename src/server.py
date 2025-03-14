from werkzeug.security import generate_password_hash
from db import get_db_connection
from user_repository import UserRepository

def main():
    # Use the provided get_db_connection function
    db_connection = get_db_connection()

    if db_connection:
        # Initialize the repository
        user_repo = UserRepository(db_connection)
        
        # Add a new user
        new_user_id = user_repo.add_user(
            username="johndoe",
            email="john.doe@example.com",
            full_name="John Doe",
            password_hash=generate_password_hash("secure_password")
        )
        
        # Get a user by ID
        user = user_repo.get_user_by_id(new_user_id)
        print(f"User created: {user['username']}")
        
        # Edit user information
        user_repo.edit_user(new_user_id, email="john.updated@example.com")
        
        # List users
        all_users = user_repo.list_users()
        for user in all_users:
            print(f"User: {user['username']} ({user['email']})")
        
        # Search for users
        search_results = user_repo.search_users("john")
        
        # Close the connection when done
        db_connection.close()
    else:
        print("Failed to connect to the database")