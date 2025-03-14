class UserRepository:
    def __init__(self, db_connection):
        """
        Initialize the UserRepository with a database connection.
        
        Args:
            db_connection: A MySQL database connection object
        """
        self.connection = db_connection
        self._create_users_table_if_not_exists()
    
    def _create_users_table_if_not_exists(self):
        """Create the users table if it doesn't already exist."""
        try:
            cursor = self.connection.cursor()
            query = """
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                full_name VARCHAR(100),
                password_hash VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
            """
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
        except Error as e:
            print(f"Error creating users table: {e}")
    
    def add_user(self, username, email, full_name, password_hash):
        """
        Add a new user to the database.
        
        Args:
            username: Unique username for the user
            email: User's email address
            full_name: User's full name
            password_hash: Hashed password for the user
            
        Returns:
            The ID of the newly created user, or None if an error occurred
        """
        try:
            cursor = self.connection.cursor()
            query = """
            INSERT INTO users (username, email, full_name, password_hash)
            VALUES (%s, %s, %s, %s)
            """
            values = (username, email, full_name, password_hash)
            cursor.execute(query, values)
            self.connection.commit()
            user_id = cursor.lastrowid
            cursor.close()
            return user_id
        except Error as e:
            print(f"Error adding user: {e}")
            return None
    
    def get_user_by_id(self, user_id):
        """
        Retrieve a user by their ID.
        
        Args:
            user_id: The ID of the user to retrieve
            
        Returns:
            A dictionary containing user information, or None if not found
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = "SELECT * FROM users WHERE id = %s"
            cursor.execute(query, (user_id,))
            user = cursor.fetchone()
            cursor.close()
            return user
        except Error as e:
            print(f"Error retrieving user: {e}")
            return None
    
    def get_user_by_username(self, username):
        """
        Retrieve a user by their username.
        
        Args:
            username: The username of the user to retrieve
            
        Returns:
            A dictionary containing user information, or None if not found
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = "SELECT * FROM users WHERE username = %s"
            cursor.execute(query, (username,))
            user = cursor.fetchone()
            cursor.close()
            return user
        except Error as e:
            print(f"Error retrieving user: {e}")
            return None
    
    def edit_user(self, user_id, **kwargs):
        """
        Update user information.
        
        Args:
            user_id: The ID of the user to update
            **kwargs: Fields to update (email, full_name, password_hash, is_active)
            
        Returns:
            True if successful, False otherwise
        """
        allowed_fields = {'email', 'full_name', 'password_hash', 'is_active'}
        update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not update_fields:
            return False
        
        try:
            cursor = self.connection.cursor()
            set_clause = ", ".join([f"{field} = %s" for field in update_fields.keys()])
            query = f"UPDATE users SET {set_clause} WHERE id = %s"
            values = list(update_fields.values()) + [user_id]
            cursor.execute(query, values)
            self.connection.commit()
            success = cursor.rowcount > 0
            cursor.close()
            return success
        except Error as e:
            print(f"Error updating user: {e}")
            return False
    
    def list_users(self, limit=100, offset=0, active_only=True):
        """
        List users with pagination.
        
        Args:
            limit: Maximum number of users to return
            offset: Number of users to skip
            active_only: If True, only return active users
            
        Returns:
            A list of dictionaries containing user information
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = "SELECT id, username, email, full_name, created_at, updated_at, is_active FROM users"
            
            if active_only:
                query += " WHERE is_active = TRUE"
                
            query += " ORDER BY id ASC LIMIT %s OFFSET %s"
            cursor.execute(query, (limit, offset))
            users = cursor.fetchall()
            cursor.close()
            return users
        except Error as e:
            print(f"Error listing users: {e}")
            return []
    
    def delete_user(self, user_id):
        """
        Delete a user by their ID.
        
        Args:
            user_id: The ID of the user to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            query = "DELETE FROM users WHERE id = %s"
            cursor.execute(query, (user_id,))
            self.connection.commit()
            success = cursor.rowcount > 0
            cursor.close()
            return success
        except Error as e:
            print(f"Error deleting user: {e}")
            return False
    
    def deactivate_user(self, user_id):
        """
        Deactivate a user instead of deleting them.
        
        Args:
            user_id: The ID of the user to deactivate
            
        Returns:
            True if successful, False otherwise
        """
        return self.edit_user(user_id, is_active=False)
    
    def activate_user(self, user_id):
        """
        Activate a previously deactivated user.
        
        Args:
            user_id: The ID of the user to activate
            
        Returns:
            True if successful, False otherwise
        """
        return self.edit_user(user_id, is_active=True)
    
    def search_users(self, search_term):
        """
        Search for users by username, email, or full name.
        
        Args:
            search_term: The term to search for
            
        Returns:
            A list of matching users
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = """
            SELECT id, username, email, full_name, created_at, updated_at, is_active 
            FROM users 
            WHERE username LIKE %s OR email LIKE %s OR full_name LIKE %s
            """
            search_pattern = f"%{search_term}%"
            cursor.execute(query, (search_pattern, search_pattern, search_pattern))
            users = cursor.fetchall()
            cursor.close()
            return users
        except Error as e:
            print(f"Error searching users: {e}")
            return []