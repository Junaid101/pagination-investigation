# Python MySQL User Data Generator

This project is a Python application that connects to a MySQL database and generates random user data. It inserts a specified number of user records into the database, making it useful for testing and development purposes.

## Project Structure

```
python-mysql-app
├── src
│   ├── app.py          # Entry point of the application
│   ├── db.py           # Database connection logic
│   └── generate_data.py # User data generation logic
├── requirements.txt     # Project dependencies
└── README.md            # Project documentation
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd python-mysql-app
   ```

2. **Install dependencies:**
   Make sure you have Python installed. Then, install the required packages using pip:
   ```
   pip install -r requirements.txt
   ```

3. **Configure the Database:**
   Ensure you have a MySQL database set up. Update the database connection details in `src/db.py` to match your MySQL configuration.

## Usage

To generate and insert user data into the database, run the following command:
```
python src/app.py
```

You can modify the number of users to generate by changing the parameter in the `generate_users` function within `src/generate_data.py`.

## Dependencies

- `mysql-connector-python`: A MySQL driver for Python.
- `Faker`: A library for generating fake data.

## License

This project is licensed under the MIT License.