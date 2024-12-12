from flask import Flask, jsonify
import pyodbc
import os

app = Flask(__name__)

# Define the SQL Server connection string
conn_str = r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=xxxxxxxx;DATABASE=AQOA_Core;UID=xxxxxx;PWD=xxxxxxx'


# Function to test the database connection
def test_db_connection():
    try:
        # Try to establish the database connection
        conn = pyodbc.connect(conn_str)
        conn.close()  # Close the connection immediately if successful
        return True  # Return True if connection is successful
    except pyodbc.Error as e:
        # Log the error if connection fails
        print(f"Database connection failed: {e}")
        return False  # Return False if connection fails


# Route for the root URL
@app.route('/')
def home():
    # Test the connection each time the route is accessed
    if test_db_connection():
        return "Database connection successful!"
    else:
        return "Database connection failed!", 500


def get_data_from_sql():
    try:
        # Read the SQL query from the file
        sql_file_path = os.path.join(os.path.dirname(__file__), 'ikosoft.sql')

        with open(sql_file_path, 'r') as file:
            query = file.read().strip()  # Read and strip any extra whitespace/newlines

        # Establish the database connection
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch all results
        rows = cursor.fetchall()

        # Convert rows to a list of dictionaries
        columns = [column[0] for column in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]

        # Close resources
        cursor.close()
        conn.close()

        return data

    except Exception as e:
        print(f"Error: {e}")
        return None


@app.route('/ikosoftData', methods=['GET'])
def get_data():
    # Call the function to get data
    data = get_data_from_sql()


    if not data:
        return jsonify({"message": "No data found"}), 404

    # Return the data in JSON format
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
