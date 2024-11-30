import mysql.connector
from mysql.connector import Error

db_servers = [
{
    'host': '192.168.200.34',
    'user': 'app_user',
    'password': '1234',
    'database': 'test_db'},
{
    'host': '192.168.200.157',
    'user': 'app_user',
    'password': '1234',
    'database': 'test_db'}
]

def connect_to_db():
    for server in db_servers:
        try:
            connection = mysql.connector.connect(host=server['host'],
                                                 user=server['user'],
                                                 password=server['password'],
                                                 database=server['database'])
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)
        except Error as e:
            print("Error while connecting to MySQL", e)
    print("All database servers are down.")
    return None

def execute_with_failover(operation, *args):
    """
    Execute a database operation with failover.
    Tries all available servers until an operation succeeds.
    """
    for server in db_servers:
        try:
            connection = mysql.connector.connect(
                host=server["host"],
                user=server["user"],
                password=server["password"],
                database=server["database"]
            )
            if connection.is_connected():
                print(f"Performing operation on {server['host']}")
                operation(connection, *args)
                connection.close()
                return
        except Error as e:
            print(f"Failed to execute operation on {server['host']}: {e}")
    print("Operation failed: All servers are down.")

def create_user(connection, name, email):
    try:
        cursor = connection.cursor()
        query = "INSERT INTO users (name, email) VALUES (%s, %s)"
        cursor.execute(query, (name, email))
        connection.commit()
        print(f"User {name} created successfully")
    except Error as e:
        print(f"Error while creating user: {e}")

def read_users(connection):
    try:
        cursor = connection.cursor()
        query = "SELECT * FROM users"
        cursor.execute(query)
        users = cursor.fetchall()
        for user in users:
            print(user)
    except Error as e:
        print(f"Error while reading data: {e}")

def update_user_email(connection, user_id, new_email):
    try:
        cursor = connection.cursor()
        query = "UPDATE users SET  email = %s WHERE id = %s"
        cursor.execute(query, (new_email, user_id))
        connection.commit()
        print(f"User {user_id} email updated to {new_email} successfully")
    except Error as e:
        print(f"Error while updating user: {e}")

def delete_user(connection, user_id):
    try:
        cursor = connection.cursor()
        query = "DELETE FROM users WHERE id = %s"
        cursor.execute(query, (user_id,))
        connection.commit()
        print(f"User {user_id} deleted successfully")
    except Error as e:
        print(f"Error while deleting user: {e}")

if __name__ == '__main__':
    execute_with_failover(create_user,"tanjim", "tanjim@example.com" )
    execute_with_failover(read_users)
    
