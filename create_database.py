"""
This module is to create a database
"""
from mysql.connector import connect, Error

try:
    conn = connect(host='localhost', user='root',
                   password='yogesh1304')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE fake_db")
        print("Database created")
except Error as error_e:
    print("Error while connecting to MySQL", error_e)
