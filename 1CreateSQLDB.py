#!/usr/bin/env python3

#To manage the rutes
import sys 
import os
#To work with SQL
import mysql.connector
from mysql.connector import Error

"""
Create the data base structure
Need to input the root password
"""

def create_server_connection(host_name, user_name, user_password): #To connect to MySQL
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query): #To create the Data Base
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

usr = "root"
pw = input("Enter your password for 'root' : ")
connection = create_server_connection("localhost",usr,pw)

#Create the SQL Base on the folder
with open(os.path.join(sys.path[0], "DBStructure.txt"), "r") as DBStructure:
    DBSQ = DBStructure.read()
    DBStructure.close()

create_database(connection, DBSQ)



