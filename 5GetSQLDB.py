#!/usr/bin/env python3

#To manage the rutes
import sys 
import os
#To work with SQL
import mysql.connector
from mysql.connector import Error
#To manage the data and plot
import pandas as pd
import folium
import json

"""
Get geographic and environmental information of GBIF samples on the data base by species
Requieres connection to SQL data base 
Use a SQL query format to access the data
Note: In case code editor doesnt support '.html' the map is saved on the folder
"""

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection



usr = "root"
pw = input("Enter your password for 'root' : ")
db = "bdbproj"

connection = create_db_connection("localhost", "root", pw, db) #Connect to the data base

while True :
    print("Enter your query on SQL format (Example bellow) or 'Exit' to leave")
    print("EX :\nselect * from samples,environmentdata where samples.ScientificName = 'Adenomera andreae' and  samples.DayEnvData = environmentdata.EnvironmentID;")
    q1 = input()
    if q1 == "Exit" : break 

    results = read_query(connection, q1)

    #Get the retrived coordinates from the data base
    Coordinates = [] #Longitud y latitud
    EnvitID = []
    for result in results:
        rlist = list(result)
        Coordinates.append([rlist[4],rlist[5]])
        EnvitID.append([rlist[13],rlist[14],rlist[15],rlist[16],rlist[17],rlist[18],rlist[19],rlist[20]])

    EnvData = pd.DataFrame(EnvitID,columns=["Temp","MaxTem","MinTemp","Hum","Prec","Rain","SRad","WndSp"])
    print("\nEnvironmental information by sample:")
    print(EnvData)
    print("\nSummary of the environmental information of the species:")
    print(EnvData.describe())


    fig = folium.Map(width=900, height=600)
    geo_json_map = json.load(open(os.path.join(sys.path[0], "Map.geojson")))
    folium.Choropleth(
        geo_data = geo_json_map,
        fill_color = "steelblue",
        fill_opacity = 0.4,
        line_color = "steelblue",
        line_opacity = 0.9
    ).add_to(fig)

    fig #To show the map 
    #Note: In case code editor doesnt support html the map is saved on the folder

    for point in range(0, len(Coordinates)):
        folium.Marker(Coordinates[point]).add_to(fig)

    fig.save(os.path.join(sys.path[0], "Map.html"))
    print("\nThe map with the locations is saved on the local folder as 'Map.html' \n\n\n")

print("Close")
