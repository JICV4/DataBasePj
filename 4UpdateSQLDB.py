#!/usr/bin/env python3

#To manage the rutes
import sys 
import os
#To work with SQL
import mysql.connector
from mysql.connector import Error
#To manage the data
import pandas as pd
import datetime

"""
Add the Samples information with the environmental data to the MySQL Data Base
Use the file 'DataBaseInfo.txt' with the information of the data bases (This file need to be manually updated)
Empty the FilledDataSet
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

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

pw = input("Enter your password for 'root' : ")
db = "bdbproj"

#Upload the filled data as a pd
FilledDB = pd.read_csv(os.path.join(sys.path[0], "FilledDataSet.csv"),sep='\t',index_col=0,encoding='latin-1') 
FilledDB = FilledDB.dropna()

EnvDF = pd.DataFrame(data=None, index=None, columns=["EnvironmentID","Temperature","MaxTemperature","MinTemperature","Humidity","Precipitation","Rain","SolarRadiation","WindSpeed"], dtype=None, copy=None)
EnvDF = EnvDF.set_index("EnvironmentID")
SpeciesDF = pd.DataFrame(data=None, index=None, columns=["ScientificName","SpecieFamily","SpecieOrder","SpecieClass","SpeciePhylum","SpecieKingdom"], dtype=None, copy=None)
SpeciesDF = SpeciesDF.set_index("ScientificName")
LocationDF =  pd.DataFrame(data=None, index=None, columns=["Location","Country","Continent"] , dtype=None, copy=None)
LocationDF = LocationDF.set_index("Location")

#Get the name of the country in base of the code
Country = {}
Country["BO"] = "Bolivia"
Country["BR"] = "Brazil"
Country["CO"] = "Colombia"
Country["EC"] = "Ecuador"
Country["PE"] = "Peru"

#Get a string for the species table and on the loop create the DF for the other tables
SQLinputSmpl = "INSERT INTO samples VALUES"
for i in range(len(FilledDB)):
    DateFormat = "%s/%s/%s" % (FilledDB.iloc[i,3],FilledDB.iloc[i,4],FilledDB.iloc[i,5])
    event2 = datetime.datetime.strptime(DateFormat, '%d/%m/%Y')
    event1 = event2 - datetime.timedelta(days=1)
    date2 = str(event2)[:10]
    date1 = str(event1)[:10]
    Env2 = str(FilledDB.iloc[i,1])+str(FilledDB.iloc[i,2])+date2
    Env1 = str(FilledDB.iloc[i,1])+str(FilledDB.iloc[i,2])+date1
    Loc = FilledDB.iloc[i,7].replace("Manabi","Manabí")+", "+FilledDB.iloc[i,6] #Correct for accent marks
    if FilledDB.iloc[i,9] == "No Info" : FilledDB.iloc[i,9] = -1
    line = f"\n('{FilledDB.index[i]}','{FilledDB.iloc[i,0]}','{Loc}','{FilledDB.iloc[i,8]}','{FilledDB.iloc[i,1]}','{FilledDB.iloc[i,2]}','{FilledDB.iloc[i,3]}','{FilledDB.iloc[i,4]}','{FilledDB.iloc[i,5]}','{FilledDB.iloc[i,9]}','{Env2}','{Env1}'),"
    SQLinputSmpl += line 

    EnvDF.loc[Env2,:] = [FilledDB.iloc[i,17],FilledDB.iloc[i,15],FilledDB.iloc[i,16],FilledDB.iloc[i,18],FilledDB.iloc[i,19],FilledDB.iloc[i,20],FilledDB.iloc[i,21],FilledDB.iloc[i,22]]
    EnvDF.loc[Env1,:] = [FilledDB.iloc[i,25],FilledDB.iloc[i,23],FilledDB.iloc[i,24],FilledDB.iloc[i,26],FilledDB.iloc[i,27],FilledDB.iloc[i,28],FilledDB.iloc[i,29],FilledDB.iloc[i,30]]
    SpeciesDF.loc[FilledDB.iloc[i,0],:] = [FilledDB.iloc[i,14],FilledDB.iloc[i,13],FilledDB.iloc[i,12],FilledDB.iloc[i,11],FilledDB.iloc[i,10]]
    LocationDF.loc[Loc] = [Country[FilledDB.iloc[i,6]],"South America"] #Add the contry information
SQLinputSmpl= SQLinputSmpl[:-1]+";"

#Loop to create the other tables
SQLinputEnv = "INSERT INTO environmentdata VALUES"
for i in range(len(EnvDF)):
    line = f"\n('{EnvDF.index[i]}',{EnvDF.iloc[i,0]},{EnvDF.iloc[i,1]},{EnvDF.iloc[i,2]},{EnvDF.iloc[i,3]},{EnvDF.iloc[i,4]},{EnvDF.iloc[i,5]},{EnvDF.iloc[i,6]},{EnvDF.iloc[i,7]})," 
    SQLinputEnv += line 
SQLinputEnv= SQLinputEnv[:-1]+";"

SQLinputSpe = "INSERT INTO species VALUES"
for i in range(len(SpeciesDF)):
    line = f"\n('{SpeciesDF.index[i]}','{SpeciesDF.iloc[i,0]}','{SpeciesDF.iloc[i,1]}','{SpeciesDF.iloc[i,2]}','{SpeciesDF.iloc[i,3]}','Animalia')," 
    SQLinputSpe += line 
SQLinputSpe = SQLinputSpe[:-1]+";"

SQLinputLoc = "INSERT INTO locations VALUES"
for i in range(len(LocationDF)):
    line = f"\n('{LocationDF.index[i]}','{LocationDF.iloc[i,0]}','{LocationDF.iloc[i,1]}')," 
    SQLinputLoc += line
SQLinputLoc = SQLinputLoc[:-1]+";"

#Add the info to the data base
connection = create_db_connection("localhost", "root", pw, db)

execute_query(connection, SQLinputEnv)
execute_query(connection, SQLinputSpe)
execute_query(connection, SQLinputLoc)

try:
    DBinfo = open(os.path.join(sys.path[0], "DataBaseInfo.txt"),"r",encoding='latin-1')
    SQLinputDB = DBinfo.read()
    DBinfo.close()
    execute_query(connection, SQLinputDB)
except:
    print("No new Data Base info")

execute_query(connection, SQLinputSmpl)

#Now that the data is on SQL you can empty the filled data base
ResetdDB = FilledDB.drop(FilledDB.index.values.tolist())
print(ResetdDB)
Output = ResetdDB.to_csv(sep="\t", index=True, index_label="gbifID")
with open(os.path.join(sys.path[0], "FilledDataSet.csv"), "w") as thefile:
    thefile.write(Output)
    print("The file with remain data is saved")
thefile.close()