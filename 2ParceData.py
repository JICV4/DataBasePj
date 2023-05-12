#!/usr/bin/env python3

#To manage the rutes and tables files
import sys 
import os
import pandas as pd

#To work with SQL
import pandas as pd

"""
Parse the Downloaded data and fill the missing information
Store the new data on ParsedDataSet
"""

#Create load the DataBase to work with
DataSet = pd.read_csv(os.path.join(sys.path[0],'DownLoadData.csv'),sep='\t',dtype = str) #Has all the info

for x in DataSet.columns : #The name used for elevation is variable, pick the first column that have the "levation" string, as normally the main name is used first
    if "levation" in x: 
        Altitute = x
        break

#Retrive the most relevant info available for the data base identification
if "datasetKey" in DataSet.columns : DataSetName = "datasetKey" 
elif "datasetName" in DataSet.columns : DataSetName = "datasetName" 

WorkingDF = DataSet[["gbifID","scientificName","decimalLatitude","decimalLongitude","day","month","year","countryCode","stateProvince",DataSetName,Altitute,"kingdom","phylum","class","order","family"]] #Keep the variables you are interested
WorkingDF[Altitute] = WorkingDF[Altitute].fillna(-1) #If the sample has no altitute store as a negative value
WorkingDF["stateProvince"] = WorkingDF["stateProvince"].fillna("Unknown") #If sample has no info of the place store as Unknown
WorkingDF = WorkingDF.dropna() #Drop samples with missing info
WorkingDF = WorkingDF.set_index("gbifID")

#Standardize col names
WorkingDF.rename(columns = {DataSetName:'DataSetID/Name', Altitute:'Elevation'}, inplace = True)
print(WorkingDF)

#Standardize species nomenclature
for x in range(len(WorkingDF)):
    A = WorkingDF.iloc[x,0].split(" ")
    if len(A) == 1:
        F = A[0] + " sp."
    else:
        F = A[0]+" "+A[1].replace(",","")
    WorkingDF.iloc[x,0] = F

#Save the parced data
DFOutput = WorkingDF.to_csv(sep="\t", header=False, index=True, index_label="gbifID")
with open(os.path.join(sys.path[0], "ParsedDataSet.csv"), "a") as thefile:
    thefile.write(DFOutput)
    print("The file is saved")
thefile.close()


