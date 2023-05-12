#!/usr/bin/env python3

#To manage the rutes and tables files
import sys 
import os
import pandas as pd
#To easy work with dates
import datetime
#For API
import requests
import io #To read df from a csv string

"""
Add the environmental information from the samples on ParsedDataSet using Vissual Crossing Historical weather data API service
A personal key for the Query is on the file
Limit on daily calls
Store the new data on FilledDataSet 
Remove the Samples with a successfull query from the ParsedDataSet
"""

def requestAPI(linkAPI):
    """
    Retrive the information from the API service
    Show some advices of possible errors
    """
    responses = {}
    responses[200] = "Everything went okay, and the result has been returned (if any)."
    responses[301] = "The server is redirecting you to a different endpoint. This can happen when a company switches domain names, or an endpoint name is changed."
    responses[400] = "The server thinks you made a bad request. This can happen when you do not send along the right data, among other things."
    responses[401] = "The server thinks you are not authenticated. Many APIs require login ccredentials, so this happens when you do not send the right credentials to access an API."
    responses[403] = "The resource you are trying to access is forbidden: you do not have the right perlessons to see it."
    responses[404] = "The resource you tried to access was not found on the server."
    responses[503] = "The server is not ready to handle the request."
    with requests.get(linkAPI,stream=True) as response:
        codeget = response.status_code
        print(codeget,responses[codeget])
        if codeget == 200:
            textoutput = response.content.decode('utf-8')
            print("Data recived")
            return textoutput   
        else: 
            print("API was not able to retrive this information")


#Parce the information retrived from the API
DBDF = pd.read_csv(os.path.join(sys.path[0],'ParsedDataSet.csv'),sep='\t',index_col=0,encoding='latin-1') 
if len(DBDF) == 0:
    print("No more samples to complete the information")
else:
    print("Your original data set:")
    print(DBDF)

    #List to store the data obtained
    Good = [] #ids of samples well done
    APIerror = [] #ids of samples with problems, maybe missing information or problems with API
    GoodData = ["gbifID","MaxTemp1","MinTemp1","Temp1","Humi1","Prec1","Rain1","SolarRad1","WindSpeed1","MaxTemp0","MinTemp0","Temp0","Humi0","Prec0","Rain0","SolarRad0","WindSpeed0"]
    GoodDF = pd.DataFrame(data=None, index=None, columns=GoodData, dtype=None, copy=None)
    GoodDF = GoodDF.set_index("gbifID")

    n = 0
    for i in range(len(DBDF)): #To get the data from the API, limited to 100 request per day
        possition = "%s,%s" % (DBDF.iloc[i,1],DBDF.iloc[i,2])
        DateFormat = "%s/%s/%s" % (DBDF.iloc[i,3],DBDF.iloc[i,4],DBDF.iloc[i,5])
        event2 = datetime.datetime.strptime(DateFormat, '%d/%m/%Y')
        event1 = event2 - datetime.timedelta(days=1)
        VCKey = "P2JB9VCV4JPUN4EUHPT6T8L4K"
        linkAPI = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/%s/%s/%s?unitGroup=metric&include=days&key=%s&contentType=csv" % (possition,str(event1)[:10],str(event2)[:10],VCKey)
        print(linkAPI)
        try: #Try to get the info
            APItxt = requestAPI(linkAPI) 
            APIex = pd.read_csv(io.StringIO(APItxt),sep=',')
            print("You get from API:")
            print(APIex)

            try: #Try to append the data
                GBIFid = DBDF.index[i]
                MaxTemp1 = APIex.iloc[1,2]
                MinTemp1 = APIex.iloc[1,3]
                Temp1 = APIex.iloc[1,4]
                Humi1 = APIex.iloc[1,9]
                Prec1 = APIex.iloc[1,10]
                if "Rain" in APIex.iloc[1,29]:
                    Rain1 = 1
                else:
                    Rain1 = 0
                WindSpeed1 = APIex.iloc[1,17]
                SolarRad1 = APIex.iloc[1,22]
                MaxTemp0 = APIex.iloc[0,2]
                MinTemp0 = APIex.iloc[0,3]
                Temp0 = APIex.iloc[0,4]
                Humi0 = APIex.iloc[0,9]
                Prec0 = APIex.iloc[0,10]
                if "Rain" in APIex.iloc[0,29]:
                    Rain0 = 1
                else:
                    Rain0 = 0
                WindSpeed0 = APIex.iloc[0,17]
                SolarRad0 = APIex.iloc[0,22]
                NextRow = [MaxTemp1,MinTemp1,Temp1,Humi1,Prec1,Rain1,WindSpeed1,SolarRad1,MaxTemp0,MinTemp0,Temp0,Humi0,Prec0,Rain0,WindSpeed0,SolarRad0]
                print("Filled data for :",GBIFid)
                print(NextRow)
                GoodDF.loc[GBIFid,:] = NextRow
                Good.append(GBIFid) #Only when everything worked append
            except: 
                APIerror.append(GBIFid) #If for some reason the data is not appendable, add it to a problem list
                with open(os.path.join(sys.path[0], "APIerrors.csv"), "a") as thefile:
                    errortxt = "%s\t%s\n%s\n" % (GBIFid,linkAPI,APItxt)
                    thefile.write(errortxt)
                    print("Error on sample",GBIFid,"The data is appended on APIerrors.csv")
                thefile.close()

        except: #If you cant maybe the API is full
            n += 1
            print("The file with GBIF id",DBDF.index[i],"was not retrived")
        if n == 5: #After several errors break the loop
            print("Maximum number of request")
            break
        
    #Use the ids to merge the parsed data with the API data
    DBDFfilled = pd.merge(DBDF, GoodDF, left_index=True, right_index=True)
    print("The merged data is:")
    print(DBDFfilled)

    #Save the Good merged data as a csv
    DFOutput = DBDFfilled.to_csv(sep="\t", header=False, index=True, index_label="gbifID")
    with open(os.path.join(sys.path[0], "FilledDataSet.csv"), "a") as thefile:
        thefile.write(DFOutput)
        print("The file is saved")
    thefile.close()

    #Over write the parced DB, eliminate the samples (use IDs) you work with ()
    Workedids = Good + APIerror 
    NewDBDF = DBDF.drop(Workedids)
    print(NewDBDF)
    NewOutput = NewDBDF.to_csv(sep="\t", index=True, index_label="gbifID")
    with open(os.path.join(sys.path[0], "ParsedDataSet.csv"), "w") as thefile:
        thefile.write(NewOutput)
        print("The file with remain data is saved")
    thefile.close()
