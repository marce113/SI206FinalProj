import sqlite3
import csv
import json
import os
import unittest
import requests
import re


'''In this File we will get the data from our API's and save them into a shared data base.
    we want to see how the response times for fire between the LAFD and NYCFD differ during similar times in the day.
    How does the time of day influence the response time for these fire departments? 

    we want to create three tables. One table will be for Structural fires in NYC, another will be for non structural fires in NYC and the other will be for fires in LA. 
    Each table will have 100 entries
    We will not be including forest fires in our data set.
    Each table will have an ID for the fire, a response time in minutes (resonse time is how long it took for first responders to arrive
    to the scene from when the call was made), and the time that the call was made. 
'''



#create Database
#pulls data from APi and puts it into database
#using code from Discussion 12
def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

    #create table for structural fires in NYC 
    #filter clasification by contains Incident type  structural fire
    #calculate response time based on first on scene and incident time 
    #save processed table 
def create_first_table(cur, conn):
    cur.execute(
        '''
            CREATE TABLE IF NOT EXISTS "StructFires" (
                "fire_id" INTEGER PRIMARY KEY, 
                "Date" TEXT,
                "Time" NUMBER, 
                "Response_time" INTEGER,
                "neighborhood" Text,
                '''
            )
    conn.commit()


#pulls from APi and creats a Json with all the data about structural fires
def get_struct_fire_data():
    '''
    creates API request
    creates json with all the data from request 
    '''
    base_url = "https://data.cityofnewyork.us/resource/8m42-w767.json?incident_classification_group=Structural Fires"
    response = requests.get(base_url)

    if response.status_code == 200:
        data = response.json()
 
        try:
            with open("data.json", "r") as json_file:
                existing_data = json.load(json_file)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            existing_data = []
 
        existing_data.append(data)
 
        with open("data.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
            print("Data appended to data.json file.")
    else:
        print("Failed to retrieve data from the API. Status code:", response.status_code)


#step 2 

def main():
    NonStructural_Fires_url = "https://data.cityofnewyork.us/resource/8m42-w767.json?incident_classification_group=NonStructural Fires"
    get_struct_fire_data()




