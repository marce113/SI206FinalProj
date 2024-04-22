import sqlite3
import csv
import json
import os
import unittest
import requests
import re
from datetime import datetime


'''In this File we will get the data from our API's and save them into a shared data base.
    we want to see how the response times for fire between the LAFD and NYCFD differ during similar times in the day.
    How does the time of day influence the response time for these fire departments? 

    we want to create three tables. One table will be for Structural fires in NYC, another will be for non structural fires in NYC and the other will be for fires in LA. 
    Each table will have 100 entries
    We will not be including forest fires in our data set.
    Each table will have an ID for the fire, a response time in minutes (resonse time is how long it took for first responders to arrive
    to the scene from when the call was made), and the time that the call was made. 
'''

#pulls from APi and creats a Json with all the data about structural fires
def get_fire_data(classification_group):
    '''
    creates API request
    creates json with all the data from request 
    '''
    base_url = "https://data.cityofnewyork.us/resource/8m42-w767.json"
    response = requests.get(base_url, params= {"incident_classification_group": classification_group})
    print (response.status_code)

    if response.status_code == 200:
        data = response.json()
 
        try:
            with open("NYC_data.json", "r") as json_file:
                existing_data = json.load(json_file)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            existing_data = []
 
        existing_data.append(data)
 
        with open("NYC_data.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
            print("Data appended to NYC_data.json file.")
    else:
        print("Failed to retrieve data from the API. Status code:", response.status_code)


#create Database
#pulls data from APi and puts it into database
#using code from Discussion 12
def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

    #create table for structural fires in NYC 
    #calculate response time based on first on scene and incident time 
    #save processed table 
def create_first_table(cur, conn):
    cur.execute(
        '''
            CREATE TABLE IF NOT EXISTS "Fires" (
                "Fire_id" INTEGER PRIMARY KEY, 
                "Date" TEXT,
                "Time" TEXT, 
                "Response_time" INTEGER
                '''
            )
    conn.commit()

#Function to add Structural Fires 
def add_fires_from_json(filename, cur, conn):
    with open(filename) as f:
        json_data = json.load(f)

    valid_fires = []  # List to store fires with a valid response time
    fire_counter = 0  # Counter to track the number of entries uploaded

    # Retrieve the maximum Fire_id from the database
    cur.execute("SELECT MAX(Fire_id) FROM Fires")
    max_fire_id = cur.fetchone()[0]  # Get the maximum Fire_id, or None if there are no entries

    if max_fire_id is None:
        Fire_id = 1  # Start Fire_id from 1 if the database is empty
    else:
        Fire_id = max_fire_id + 1  # Start Fire_id from the maximum value plus one

    # Add valid fire incidents to the list
    for data in json_data:
        if data.get("valid_incident_rspns_time_indc") == "Y":
            valid_fires.append(data)
            fire_counter += 1

            if fire_counter % 25 == 0:
                # Now 'valid_fires' contains only the dictionaries with "valid_incident_rspns_time_indc" equal to "Y"
                for fire in valid_fires:
                    Date = fire["first_activation_datetime"].split("T")[0]
                    Time = fire["first_activation_datetime"].split("T")[1]
                    Response_time = float(fire["incident_response_seconds_qy"]) / 60

                    cur.execute('''
                        INSERT INTO Fires (
                            Fire_id, Date, Time, Response_Time) 
                            VALUES(?,?,?,?)''',
                                (Fire_id, Date, Time, Response_time)
                            )
                    Fire_id += 1

                conn.commit()

                # Reset the valid_fires list for the next batch
                valid_fires = []

            # Break out of the loop if we have reached 100 entries
            if fire_counter >= 100:
                break

    return fire_counter  # Return the total number of entries uploaded
    



#step 2 

def main():
    NonStructural_Fires_url = "https://data.cityofnewyork.us/resource/8m42-w767.json?incident_classification_group=NonStructural Fires"
    get_fire_data("Structural Fires")
    get_fire_data("NonStructural Fires")

    try:
        cur, conn = set_up_database("fire_data.db")
        create_first_table(cur, conn)
        total_entries = 0
        while total_entries < 100:
            total_entries += add_fires_from_json("NYC_data.json", cur, conn)
    except Exception as e:
        print("an error has occured:", e)


if __name__ == "__main__":
    main()

