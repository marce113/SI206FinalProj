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
def create_first_nyc_table(cur, conn):
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS "NYC_Fires" (
            "Fire_id" INTEGER PRIMARY KEY, 
            "Date" TEXT,
            "Time" TEXT, 
            "Response_time" FLOAT
        )
        '''
    )
    conn.commit()

def create_neighborhood_table(cur, conn):
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS "Neighborhoods" (
            "Neighborhood_id" INTEGER PRIMARY KEY, 
            "Nighborhood" TEXT,
        )
        '''
    )
    conn.commit()


def insert_data_to_fires_table(cur, conn, json_data):
    batch_size = 25
    total_entries = 0  # initializes total entries processed

    for i in range(0, 101, batch_size):
        batch = json_data[i:i + batch_size]  # slices data into batches of 25 or less

        processed_data = []  # stores batch data

        for inner_list in batch:
            for data in inner_list:
                try:
                    if data.get("valid_incident_rspns_time_indc") == "Y":
                        Date = data["first_activation_datetime"].split("T")[0]
                        Time = data["first_activation_datetime"].split("T")[1][:5]
                        Response_time = round(float(data["incident_response_seconds_qy"]) / 60, 2)

                        processed_data.append((Date, Time, Response_time))
                        total_entries += 1
                        print(total_entries)

                        if total_entries >= 100:
                            break  # Break the loop if 100 entries have been processed

            
                except KeyError as e:
                    print("Key Error:", e, "Skipping this data entry.")
        
            if total_entries >= 100:
                break  # Break the loop if 100 entries have been processed
        
        cur.executemany(
            '''
            INSERT INTO NYC_Fires (Date, Time, Response_time) 
            VALUES (?, ?, ?)
            ''',
            processed_data
        )

        conn.commit()  # Commit the transaction

        if total_entries >= 100:
                break  # Break the loop if 100 entries have been processed

    
#insert into Neighborhood table

#step 2 Calculate something from the data



def main():
    get_fire_data("Structural Fires")
    get_fire_data("NonStructural Fires")

    try:
        #set up the database
        cur, conn = set_up_database("fire_data.db")
        create_first_nyc_table(cur, conn)
        with open("NYC_data.json", "r") as json_file:
            NYC_data = json.load(json_file)

        insert_data_to_fires_table(cur, conn, NYC_data)

        # Call the calculate_avg_fires_per_2hr function
        

         # Write the average to a text file
        #with open("calculations.txt", 'w') as f:
         #   f.write("In 2021 The average number of fires per day in NYC for January 4th to January 5th was {:.2f}".format(avg_fires_per_day))

        conn.close()  # Close the database connection after insertion 
    except Exception as e:
        print("An error has occured:", e)


if __name__ == "__main__":
    main()

