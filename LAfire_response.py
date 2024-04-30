import os
import json
import requests
import sqlite3
from datetime import datetime
def get_fire_dep_data(classification_group):
    base_url = "https://data.lacity.org/resource/n44u-wxe4.json"
    response = requests.get(base_url, params={"$where": "on_scene_time_gmt IS NOT NULL"})

    try:
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        if data:
            try:
                with open("LA_data.json", "r") as json_file:
                    existing_data = json.load(json_file)
            except (FileNotFoundError, json.decoder.JSONDecodeError):
                existing_data = []
            # Append only the relevant entries to existing data
            existing_data.extend(data)
            with open("LA_data.json", "w") as json_file:
                json.dump(existing_data, json_file, indent=4)
                print("Data appended to LA_data.json file.")
        else:
            print("No data received from the API.")
    except requests.exceptions.HTTPError as err:
        print("HTTP error occurred:", err)
    except json.decoder.JSONDecodeError as err:
        print("Error decoding JSON response:", err)


def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn


def create_first_table(cur, conn):
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS "Fires" (
            "Fire_id" INTEGER PRIMARY KEY, 
            "Time" TEXT, 
            "Response_time" FLOAT
        )
        '''
    )

  

def insert_data_to_fires_table(cur, conn, json_data):
    batch_size = 25 
    total_entries = 0 

    for i in range(-1, min(len(json_data), 101), batch_size):  # Process up to 100 entries
        batch = json_data[i:i+batch_size]  # Slice the data into batches of 25 or less

        data_buffer = [] 

        for data in batch:
            try:
                if "on_scene_time_gmt" in data and "incident_creation_time_gmt" in data:
            
                    incident_creation_time = datetime.strptime(data["incident_creation_time_gmt"], "%H:%M:%S.%f")
                
                    on_scene_time = datetime.strptime(data["on_scene_time_gmt"], "%H:%M:%S.%f")


                    Time = incident_creation_time.strftime("%H:%M")


                    Response_time = (on_scene_time - incident_creation_time).total_seconds() / 60
                    Response_time = round(Response_time, 2) 

                
                    data_buffer.append((Time, Response_time))
                    total_entries += 1  

            except KeyError as e:
                print("KeyError:", e, "Skipping this data entry.")
            except ValueError as e:
                print("ValueError:", e, "Skipping this data entry. Likely incorrect time format.")

        # Insert data from the buffer into the table
        cur.executemany(
            '''
            INSERT INTO Fires (Time, Response_time) 
            VALUES (?, ?)
            ''',
            data_buffer
        )
        conn.commit()

        # Check if total entries processed is equal to or exceeds 100
        if total_entries >= 100:
            break  # Break the loop if 100 entries have been processed






def main():
    try:
        cur, conn = set_up_database("la.db")  # Corrected: "la.db" should be a string
        create_first_table(cur, conn)
        with open("LA_data.json", "r") as json_file:
            LA_data = json.load(json_file)

        insert_data_to_fires_table(cur, conn, LA_data)  # Corrected: Call insert_data_to_fires_table

        conn.close()
    except Exception as e:
        print("An error has occurred:", e)

if __name__ == "__main__":
    main()





