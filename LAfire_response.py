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
            existing_data.append(data)
            with open("LA_data.json", "w") as json_file:
                json.dump(existing_data, json_file, indent=4)
                print("Data appended to la.db file.")
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
    fire_id = 1
    batch_size = 25

    for i in range(0, len(json_data), batch_size):
        batch = json_data[i:i+batch_size]  # Slice the data into batches of 25

        data_buffer = []  # Initialize a buffer to store batch data

        for data in batch:
            try:
                if "on_scene_time_gmt" in data and "incident_creation_time_gmt" in data:
                    incident_creation_time = datetime.strptime(data["incident_creation_time_gmt"], "%H:%M:%S.%f")
                    Time = int(incident_creation_time.hour) * 60 + int(incident_creation_time.minute)
                    on_scene_time = datetime.strptime(data["on_scene_time_gmt"], "%H:%M:%S.%f")
                    Response_time = float((on_scene_time - incident_creation_time).total_seconds() / 60)

                    # Append data to buffer
                    data_buffer.append((Time, Response_time))

            except KeyError as e:
                print("KeyError:", e, "Skipping this data entry.")
            except IndexError as e:
                print("IndexError:", e, "Skipping this data entry.")

        # Insert data from the buffer into the table
        cur.executemany(
            '''
            INSERT INTO Fires (Time, Response_time) 
            VALUES (?, ?)
            ''',
            data_buffer
        )
        conn.commit()
 

def main():
    try:
        cur, conn = set_up_database("la.db")  # Connect to the database "la.db"
        create_first_table(cur, conn)  # Create the "Fires" table if it doesn't exist
        get_fire_dep_data("Structural Fires")  # Fetch data for LA

        with open("LA_data.json", "r") as json_file:
            LA_data = json.load(json_file)  # Load JSON data from the file

        insert_data_to_fires_table(cur, conn, LA_data)  # Insert JSON data into the database
        conn.close()  # Close the database connection after insertion
    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    main()



