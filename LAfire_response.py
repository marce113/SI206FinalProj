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
        CREATE TABLE IF NOT EXISTS "LA_Fires" (
            "Fire_id" INTEGER PRIMARY KEY, 
            "Time" TEXT, 
            "Response_time" FLOAT
        )
        '''
    )

def insert_data_to_fires_table(cur, conn, json_data):
    total_entries = 0

    # Initialize a dictionary to keep track of the number of batches per 2-hour period
    batches_per_period = {}

    for data in json_data:
        try:
            if "on_scene_time_gmt" in data and "incident_creation_time_gmt" in data:
                # Extract incident creation and on-scene times
                incident_creation_time = data["incident_creation_time_gmt"]
                on_scene_time = data["on_scene_time_gmt"]

                # Extract the hour from incident creation time
                hour = incident_creation_time.split(":")[0]

                # Check if this hour already has 4 batches
                if hour not in batches_per_period:
                    batches_per_period[hour] = 1
                elif batches_per_period[hour] >= 4:
                    continue  # Skip this entry if we've reached the batch limit
                else:
                    batches_per_period[hour] += 1

                # Calculate the response time in minutes
                response_time = (datetime.strptime(on_scene_time, "%H:%M:%S.%f") - datetime.strptime(incident_creation_time, "%H:%M:%S.%f")).total_seconds() / 60
                response_time = round(response_time, 2)

                # Add the data to the database
                cur.execute(
                    '''
                    INSERT INTO LA_Fires (Time, Response_time) 
                    VALUES (?, ?)
                    ''',
                    (incident_creation_time[:2] + ":00", response_time)
                )
                conn.commit()
                total_entries += 1

                # Break the loop if we've processed 100 entries
                if total_entries >= 100:
                    break

        except KeyError:
            print("Skipping incomplete data entry.")

    print("Data insertion completed.")





def calculate_avg_response_time_per_period(cur, conn):
    try:
        cur.execute('''
            SELECT CAST(SUBSTR(Time, 1, 2) AS INTEGER) AS hour,
                   ROUND(AVG(Response_time), 2) AS avg_response_time
            FROM LA_Fires
            GROUP BY hour
        ''')

        avg_response_times_per_period = cur.fetchall()

        with open("calculations.txt", 'a') as f:  # Append to the file
            f.write("\nAverage response time per 2-hour period in LA:\n")
            for hour, avg_response_time in avg_response_times_per_period:
                period_start = "{:02d}:00".format(hour)
                period_end = "{:02d}:59".format((hour + 1) % 24)  # Next hour minus 1 minute
                period = f"{period_start} - {period_end}"
                f.write(f"{period}: {avg_response_time} minutes\n")

        print("Average response time per 2-hour period calculated and written to calculations.txt.")
    except Exception as e:
        print("An error occurred while calculating the average response time per period:", e)


def main():
    try:
        cur, conn = set_up_database("fire_data.db")  # Corrected: "la.db" should be a string
        create_first_table(cur, conn)
        with open("LA_data.json", "r") as json_file:
            LA_data = json.load(json_file)

        insert_data_to_fires_table(cur, conn, LA_data) 
        calculate_avg_response_time_per_period(cur, conn) # Corrected: Call insert_data_to_fires_table

        conn.close()
    except Exception as e:
        print("An error has occurred:", e)

if __name__ == "__main__":
    main()





