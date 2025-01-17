import os
import json
import requests
import sqlite3
from datetime import datetime

def get_fire_dep_data():
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
    path = os.getcwd()  # Get the current working directory
    conn = sqlite3.connect(os.path.join(path, db_name))  # Use os.path.join to construct the file path
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
    conn.commit()

def insert_data_to_fires_table(cur, conn, json_data):
    batch_size = 25 
    total_entries = 0 

    for i in range(0, min(len(json_data), 100), batch_size):  # Process up to 100 entries
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
            INSERT INTO LA_Fires (Time, Response_time) 
            VALUES (?, ?)
            ''',
            data_buffer
        )
        conn.commit()

        # Check if total entries processed is equal to or exceeds 100
        if total_entries >= 100:
            break  # Break the loop if 100 entries have been processed

def calculate_avg_response_time_per_period(cur, conn):
    try:
        sql_query = '''
            SELECT 
                CASE 
                    WHEN Time BETWEEN '00:00' AND '01:59' THEN '00:00 - 01:59'
                    WHEN Time BETWEEN '02:00' AND '03:59' THEN '02:00 - 03:59'
                    WHEN Time BETWEEN '04:00' AND '05:59' THEN '04:00 - 05:59'
                    WHEN Time BETWEEN '06:00' AND '07:59' THEN '06:00 - 07:59'
                    WHEN Time BETWEEN '08:00' AND '09:59' THEN '08:00 - 09:59'
                    WHEN Time BETWEEN '10:00' AND '11:59' THEN '10:00 - 11:59'
                    WHEN Time BETWEEN '12:00' AND '13:59' THEN '12:00 - 13:59'
                    WHEN Time BETWEEN '14:00' AND '15:59' THEN '14:00 - 15:59'
                    WHEN Time BETWEEN '16:00' AND '17:59' THEN '16:00 - 17:59'
                    WHEN Time BETWEEN '18:00' AND '19:59' THEN '18:00 - 19:59'
                    WHEN Time BETWEEN '20:00' AND '21:59' THEN '20:00 - 21:59'
                    WHEN Time BETWEEN '22:00' AND '23:59' THEN '22:00 - 23:59'
                    ELSE 'Unknown'
                END AS period,
                AVG(Response_time) AS avg_response_time
            FROM LA_Fires
            GROUP BY period;
        '''
        cur.execute(sql_query)

        avg_response_times_per_period = cur.fetchall()

        periods = []
        avg_response_times = []

        with open("calculations.txt", 'a') as f:  # Append to the file
            f.write("\nAverage response time per 2-hour period in LA:\n")
            for row in avg_response_times_per_period:
                period, avg_response_time = row
                periods.append(period)
                avg_response_times.append(avg_response_time)
                f.write(f"{period}: {avg_response_time:.2f} minutes\n")

        print("Average response time per 2-hour period calculated and written to calculations.txt.")
        return periods, avg_response_times
    except Exception as e:
        print("An error occurred while calculating the average response time per period:", e)
        return None, None

def main():
    try:
        cur, conn = set_up_database("fire_data.db")  
        create_first_table(cur, conn)
        
        get_fire_dep_data()

        with open("LA_data.json", "r") as json_file:
            LA_data = json.load(json_file)

        insert_data_to_fires_table(cur, conn, LA_data) 
        calculate_avg_response_time_per_period(cur, conn)

        conn.close()
    except Exception as e:
        print("An error has occurred:", e)

if __name__ == "__main__":
    main()


