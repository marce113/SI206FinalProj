import sqlite3
import json
import os
import requests
from datetime import datetime
import matplotlib.pyplot as plt


'''In this File we will get the data from one of our API's and save it into a Json and then a shared data base.
    we want to see how the response times for fire between the LAFD and NYCFD differ during similar times in the day.
    How does the time of day influence the response time for these fire departments?
    Additionally we want to see if certain neighborhoods in NYC have more fires on average.
    It's important to note that the data set is only 100 points and any correlations should be evaluated with caution. 

    we want to create three tables. One table will be for Structural fires in NYC, another will assign ID numbers to neighborhoods in New York and the third assigns these neighborhood ID's to Fire ID's in NYC. 
    Each table will have 100 entries
    We will not be including forest fires in our data set.
    The NYC_Fires table will have an ID for the fire, a response time in minutes (resonse time is how long it took for first responders to arrive
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

    # Drop existing tables if they exist
    cur.execute("DROP TABLE IF EXISTS NYC_Fires")
    cur.execute("DROP TABLE IF EXISTS neighborhood_ID")
    cur.execute("DROP TABLE IF EXISTS Fire_Neighborhood_Relationship")
    conn.commit()

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
        CREATE TABLE IF NOT EXISTS "neighborhood_ID" (
            "Neighborhood_ID" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "Neighborhood" TEXT
        )
        '''
    )
    conn.commit()

def create_fire_neighborhood_relationship_table(cur, conn):
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS Fire_Neighborhood_Relationship (
            Fire_ID INTEGER,
            Neighborhood_ID INTEGER,
            FOREIGN KEY(Fire_ID) REFERENCES NYC_Fires(Fire_ID),
            FOREIGN KEY(Neighborhood_ID) REFERENCES Neighborhoods(Neighborhood_ID)
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
                        #print(total_entries)

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

    
#insert into Neighborhood table and relationship table
def insert_data_to_neighborhood_table(cur, conn, json_data):
    batch_size = 25
    total_entries = 0  # initializes total entries processed
    fire_id = 1  # initialize fire ID

    neighborhoods_set = set()  # store unique neighborhoods

    for i in range(4):  # Loop 4 times
        print("Loading batch of 25 entries...")  # Print batch loading message

        # Process each entry in the JSON data
        for inner_list in json_data:
            for data in inner_list:
                try:
                    if data.get("valid_incident_rspns_time_indc") == "Y":
                        neighborhood = data["incident_borough"]

                        # Check if the neighborhood already exists in the neighborhood_ID table
                        cur.execute(
                            '''
                            SELECT Neighborhood_ID FROM neighborhood_ID WHERE Neighborhood = ?
                            ''',
                            (neighborhood,)
                        )
                        existing_neighborhood = cur.fetchone()

                        if existing_neighborhood is None:
                            # Insert neighborhoods into the neighborhood_ID table
                            cur.execute(
                                '''
                                INSERT INTO neighborhood_ID (Neighborhood) 
                                VALUES (?)
                                ''',
                                (neighborhood,)
                            )

                        # retrieve the Neighborhood_ID of the inserted neighborhood
                        cur.execute(
                            '''
                            SELECT Neighborhood_ID FROM neighborhood_ID WHERE Neighborhood = ?                                
                            ''',
                            (neighborhood,)
                        )
                        neighborhood_id = cur.fetchone()[0]

                        # Insert the relationship into the Fire_Neighborhood_Relationship table
                        cur.execute(
                            '''
                            INSERT INTO Fire_Neighborhood_Relationship (Fire_ID, Neighborhood_ID) 
                            VALUES (?, ?)
                            ''',
                            (fire_id, neighborhood_id)
                        )

                    total_entries += 1
                    fire_id += 1

                    if total_entries  >= 100:
                        break  # Break the loop if 25 entries have been processed

                except KeyError as e:
                    print("Key Error:", e, "Skipping this data entry.")
                except Exception as ex:
                    print("Error occurred:", ex)  # Log any other exceptions

        conn.commit()  # Commit the transaction

        if total_entries >= 100:
            break  # Break the loop if 100 entries have been processed



#step 2 Calculate something from the data
def calculate_avg_fires_per_neighborhood(cur, conn):
    try:
        cur.execute('''
            SELECT neighborhood_ID.Neighborhood, COUNT(NYC_Fires.Fire_id) AS num_fires
            FROM neighborhood_ID
            LEFT JOIN Fire_Neighborhood_Relationship ON neighborhood_ID.Neighborhood_ID = Fire_Neighborhood_Relationship.Neighborhood_ID
            LEFT JOIN NYC_Fires ON Fire_Neighborhood_Relationship.Fire_ID = NYC_Fires.Fire_id
            GROUP BY neighborhood_ID.Neighborhood
        ''')

        avg_fires_per_neighborhood = cur.fetchall()

        with open("calculations.txt", 'w') as f:
            f.write("Average number of fires per neighborhood:\n")
            for neighborhood, num_fires in avg_fires_per_neighborhood:
                f.write(f"{neighborhood}: {num_fires}\n")

        print("Average number of fires per neighborhood calculated and written to calculations.txt.")
    except Exception as e:
        print("An error occurred while calculating the average number of fires per neighborhood:", e)


#function that calculates average response time for 2 hour time periods for fires in NYC 
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
            FROM NYC_Fires
            GROUP BY period;
        '''
        cur.execute(sql_query)

        avg_response_times_per_period = cur.fetchall()

        #create lists to make creating the vizualization more straight forward
        periods = []
        avg_response_times = []

        with open("calculations.txt", 'a') as f:  # Append to the file
            f.write("\nAverage response time per 2-hour period in NYC:\n")
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
    


#Step 3 create vizualizations from the calculated data
#creates plot for avg fires in each NYC neighborhood
def create_neighborhood_viz(file_path):
    neighborhoods = []
    num_fires = []
    section_found = False
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines:
            if not section_found:
                if "Average number of fires per neighborhood" in line:
                    section_found = True
                continue
            
            if "Average response time per 2-hour period" in line:
                break
            
            if ':' not in line:
                continue
                
            neighborhood, num_fire = line.strip().split(': ')
            neighborhoods.append(neighborhood)
            num_fires.append(int(num_fire))
    
    plt.figure(figsize=(10, 6))
    plt.bar(neighborhoods, num_fires, color='deeppink')
    plt.title('Average Number of Fires per Neighborhood')
    plt.xlabel('Neighborhood')
    plt.ylabel('Number of Fires')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()


#creates plot for average response time for each two hour period in the day
def create_NYC_response_time_viz(periods, avg_response_times):
    plt.figure(figsize=(10, 6))
    plt.plot(periods, avg_response_times, marker='o', color='blue')
    plt.title('Average Response Time per 2-hour Period in NYC')
    plt.xlabel('Time Period (military time)')
    plt.ylabel('Average Response Time (minutes)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()



def main():
    get_fire_data("Structural Fires")

    try:
        #set up the database
        cur, conn = set_up_database("fire_data.db")
        create_first_nyc_table(cur, conn)
        create_neighborhood_table(cur, conn)
        create_fire_neighborhood_relationship_table(cur, conn)
        with open("NYC_data.json", "r") as json_file:
            NYC_data = json.load(json_file)

        insert_data_to_fires_table(cur, conn, NYC_data)
        insert_data_to_neighborhood_table(cur, conn, NYC_data)

        calculate_avg_fires_per_neighborhood(cur, conn)
        periods, avg_response_times = calculate_avg_response_time_per_period(cur, conn)

        if periods and avg_response_times:
            create_NYC_response_time_viz(periods, avg_response_times)

        conn.close()  # Close the database connection after insertion

        create_neighborhood_viz("calculations.txt")
        create_NYC_response_time_viz("calculations.txt")

    except Exception as e:
        print("An error has occured:", e)


if __name__ == "__main__":
    main()

