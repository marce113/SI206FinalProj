import os
import matplotlib.pyplot as plt


#Step 3 create vizualizations from the calculated data
#creates plot for avg fires in each NYC neighborhood
def create_neighborhood_viz(file_path):
    neighborhoods = []
    num_fires = []
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:  # Skip the header line
            neighborhood, num_fire = line.strip().split(': ')
            neighborhoods.append(neighborhood)
            num_fires.append(int(num_fire))
    
    plt.figure(figsize=(10, 6))
    plt.bar(neighborhoods, num_fires, color='deeppink')
    plt.title('Average Number of Fires per Neighborhood')
    plt.xlabel('Neighborhood')
    plt.ylabel('Number of Fires')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

#creates plot for average response time for each two hour period in the day
def plot_avg_response_time_per_period(file_path):
    periods = []
    avg_response_times = []
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:  # Skip the header line
            period, avg_response_time = line.strip().split(': ')
            periods.append(period)
            avg_response_times.append(float(avg_response_time.split()[0]))
    
    plt.figure(figsize=(10, 6))
    plt.plot(periods, avg_response_times, marker='o', color='green')
    plt.title('Average Response Time per 2-hour Period in NYC')
    plt.xlabel('Time Period')
    plt.ylabel('Average Response Time (minutes)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def main():
    try:
        create_neighborhood_viz("calculations.txt")

    except Exception as e:
        print("An error has occured:", e)


if __name__ == "__main__":
    main()

