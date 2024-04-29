import os
import matplotlib.pyplot as plt


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
def create_NYC_response_time_viz(file_path):




def main():
    try:
        #create_neighborhood_viz("calculations.txt")
        create_NYC_response_time_viz("calculations.txt")


    except Exception as e:
        print("An error has occured:", e)


if __name__ == "__main__":
    main()

