import sqlite3
import csv
import json
import os
import unittest
import requests
import re

#authors: Marcela Passos and Carolina Janicke
#emails: marcelp@umich.edu,

'''In this File we will get the data from our API's and save them into a shared data base.
    we want to see how the response times for fire between the LAFD and NYCFD differ during similar times in the day.
    How does the time of day influence the response time for these fire departments? 

    we want to create three tables. One table will be for Structural fires in NYC, another will be for non structural fires in NYC and the other will be for fires in LA. 
    Each table will have 100 entries
    We will not be including forest fires in our data set.
    Each table will have an ID for the fire, a response time in minutes (resonse time is how long it took for first responders to arrive
    to the scene from when the call was made), and the time that the call was made. 
'''


#pulls from APi and creats a Json with all the data
def get_fire_dep_data():
    '''
    creates API request
    creates json with all the data from request 
    '''

    #filters to just find data about structural and non structural fires 
    structural_Fires_url = "https://data.cityofnewyork.us/resource/8m42-w767.json?incident_classification_group=Structural Fires"
    NonStructural_Fires_url = "https://data.cityofnewyork.us/resource/8m42-w767.json?incident_classification_group=NonStructural Fires"
    

#create Database
#pulls data from APi and puts it into database
def set_up_database(db_name):
    #filter clasification by contains fire
    #calculate response time based on first on scene and incident time 
    #save processed table 
    pass

#step 2 





