import sqlite3
import csv
import json
import os
import unittest
import requests

#authors: Marcela Passos and Carolina Janicke
#emails: marcelp@umich.edu,

'''In this File we will get the data from our API's and save them into a shared data base.
    we want to see how the response times for fire between the LAFD and NYCFD differ during similar times in the day.
    How does the time of day influence the response time for these fire departments? 

    we want to create two tables. One table will be for fires in NYC and the other will be for fires in LA. 
    Each table will have 100 entries
    We will not be including forest fires in our data set.
    Each table will have an ID for the fire, a response time in minutes (resonse time is how long it took for first responders to arrive
    to the scene from when the call was made), and the time that the call was made. 
'''


#create Database
def set_up_database(db_name):
    pass


