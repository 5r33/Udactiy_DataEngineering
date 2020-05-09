"""
    Create the Project1B to
        - Use provided dataset to create a denormalized dataset
        - model the data tables keeping in mind the queries you need to run
        - load the data into tables created in Apache Cassandra and run your queries
"""
# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    print("Number of Files: ")
    print(len(file_path_list))
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print("number of combined rows : ")
    print(sum(1 for line in f) )
    




# To establish connection and begin executing queries, need a session
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)
    
"""
    Create the keyspace with name Udacity
    This will be the default keyspace to be used for this project
"""
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
    
#  Set KEYSPACE to the keyspace udacity created above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


## Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query = "CREATE TABLE IF NOT EXISTS song_event "
query = query + "(artist text,song text,length float,sessionid int,iteminsession int, PRIMARY KEY(sessionid,iteminsession))"
try:
    session.execute(query)
except Exception as e:
    print(e)

    
# Using the file provided ,create and insert data required to execute the query. 
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## INSERT statement for the Query1
        
        query = "INSERT INTO song_event (artist,song,length,sessionid,iteminsession)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[0],line[9],float(line[5]),int(line[8]),int(line[3])))
        
## SELECT QUERY 1      
rows=[]
query = "select artist,song,length from song_event where sessionId = 338 and itemInSession = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print("QUERY1- Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4")
    print("o/p")
    print (row.artist,row.song,row.length)
    
## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "CREATE TABLE IF NOT EXISTS song_user_event "
query = query + "(artist text,song text,firstname text,lastname text,userid int,sessionid int ,iteminsession int,PRIMARY KEY((userid,sessionid),iteminsession))"
try:
    session.execute(query)
    #session.execute("Drop table song_user_event ")
except Exception as e:
    print(e)

#select the required data - Query2 
rows=[]
query = "select artist,song,firstname,lastname from song_user_event where userid = 10 and  sessionid = 182 order by iteminsession"
#query = "select * from song_user_event where userid = 10 and  sessionid = 182 order by iteminsession"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

print("QUERY2- Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182")
print("o/p")
for row in rows:

    print (row.artist,row.song,row.firstname,row.lastname)
    
# Using the file provided ,create and insert data required to execute the query. 
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## INSERT statement for the Query2
        
        query = "INSERT INTO song_user_event (artist ,song ,firstname ,lastname ,userid ,sessionid  ,iteminsession )"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ##  Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[0],line[9],line[1],line[4],int(line[10]),int(line[8]),int(line[3])))

        
##Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = "CREATE TABLE IF NOT EXISTS song_ALLHANDS_event "
query = query + "(firstname text,lastname text,song text,userid int,PRIMARY KEY(song,userid))"
try:
    session.execute(query)
    #session.execute("Drop table song_ALLHANDS_event ")
except Exception as e:
    print(e)


# Using the file provided ,create and insert data required to execute the query. 
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## INSERT statement for the Query3
        
        query = "INSERT INTO song_ALLHANDS_event (firstname ,lastname ,song ,userid)"
        query = query + " VALUES (%s, %s, %s, %s)"
        ##  Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[1],line[4],line[9],int(line[10])))
    

## SELECT QUERY 3
#select the required data - Query3
rows=[]
#query = "select song,firstname,lastname from song_user_event where userid = 10 and  sessionid = 182 order by iteminsession"
query = "select * from song_ALLHANDS_event where  song ='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

    
print("Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'")
print("o/p")

for row in rows:
    print (row.firstname,row.lastname) 
    
## Once complete DROP the tables.

query = "drop table song_event"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
query = "drop table song_user_event"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
query = "drop table song_ALLHANDS_event"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
    
## Shutdown the session
session.shutdown()
cluster.shutdown()