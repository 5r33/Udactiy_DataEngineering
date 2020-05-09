
# Summary of project

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

In order to enable Sparkify to analyze their data, selected database is Apache Cassandra which can create queries on song play data to answer the questions

# How to run the python scripts

To create the database tables and run the ETL pipeline, you must run the following two files in the order that they are listed below

To run the entire program:
```bash
python3 Project_1B.py
```
Use the Notebook to run through the steps sequentially:
```bash
Project_1B.ipynb
```

# Data Files in the repository


For this project, you'll be working with one dataset: `event_data`. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

# ETL code in the repository



* **[Project_1B.py](./Project_1B.py)**: Python script to run the entire requirement 

### Data
Number of file: 30
Number of records : 6821



## **Project Steps:**

Below are steps you can follow to complete each component of this project.

**Modelling your NoSQL Database or Apache Cassandra Database:**

1.  Design tables to answer the queries outlined in the project template
2.  Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
3.  Develop your CREATE statement for each of the tables to address each question
4.  Load the data with INSERT statement for each of the tables
5.  Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. We recommend you also include DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
6.  Test by running the proper select statements with the correct WHERE clause

**Build ETL Pipeline:**

1.  Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
2.  Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
3.  Test by running three SELECT statements after running the queries on your database
4.  Finally, drop the tables and shutdown the cluster

**Files:**

**Project_1B_Project_Template.ipynb:**  This was template file provided to fill in the details and write the python script

**Project_1B.ipynb:**  This is the final file provided in which all the queries have been written with importing the files, generating a new csv file and loading all csv files into one. All verifying the results whether all tables had been loaded accordingly as per requirement

**Event_datafile_new.csv:**  This is the final combination of all the files which are in the folder event_data

**Event_Data Folder:**  Each event file is present separately, so all the files would be combined into one into event_datafile_new.csv

# End result of project
User is able to get the specified output as required.