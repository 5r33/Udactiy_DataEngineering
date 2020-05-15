
# Project Title
### Data Engineering Capstone Project

#### Project Summary
This project has been created to capture immigrant data arriving at the US entries. The datwarehouse has ETL processing
with Spark and Final data will be stored in parquet form on disk.
This would enable the end user to read data as required by using processing power of Spark as the final data is stored in a Fact and Dimension creating a Star Schema.



# Files in the repository

* **[Capstone Project Template.ipynb](./Capstone Project Template.ipynb)**: Jupyter notebook  to outlining step by step of all the activities completed to achieve the end result,

* **[RawDataProcess.py](./RawDataProcess.py)**: Python script to extract the needed information from the provided local directories
* * **[DataCleanse.py](./DataCleanse.py)**: Python script to cleanse the data from the raw data
* * **[DataTransform.py](./DataTransform.py)**: Python script to transform the cleansed data as required
* * **[DataLoader.py](./DataLoader.py)**: Load the final datawarehouse
* * **[DataValidation.py](./DataValidation.py)**: Python script to validate the data warehouse
* * * **[DataDictionary.md](./DataDictionary.md)**: Data dictionary

### Step 1: Scope the Project and Gather Data

#### Scope 
As part of this project, we would look at the immigration data in USA and how Temperature,Race affect the decision for immigrants. Data set used are i94 immigration data coupled with temperature for US cities and demographic data that is available to general public. The output of this project would have Fact and Dimension table that will enable to get the required data in an efficient and confirmed way.
Datawarehouse will reside in parquet files that have been created using python ETL scripts and Spark processing power.


#### Describe and Gather Data 
***U.S. City Demographic Data (demog):*** Extracted from OpenSoft and includes data by city, state, age, population, veteran status and race.

***I94 Immigration Data (sas_data):*** Extracted from the US National Tourism and Trade Office and includes details on incoming immigrants and their ports of entry.

***Airport Code Table (airport):*** Extracted from datahub.io and includes airport codes and corresponding cities.

***Countries (countries):*** Extracted from I94_SAS_Labels_Descriptions.SAS with country to code mapping

***Visas (visa):*** Extracted from I94_SAS_Labels_Descriptions.SAS with visa code to type

***Inmigrant Entry Mode (mode):*** Extracted from I94_SAS_Labels_Descriptions.SAS with id maped to Entry mode 

***Port(Port):*** Extracted from I94_SAS_Labels_Descriptions.SAS with Port mapped to country
### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
*Clean countries dataset to get the port code and state code for only USA

*Clean demograpihc data for converting string to double for double values and correct column name

*Clean ports dataset to get the port code and state code for only USA

*Clean airports dataset filtering only US airports and discarding data that is not an airport("large_airport", "medium_airport", "small_airport"). Extract iso regions and cast fields as required.

*Clean the immigration dataset.Standardize date. Mark data that is marked as type invalid in coutry code or ports code with a new column to identif why data set is missing

All the data cleansing function has been done with help of a class called DataCleanse.py

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

##### we have chosen the Star Schema with fact and dimension to generate our warehouse. Details related to the dimension and fact tables are as below

##### Star Schema
Dimension Tables:

* dim_demographics
City,State,State_code,Median_age,Male_population,Female_population,Total_population,number_of_veterans,foreign_born,avg_hshld_size, Race,count ,male_ratio,female_ratio

* dim_airports:
ident, type, name, elevation_ft, continent, iso_country, iso_region, municipality, gps_code, iata_code, local_code, coordinates,state_code

* dim_countries:
country_code,country_name,country_status

* dim_get_visa:
visa_id,visa_type

* dim_get_mode:
mode_id,mode_name

* dim_us_states:
State,State_Code

Fact Table:

* immigration_fact_table
cic_id,year,month,org_cntry_code,org_country_name,port_id,visa_id,birth_year,age,state_code,mode_id,arrrival_date,departure_date,gender,airline,flight_no,visatype,occupation,counter,

#### 3.2 Mapping Out Data Pipelines
* There are two steps:

** Tranform data:

    1. Transform demographics dataset to inlcude ratio for male and female population
    2. Transform inmigration dataset on order to get arrival date in different columns (year, month, day) for partitioning the dataset.
    
** Generate Model (Star Schema):

    1. Create all dimensions in parquet.
    2. Create fact table in parquet particioned by year, month, day of th arrival date.
    3. Insert in fact table only items with dimension keys right. For integrity and consistency.

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model using the scripts created

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.
 
 
#### Rationale
* For the project the scripting language of choice is python as there are are number of libraries available to perform various functions. the data processing is done using Spark as spark can process a huge volume of data and has huge number of library available to support.
* The final datawarehouse has been persisted  in parquet files as they can scale upto petabytes of data without any major issues

#### Dataupdation
* propose that the data be updated everyday as we have partioned by month and year and should be easily scalable. and can be traversed without issues. we can use APache airflow to perform this activity.

#### Different solution
* if the data was increased by 100x Spark should be still ale to handle it.
* Use Apache airflow  as it has effective SLA, email notificaiton and monitoring capability to help for daily SLA based requirement
* Use Hive and spark sql template views for more than 100 users