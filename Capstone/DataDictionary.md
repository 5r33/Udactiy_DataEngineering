
# Capstone Project

# **Data Dictionary Dimension Tables**
## Airports Data
 |-- ident: string (nullable = true) - identity number
 |-- type: string (nullable = true) - type of airport
 |-- name: string (nullable = true) - Name of airport
 |-- elevation_ft: string (nullable = true) - elevation
 |-- continent: string (nullable = true) - continent
 |-- iso_country: string (nullable = true) - country
 |-- iso_region: string (nullable = true) - region
 |-- municipality: string (nullable = true) - muncipality
 |-- gps_code: string (nullable = true) - gps code
 |-- iata_code: string (nullable = true) - IATA code
 |-- local_code: string (nullable = true) - local code
 |-- coordinates: string (nullable = true)  - coordinates
 |-- state_code: string (nullable = true)  - State code
 
## U.S. Demographic by State
|-- City: string (nullable = true) -city name
 |-- State: string (nullable = true) - State Name
 |-- State_code: string (nullable = true) - State code for lookup
 |-- Median_age: double (nullable = true) -median age of the city
 |-- Male_population: double (nullable = true) -number of males
 |-- Female_population: double (nullable = true) - number of females 
 |-- Total_population: double (nullable = true) - totalpopulation of city
 |-- number_of_veterans: string (nullable = true) - number of veterans
 |-- foreign_born: string (nullable = true) - foregin born
 |-- avg_hshld_size: string (nullable = true) - average size of household
 |-- Race: string (nullable = true) - Race categorization
 |-- count: string (nullable = true)  - counts for each 
 
## Ports
|-- code: string (nullable = true) - port code
 |-- airport_name: string (nullable = true) airport_name

## Countries
|-- code: string (nullable = true) country code
 |-- country_name: string (nullable = true) country name
 

## Visas
 |-- visa_id: string (nullable = true) - visa code
 |-- visa_type: string (nullable = true) - visa type
 
## Mode to access
 |-- mode_id: integer (nullable = true) - mode code
 |-- mode_name: string (nullable = true) -mode name

# **Data Dictionary Dimension Tables**
# Imigration Registry (Fact)
  |-- cic_id: integer (nullable = true) - id number
 |-- org_cntry_code: integer (nullable = true) - immigrant country code
 |-- org_country_name: integer (nullable = true) - immigrant country name
 |-- port_id: integer (nullable = true) - port id
 |-- visa_id: integer (nullable = true) - visa id
 |-- birth_year: integer (nullable = true) - year of birth
 |-- age: integer (nullable = true) - Age
 |-- state_code: string (nullable = true) - state_code
 |-- mode_id: integer (nullable = true) - mode id
 |-- arrrival_date: date (nullable = true) arrival date in full
 |-- departure_date: date (nullable = true) departure date in full
 |-- gender: string (nullable = true) gender
 |-- airline: string (nullable = true) airline name
 |-- flight_no: string (nullable = true) flight number
 |-- visatype: string (nullable = true) visatype
 |-- occupation: string (nullable = true) -occupation
 |-- counter: double (nullable = true) -counter for stats
 |-- year: integer (nullable = true) -year
 |-- month: integer (nullable = true) -monht