from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType

class DataCleanse:
    """
     ***Clean the original dataset with the below minimum conditions
    *Clean countries dataset to add a column as valid/invalid
    *Clean ports dataset to get the port code and state code for only USA
    *Clean airports dataset filtering only US airports and discarding data that is not an airport("large_airport",   "medium_airport", "small_airport"). Extract iso regions and cast fields as required.
    *Clean the immigration dataset.Standardize date. Mark data that is marked as type invalid in coutry code or ports code with a new column to identify why data set is missing
    """
    def __init__(self, spark):
        self.spark = spark

    def get_demo_cleansed(self,dfdemog):
        """
        Clean demograpihc data for converting string to double for double values and correct column name"). 
        Extract iso regions and cast fields as required.
        :input airports: airports dataframe
        :output: airports dataframe cleaned
        """
        dfdemog.createOrReplaceTempView("City")
        cl_dfdemog=self.spark.sql("""
        select City,State,`State Code` as State_code,
        cast(`Median Age` as double) Median_age,cast(`Male Population` as double)Male_population,
        cast(`Female Population` as double)Female_population,cast(`Total Population` as double)Total_population,
        `Number of Veterans` as number_of_veterans,`Foreign-born` as foreign_born,
        `Average Household Size` avg_hshld_size, Race,count from City
                """)
        return cl_dfdemog
   
    def get_airports_cleansed(self,dfairport):
        """
        Clean airports dataset filtering only US airports and discarding data that is not an airport("large_airport",   
        "medium_airport", "small_airport"). 
        Extract iso regions and cast fields as required.
        :input airports: airports dataframe
        :output: airports dataframe cleaned
        """
        dfairport.createOrReplaceTempView("Airport")
        cl_dfairport=self.spark.sql("""
              select a.*,substring(iso_region,-2)state_code  from Airport a
              where iso_country='US'
              and type in ("large_airport", "medium_airport", "small_airport")
                """)
        return cl_dfairport
    
    def get_ports_cleansed(self,dfports):
        """
        Clean ports dataset to get the port code and state code for only USA
        :input orts: airports dataframe
        :output: ports dataframe cleaned
        """
        dfports.createOrReplaceTempView("Port")
        cl_dfport=self.spark.sql("""
              select code as port_code,airport_name,substring_index(airport_name, ',', -1) port_state_code from Port p
                """)
        return cl_dfport

    def get_countries_cleansed(self,dfcountries):
        """
        Clean countries dataset to add a column as valid/invalid
        :input orts: airports dataframe
        :output: ports dataframe cleaned
        """
        dfcountries.createOrReplaceTempView("Countries")
        cl_dfcountries=self.spark.sql("""
              select code as country_code,country_name,case when country_name like '%INVALID%' or country_name like '%Collapsed%' or country_name like '%No Country Code%' then 'INVALID'
              else 'VALID' end country_status from Countries c             
                """)
             
        return cl_dfcountries
    
    def get_mode_cleansed(self,dfmode):
        """
        Format ID column to integer
        """
        dfmode.createOrReplaceTempView("Mode")
        cl_dfmode=self.spark.sql("""
              select cast(cod_id as integer)mode_id,mode_name from Mode
                """)
        return cl_dfmode
    
    def get_Immigration_cleansed(self,dfsas_data):
        """
        Clean the immigration dataset.Standardize date. 
        Mark data that is marked as type invalid in coutry code or ports code with a new column to identify why data set is missing
        """
        dfsas_data.createOrReplaceTempView("Immigration")
        cl_dfsas_data=self.spark.sql("""
              select cast(cicid as integer)cic_id,
               cast(i94yr as integer)year,
              cast(i94mon as integer)month,
              cast(i94cit as integer)org_cntry_code,
              cast(i94res as integer)org_country_name,
              cast(i94port as integer)port_id,
              cast(i94visa as integer)visa_id,
              cast(biryear as integer)birth_year,
              cast(i94bir as integer)age,
              i94addr as state_code,
              cast(i94mode as integer)mode_id,
              date_add(to_date("01/01/1960", "MM/dd/yyyy"),arrdate) arrrival_date,
              date_add(to_date("01/01/1960", "MM/dd/yyyy"),depdate) departure_date,
              gender,airline,fltno as flight_no,visatype,OCCUP as occupation,
              count as counter
              from Immigration
                """)
        return cl_dfsas_data