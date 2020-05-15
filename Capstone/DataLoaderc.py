from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType

from pyspark.sql.functions import *


class DataLoader:
    """
    Loads the datawarehouse (star schema) from datasets. Creating the facts table and dimension tables.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def DataLoader_demographics(self, demographics):
        """
        Create demographics dimension table in parquet.
        :param demographics: demographics dataset.
        """
        demographics.write.mode('overwrite').parquet(self.paths["demographics"])

    def DataLoader_airports(self, airports):
        """
        Create airports dimension table in parquet.
        :param airports: airports dataset
        """
        airports.write.mode('overwrite').parquet(self.paths["airports"])

    def DataLoader_ports(self, ports):
        """
        Create ports dimension table in parquet.
        :param ports: ports dataset
        """
        ports.write.mode('overwrite').parquet(self.paths["ports"])

    def DataLoader_countries(self, countries):
        """
        Create countries dimension table in parquet
        :param countries: countries dataset
        """
        countries.write.mode('overwrite').parquet(self.paths["countries"])

    def DataLoader_visa(self, visa):
        """
        Create visa dimension table in parquet
        :param visa: visa dataset
        """
        visa.write.mode('overwrite').parquet(self.paths["visa"])

    def DataLoader_mode(self, mode):
        """
        Create modes dimension table in parquet
        :param mode: modes dataset
        """
        mode.write.mode('overwrite').parquet(self.paths["mode"])

    def DataLoader_facts(self, facts):
        """
        Create facts table from inmigration in parquet particioned by arrival_year, arrival_month and arrival_day
        :param facts: inmigration dataset
        """
        facts.write.partitionBy("year", "month").mode('overwrite').parquet(
            self.paths["facts"])
        
    def FinalWriter(self, facts, dim_demographics, dim_airports, dim_ports, dim_countries, dim_visa, dim_mode):
        """
        Create the Star Schema for the Data Warwhouse
        :param facts: facts table, inmigration dataset
        :param dim_demographics: dimension demographics
        :param dim_airports: dimension airports
        :param dim_ports dimension ports
        :param dim_countries: dimension countries
        :param dim_visa: dimension visa
        :param dim_mode: dimension mode
        """
        facts.createOrReplaceTempView("immigration")
        dim_airports.createOrReplaceTempView("airport")
        dim_demographics.createOrReplaceTempView("demo")
        dim_ports.createOrReplaceTempView("ports")
        dim_countries.createOrReplaceTempView("countries")
        dim_mode.createOrReplaceTempView("mode")
        dim_visa.createOrReplaceTempView("visa")
        finalfact=self.spark.sql("""
        select f.* from  immigration f join demo dd
        on f.state_code = dd.state_code
        join visa on f.visa_id = visa.visa_id 
        join mode on f.mode_id = mode.mode_id
        """)
        self.DataLoader_demographics(dim_demographics)
        self.DataLoader_airports(dim_airports)
        self.DataLoader_ports(dim_ports)
        self.DataLoader_countries(dim_countries)
        self.DataLoader_visa(dim_visa)
        self.DataLoader_mode(dim_mode)

        self.DataLoader_facts(finalfact)
