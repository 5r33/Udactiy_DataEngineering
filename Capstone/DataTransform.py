from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType

class DataTransform:
    """
    Transform demographics dataset to inlcude ratio for male and female population
    Transform inmigration dataset on order to get arrival date in different columns (year, month, day) for partitioning the dataset.
    """
    def __init__(self, spark):
        self.spark = spark

    def get_demo_transform(self,cl_dfdemog):
        """
        Add logic for male and female ratio
        """
        cl_dfdemog.createOrReplaceTempView("City")
        tr_dfdemog=self.spark.sql("""
        select a.*,
        (Male_population/Total_population)male_ratio,
        (Female_population/Total_population)female_ratio from City a
                """)
        return tr_dfdemog
   
