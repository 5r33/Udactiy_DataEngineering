from pyspark.sql.functions import *


class DataValidation:
    """
    Validate and checks the model and data.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def getdata_demographics(self):
        """
        Get demographics dimension
        :return: demographics dimension
        """
        return self.spark.read.parquet(self.paths["demographics"])

    def getdata_airports(self):
        """
        Get airports dimension
        :return: airports dimension
        """
        return self.spark.read.parquet(self.paths["airports"])

    def getdata_ports(self):
        """
        Get ports dimension
        :return: ports dimension
        """
        return self.spark.read.parquet(self.paths["ports"])

    def getdata_countries(self):
        """
        Get countries dimension
        :return: countries dimension
        """
        return self.spark.read.parquet(self.paths["countries"])

    def getdata_visa(self):
        """
        Get visa dimension
        :return: visa dimension
        """
        return self.spark.read.parquet(self.paths["visa"])

    def getdata_mode(self):
        """
        Get mode dimension
        :return: mode dimension
        """
        return self.spark.read.parquet(self.paths["mode"])

    def get_facts(self):
        """
        Get facts table
        :return: facts table
        """
        return self.spark.read.parquet(self.paths["facts"])

    def get_dimensions(self):
        """
        Get all dimensions of the model
        :return: all dimensions
        """
        return self.getdata_demographics(), self.getdata_airports(), self.getdata_ports() \
            , self.getdata_countries(), self.getdata_visa(), self.getdata_mode()

    def exists_rows(self, dataframe):
        """
        Checks if there is any data in a dataframe
        :param dataframe: dataframe
        :return: true or false if the dataset has any row
        """
        return dataframe.count() > 0
