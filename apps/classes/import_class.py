from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

class Importclass:
    """ handles source files """

    # tag::importData[]
    def importData(spark: SparkSession, in_path: str, flag:str) -> DataFrame:
        """ will return a dataframe"""

        def openFile(spark: SparkSession, in_path: str, flag:str) -> DataFrame:
            df = spark.read.format("csv") \
                .option("header", flag) \
                .load(in_path)

            return df

        return openFile(spark, in_path, flag)

    # end::importData[]