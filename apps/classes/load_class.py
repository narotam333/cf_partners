from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import shutil
import os, sys

class Loadclass:
    """ load delta table """

    def loadData(spark: SparkSession, tgtDf: DataFrame, out_path: str):
        """ load delta table using transformed dataframe """

        try:
            tgtDf.write.format("delta").mode("append").option("mergeSchema", "true").save(out_path)
        except ValueError as e:
            print("Error: ", e)
            spark.stop()
            sys.exit(1)
        
    def archiveData(spark: SparkSession, in_path: str, archive_path: str):
        """ archive processed files """

        try:
            files = [f for f in os.listdir(in_path)]
            
            for file in files:
                path1 = in_path + '/' + file
                path2 = archive_path + '/' + file
                shutil.move(path1, path2)
        except ValueError as e:
            print("Error: ", e)
            spark.stop()
            sys.exit(1)