#!/usr/bin/python3

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from delta import *

from classes.spark_class import Sparkclass
from classes.import_class import Importclass
from classes.transform_class import Transformclass
from classes.load_class import Loadclass

project_dir = os.path.dirname(os.path.abspath(__file__))

def main():
    """ purpose of this application is to,
        1. import raw data (csv files with or without header) into spark dataframe
        2. transform the dataframe based on header flag
        3. export the dataframe result to a delta table in append mode
    """

    # start session from json config file
    conf = openConfig(f"{project_dir}/config/config.json")
    spark = sparkStart(conf)

    # 1. import the raw data into a spark dataframe
    srcDf = importData(spark, in_path, flag)
    
    # 2. transform the data to create target dataframe
    tgtDf = transformData(spark, srcDf, flag, out_path)

    # 3. create delta table using target dataframe
    loadData(spark, tgtDf, out_path)

    # 4. archive processed file(s)
    archive_path = f"{project_dir}/../spark-data/cf_processed"
    archiveData(spark, in_path, archive_path)

    # close and quit the spark session
    stopSpark(spark)


def openConfig(filepath:str) -> dict:
    """ opens the json configuration file  """
    if os.path.exists(filepath) and os.path.getsize(filepath) != 0:
        return Sparkclass.openJson(filepath)
    elif not os.path.exists(filepath):
        print("ERROR: "+filepath+" doesn't exists")
        sys.exit(1)
    elif os.path.getsize(filepath) == 0:
        print("ERROR: "+filepath+" is empty")
        sys.exit(1)

def sparkStart(conf:dict) -> SparkSession:
    """ start a spark session """
    if isinstance(conf, dict):
        return Sparkclass.sparkStart(conf)
    else:
        print("ERROR: Incorrect data set-up in config_empty.json file")
        sys.exit(1)

def stopSpark(spark) -> None:
    """ ends the spark session """
    spark.stop() if isinstance(spark, SparkSession) else None

def importData(spark:SparkSession, in_path:str, flag:str) -> DataFrame:
    """ get data from source directory """
    if os.path.exists(in_path) and len(os.listdir(in_path)) != 0:
        return Importclass.importData(spark, in_path, flag)
    elif not os.path.exists(in_path):
        print("ERROR: "+in_path+" doesn't exists")
        spark.stop()
        sys.exit(1)
    elif len(os.listdir(in_path)) == 0:
        print("INFO: "+in_path+" is empty")
        spark.stop()
        sys.exit(0)

def transformData(spark:SparkSession, srcDf:DataFrame, flag: str, out_path: str) -> DataFrame:
    """ call your custom functions to transform your data """
    if isinstance(srcDf, DataFrame):
        return Transformclass.transformData(spark, srcDf, flag, out_path)

def loadData(spark:SparkSession, tgtDf:DataFrame, out_path:str) -> None:
    """ call your custom functions to load your delta table """
    if isinstance(tgtDf, DataFrame) and os.path.exists(out_path):
        return Loadclass.loadData(spark, tgtDf, out_path)
    elif not os.path.exists(out_path):
        print("ERROR: "+out_path+" doesn't exists")
        spark.stop()
        sys.exit(1)
    elif not isinstance(tgtDf, DataFrame):
        print("ERROR: tgtDf is not a dataframe")
        spark.stop()
        sys.exit(1)

def archiveData(spark:SparkSession, in_path:str, archive_path:str) -> None:
    """ call your custom functions to archive your data """
    if os.path.exists(archive_path):
        Loadclass.archiveData(spark, in_path, archive_path)
    else:
        print("ERROR: "+archive_path+" doesn't exists")
        spark.stop()
        sys.exit(1)


if __name__ == '__main__':

    # Create the parser
    my_parser = argparse.ArgumentParser(add_help=False, description='Input folder path, output delta path and header indicator')

    # Add the arguments
    my_parser.add_argument("-i",
                       dest = 'input_path',
                       type = str,
                       default = "/opt/spark-data/cf_in",
                       help="Input path for csv file(s)")
    my_parser.add_argument("-o",
                       dest = 'output_path',
                       type = str,
                       default = "/opt/spark-data/cf_out/delta/cf",
                       help="Output path for delta table")
    my_parser.add_argument("-h",
                       dest = 'header_flag',
                       type = str,
                       default = "true",
                       help="header flag")

    # Execute the parse_args() method
    args = my_parser.parse_args()

    in_path = args.input_path
    out_path = args.output_path
    flag = args.header_flag
    main()