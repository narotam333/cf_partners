import json
from pyspark.sql import SparkSession

class Sparkclass:
    """ handles config file and spark tasks """

    # tag::openJson[]
    def openJson(filepath: str) -> dict:
        """ open a json file and return a dict """
        with open(filepath, "r") as f:
            data = json.load(f)
        return data

    # end::openJson[]

    # tag::sparkStart[]
    def sparkStart(kwargs:dict) -> SparkSession:
        """ spark session from dict configuraton """

        def createSession(master: str, appname: str) -> SparkSession:
            """ create a spark session """
            builder = SparkSession \
                .builder \
                .master(master) \
                .appName(appname)
            return builder.getOrCreate()

        def setLogging(spark: SparkSession, log_level: str) -> None:
            """ set log level - ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN """
            if isinstance(spark, SparkSession):
                spark.sparkContext.setLogLevel(log_level) if isinstance(log_level, str) else None

        MASTER = kwargs.get('spark_conf', {}).get('master', 'local[*]')
        APPNAME = kwargs.get('spark_conf', {}).get('appname', 'cgi')
        LOG_LEVEL = kwargs.get('log', {}).get('level', None)

        spark = createSession(MASTER, APPNAME)
        setLogging(spark, LOG_LEVEL)
        return spark

    # end::sparkStart[]