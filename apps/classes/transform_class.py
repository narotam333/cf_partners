from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import uuid

class Transformclass:
    """ transforms data """

    def transformData(spark: SparkSession, srcDf: DataFrame, flag: str, out_path: str) -> DataFrame:
        """ rename the default columns based on target table if input file is without header """

        if flag == "false":
            
            deltaDf = spark.read.format("delta").load(out_path)
            deltaDf = deltaDf.drop("ingestion_tms", "batch_id")

            for index, column in enumerate(deltaDf.columns):
                if index > len(srcDf.columns)-1:
                    continue
                oldColumn = '_c'+str(index)
                newColumn = column
                srcDf = srcDf.withColumnRenamed(oldColumn, newColumn)
        
        #srcDf.show(truncate=False)
        
        """ add ingestion timestamp column and uuid column """

        transformedDf = srcDf \
            .withColumn("ingestion_tms", lit(current_timestamp())) \
                .withColumn("batch_id", lit(str(uuid.uuid4())))

        return transformedDf