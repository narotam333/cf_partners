# cf_partners

**Dockerfile** - used to build image for spark

**spark-defaults.conf** - properties for history server

**start-spark.sh** - starts the Spark master and worker service, binds it to a specific IP address and port, and configures the web UI port. 
It also writes the console output to a log file for debugging and monitoring purposes.

**docker-compose.yml** - creates spark master, workers and history server containers using image built by Dockerfile


**Folder Structure:**
    **apps** - 
        **main.py** - Pyspark code to import csv (with or without header) file(s), transform and load them to the delta table. 
        This script takes 3 parameters (input csv file path, output path for delta table and header flag for input csv file(s))
        **classes** - Folder contains classes required in main.py
        **config** - contains configuration file for spark job
        **test** - Folder contains test scripts for main.py
        **venv** - virtual env to run test locally
    **data** - input folder for csv files (cf_in), output folder for delta table (cf_out), archive folder (cf_processed) and 
           test folders for unittests
    **logs** - folder to hold all the event logs
    **venv** - venv to run the tests locally
    
***NOTE**: cf_in folder needs to be created manually under data folder
