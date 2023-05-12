import unittest
import json, collections
from main import *

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class TestLoadData(unittest.TestCase):

    def test_normal_case_load(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)
        srcDf = importData(spark_session, f"{project_dir}/../data/cf_in", "true")

        tgtDf = transformData(spark_session, srcDf, "true", f"{project_dir}/../data/cf_out")
        
        res = loadData(spark_session, tgtDf, f"{project_dir}/../data/cf_out")

        self.assertEqual(res, True)
        spark_session.stop()

    def test_normal_case_archive(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)
        srcDf = importData(spark_session, f"{project_dir}/../data/cf_in", "true")

        tgtDf = transformData(spark_session, srcDf, "true", f"{project_dir}/../data/cf_out")
        
        res = loadData(spark_session, tgtDf, f"{project_dir}/../data/cf_out")

        arch = archiveData(spark_session, f"{project_dir}/../data/cf_in", f"{project_dir}/../data/cf_processed")

        self.assertEqual(arch, True)
        spark_session.stop()

if __name__ == '__main__':
 unittest.main()