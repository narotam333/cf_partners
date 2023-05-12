import unittest
import os, json
from main import *

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class TestSparkStart(unittest.TestCase):
    def test_normal_case(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)
        self.assertIsInstance(spark_session, SparkSession)
        spark_session.stop()
    def test_negative_case_no_dict_data(self):
        with self.assertRaises(SystemExit) as cm:
            spark_session = sparkStart("Test")
        self.assertEqual(cm.exception.code, 1)

if __name__ == '__main__':
 unittest.main()