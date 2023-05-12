import unittest
import json
from main import *

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class TestImportData(unittest.TestCase):
    def test_normal_case_true_flag(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)
        df = importData(spark_session, f"{project_dir}/../data/cf_true_test", "true")
        self.assertIsInstance(df, DataFrame)
        spark_session.stop()
    def test_normal_case_false_flag(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)
        df = importData(spark_session, f"{project_dir}/../data/cf_false_test", "false")
        self.assertIsInstance(df, DataFrame)
        spark_session.stop()
    def test_negative_case_no_file(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)
        with self.assertRaises(SystemExit) as cm:
            importData(spark_session, f"{project_dir}/../data/cf_empty_test", "true")
        print(cm.exception.code)
        self.assertEqual(cm.exception.code, 0)
    def test_negative_case_invalid_path(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)
        with self.assertRaises(SystemExit) as cm:
            importData(spark_session, f"{project_dir}/../data/cf", "true")
        self.assertEqual(cm.exception.code, 1)

if __name__ == '__main__':
 unittest.main()