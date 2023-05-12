import unittest
import json
from main import *

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class TestTransformData(unittest.TestCase):
    def test_normal_case(self):
        with open(f"{project_dir}/config/config.json", "r") as f:
            data = json.load(f)
        spark_session = sparkStart(data)

        srcDf = importData(spark_session, f"{project_dir}/../data/cf_true_test", "true")

        transformedDf = transformData(spark_session, srcDf, "true", f"{project_dir}/../data/cf_out")
        self.assertIsInstance(transformedDf, DataFrame)
        spark_session.stop()

if __name__ == '__main__':
 unittest.main()