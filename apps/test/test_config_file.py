import unittest
from main import *

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestOpenConfig(unittest.TestCase):
    def test_normal_case(self):
        config_file_path = openConfig(f"{project_dir}/config/config.json")
        self.assertIsInstance(config_file_path, dict)
    def test_negative_case_not_exists(self):
        with self.assertRaises(SystemExit) as cm:
            openConfig(f"{project_dir}/test/conf.json")
        self.assertEqual(cm.exception.code, 1)
    def test_negative_case_empty_file(self):
        with self.assertRaises(SystemExit) as cm:
            openConfig(f"{project_dir}/test/config_empty.json")
        self.assertEqual(cm.exception.code, 1)


if __name__ == '__main__':
    unittest.main()