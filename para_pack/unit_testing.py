import unittest
from pyspark.sql import SparkSession
from para_pack.assignment import clean_data, filter_countries

class TestDataCleaning(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create or get the SparkSessionn
        cls.spark = SparkSession.builder.appName("TestCleanData").getOrCreate()

        # Paths used for the data
        cls.df1_path = '../ABN/DATA/dataset_one.csv'
        cls.df2_path = '../ABN/DATA/dataset_two.csv'
        cls.countries = ['United Kingdom', 'Netherlands']

        # Load the DataFrame using the clean_data function or any other method
        cls.df = filter_countries(cls.spark.read.csv(cls.df1_path, header=True, inferSchema=True), 'country', cls.countries)

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession after all tests are done
        cls.spark.stop()

    def test_contains_only_united_kingdom_and_netherlands(self):
        # Get a list of unique countries in the DataFrame
        actual_countries = self.df.select("country").distinct().rdd.flatMap(lambda x: x).collect()

        # Compare the list of countries with the expected values
        expected_countries = ['United Kingdom', 'Netherlands']
        self.assertCountEqual(actual_countries, expected_countries)

if __name__ == "__main__":
    unittest.main()