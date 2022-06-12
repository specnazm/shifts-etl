"""
test_kpis_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in kpis_job.py.
"""
import unittest

from pyspark.sql import SparkSession
from app.tests.schemas import shift_schema, break_schema
from app.jobs.kpis_job import mean_break_len, total_paid_breaks, min_shift_len, max_allowance_cost_14d, max_break_free_period, mean_shift_cost
import datetime
import time

"""Data used for unix time in csv files."""
shift_1 = [datetime.datetime(2021, 7, 19, 9,30), datetime.datetime(2021,9, 17, 16,45)]
shift_2 = [datetime.datetime(2021,9, 17, 7, 45), datetime.datetime(2021, 9, 17, 16,45)]
shift_3 = [datetime.datetime(2022, 5, 14, 8, 3), datetime.datetime(2022, 5, 14, 9, 30)]     #shift 1 hour, smallest value
#used in csv file shifts start and finish values
unix_dates_1 = [time.mktime(date_time.timetuple()) for date_time in shift_1]
unix_dates_2 = [time.mktime(date_time.timetuple()) for date_time in shift_2]
unix_dates_3 = [time.mktime(date_time.timetuple()) for date_time in shift_3]


break_1 = [datetime.datetime(2021, 2, 2, 9,30), datetime.datetime(2021,2, 2, 9, 45)]      #break 15 mins
break_2 = [datetime.datetime(2021,2, 2, 7, 45), datetime.datetime(2021, 2, 2, 7, 55)]     #break 10 mins
break_3 = [datetime.datetime(2022, 2, 2, 8, 30), datetime.datetime(2022, 2, 2, 8, 50)]     #break 20 mins,         average 15 min

#used in csv file break start and finish values
unix_dates_1 = [time.mktime(date_time.timetuple()) for date_time in break_1]
unix_dates_2 = [time.mktime(date_time.timetuple()) for date_time in break_2]
unix_dates_3 = [time.mktime(date_time.timetuple()) for date_time in break_3]

class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        spark_builder = SparkSession.builder.appName('testing-app')
        self.spark = spark_builder.getOrCreate()

        self.test_data_path = 'test_data.json'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_min_shift_len(self):
        df = self.spark.read.option("header",True).schema(shift_schema).csv("tests/test_data/shifts.csv")
        expected = 1
        actual = min_shift_len(df)

        self.assertEqual(expected, actual)

    def test_total_paid_breaks(self):
        df = self.spark.read.option("header",True).csv("tests/test_data/breaks.csv")
        expected = 2
        actual = total_paid_breaks(df)

        self.assertEqual(expected, actual)

    def test_mean_shift_cost(self):
        df = self.spark.read.option("header", True).schema(shift_schema).csv("tests/test_data/shifts.csv")
        expected = 6
        actual = mean_shift_cost(df)

        self.assertEqual(expected, actual)

    def test_mean_break_len(self):
        df = self.spark.read.option("header", True).schema(break_schema).csv("tests/test_data/breaks.csv")
        expected = 15
        actual = mean_break_len(df)

        self.assertEqual(expected, actual)

    def test_max_break_free_period(self):
        break_df = self.spark.read.option("header", True).schema(break_schema).csv("tests/test_data/breaks.csv")
        shift_df = self.spark.read.option("header", True).schema(shift_schema).csv("tests/test_data/shifts.csv")

        expected = 4
        actual = max_break_free_period(break_df, shift_df)

        self.assertEqual(expected, actual)




if __name__ == '__main__':
    unittest.main()
