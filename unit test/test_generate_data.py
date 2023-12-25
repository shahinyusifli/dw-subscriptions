import unittest
from ..schedular.utility.generate_data import DataGenerator
from datetime import datetime
import pandas as pd

class TestDataGenerator(unittest.TestCase):
    def setUp(self):
        # Create a DataGenerator instance with a smaller number of rows for testing
        self.generator = DataGenerator(num_rows=10)

    def tearDown(self):
        pass

    def test_generate_data(self):
        # Test if generate_data method returns a Pandas DataFrame
        result_dataframe = self.generator.generate_data()
        self.assertIsInstance(result_dataframe, pd.DataFrame)

        # Check if the DataFrame has the expected columns
        expected_columns = [
            'subscription_type', 'monthly_revenue', 'join_date', 'last_payment_date',
            'cancel_date', 'country', 'age', 'gender', 'device', 'plan_duration',
            'active_profiles', 'household_profile_ind', 'movies_watched', 'series_watched'
        ]
        self.assertCountEqual(result_dataframe.columns, expected_columns)

        # Check if join_date, last_payment_date, and cancel_date are within the expected date ranges
        join_date_range = [datetime(2022, 3, 1), datetime(2022, 10, 31)]
        last_payment_date_range = [datetime(2023, 7, 1), datetime(2023, 6, 30)]
        cancel_date_range = [datetime(2022, 4, 1), datetime(2022, 7, 30)]

        for date in result_dataframe['join_date']:
            self.assertTrue(join_date_range[0] <= date <= join_date_range[1])

        for date in result_dataframe['last_payment_date']:
            self.assertTrue(last_payment_date_range[0] <= date <= last_payment_date_range[1])

        for date in result_dataframe['cancel_date']:
            if date is not None:
                self.assertTrue(cancel_date_range[0] <= date <= cancel_date_range[1])

    def test_generate_data_error_handling(self):
        # Test error handling when an exception occurs during data generation
        generator_with_error = DataGenerator(num_rows=5)
        generator_with_error.random = None  # Simulate an error by removing the 'random' attribute
        result_dataframe = generator_with_error.generate_data()
        self.assertIsNone(result_dataframe)

if __name__ == '__main__':
    unittest.main()
