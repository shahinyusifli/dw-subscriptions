import pandas as pd
from random import choice, randint, random
from datetime import datetime, timedelta

class DataGenerator:
    def __init__(self, num_rows=100):
        """
        Initializes the DataGenerator with the number of rows to generate.

        Args:
            num_rows (int): Number of rows to generate in the dataset (default is 100).
        """
        self.num_rows = num_rows

    def generate_data(self):
        """
        Generates synthetic data for a subscription dataset.

        Returns:
            pd.DataFrame or None: A Pandas DataFrame containing the generated data, or None if an error occurs.
        """
        try:
            # Define data ranges for various attributes
            subscription_type = ['Basic', 'Premium', 'Standard']
            monthly_revenue = [10, 11, 12, 13, 14, 15]
            join_date_range = [datetime(2022, 3, 1), datetime(2022, 10, 31)]
            last_payment_date_range = [datetime(2023, 7, 1), datetime(2023, 6, 30)]
            cancel_date_range = [datetime(2022, 4, 1), datetime(2022, 7, 30)]  # Nullable

            country = ['Brazil', 'Italy', 'UK', 'US', 'Germany', 'Mexico', 'France']
            age_range = (6, 120)
            gender = ['Male', 'Female']
            device = ['Laptop', 'Smart TV', 'Smartphone', 'Tablet']
            plan_duration = ['1 month', '6 month']
            active_profiles = [1, 2, 3, 4, 5, 6, 7, 8]
            household_profile_ind = [1, 2]
            movies_watched = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            series_watched = [1, 2, 3, 4]

            # Generate data
            data = []
            for _ in range(self.num_rows):
                # Generate random dates within specified ranges
                join_date = join_date_range[0] + timedelta(days=int((join_date_range[1] - join_date_range[0]).days * random()))
                last_payment_date = last_payment_date_range[0] + timedelta(days=int((last_payment_date_range[1] - last_payment_date_range[0]).days * random()))

                # Introduce chance for cancel_date to be None
                cancel_date = None if random() < 0.2 else cancel_date_range[0] + timedelta(days=int((cancel_date_range[1] - cancel_date_range[0]).days * random()))

                # Create a dictionary for each row of data
                row = {
                    'subscription_type': choice(subscription_type),
                    'monthly_revenue': choice(monthly_revenue),
                    'join_date': join_date,
                    'last_payment_date': last_payment_date,
                    'cancel_date': cancel_date,
                    'country': choice(country),
                    'age': randint(age_range[0], age_range[1]),
                    'gender': choice(gender),
                    'device': choice(device),
                    'plan_duration': choice(plan_duration),
                    'active_profiles': choice(active_profiles),
                    'household_profile_ind': choice(household_profile_ind),
                    'movies_watched': choice(movies_watched),
                    'series_watched': choice(series_watched)
                }
                data.append(row)

            # Create a Pandas DataFrame from the generated data
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            # Print an error message if an exception occurs during data generation
            print(f"Error generating data: {e}")
            return None
