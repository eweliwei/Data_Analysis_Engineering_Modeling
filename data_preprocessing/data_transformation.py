import re
import pandas as pd
import numpy as np
import logging
from typing import List, Optional
from pandas import DataFrame

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataTransformer:
    def __init__(self, df: Optional[DataFrame] = None) -> None:
        self.df = df if df is not None else pd.DataFrame()

    def transform_fuel_data(self) -> DataFrame:
        """
        Transformation Steps:
        1. Convert JSON to DataFrame (if input is not already a DataFrame)
        2. Rename columns
        3. Convert string dates to datetime objects
        4. Sort by dates

        Additional Columns:
        1. Weekly difference in prices 
        2. Rolling averages (4 weeks)
        3. Month and year based on price_date
        4. Cumulative changes from start date
        5. Binary column that indicates price increase from previous week
        6. Calculate price percentage changes

        """
        # 1. Use input data if provided, otherwise use self.df
        df = pd.DataFrame(self.df.copy())

        # 2. Rename columns (corrected case sensitivity in fuel types)
        column_mapping = {
            'ron95': 'ron95_price',
            'ron97': 'ron97_price',
            'diesel': 'diesel_price',
            'date': 'price_date'
        }
        df = df.rename(columns=column_mapping)

        # 3. Convert string dates to datetime objects
        df['price_date'] = pd.to_datetime(df['price_date'])

        # 4. Sort by price_date
        df = df.sort_values('price_date').reset_index(drop=True)

        # 5. Replace NaN with None
        df = df.fillna(np.nan).replace({np.nan: None})

        # Additional col 1: Calculate weekly price differences
        df['ron95_week_diff'] = df['ron95_price'].diff()
        df['ron97_week_diff'] = df['ron97_price'].diff()
        df['diesel_week_diff'] = df['diesel_price'].diff()

        # Additional col 2: Calculate rolling averages (4 weeks)
        # Fixed: Column names should match renamed columns (lowercase)
        for fuel in ['ron95', 'ron97', 'diesel']:
            df[f'{fuel}_4wk_avg'] = df[f'{fuel}_price'].rolling(
                window=4).mean()

        # Additional col 3: Add month, year, and week of year
        df['year'] = df['price_date'].dt.year
        df['month'] = df['price_date'].dt.month

        # Additional col 4: Cumulative changes from start date
        # Fixed: Use lowercase column names
        start_price = df.iloc[0]
        for fuel in ['ron95', 'ron97', 'diesel']:
            df[f'{fuel}_cumulative_change'] = (
                df[f'{fuel}_price'] - start_price[f'{fuel}_price']
            )

        # Additional col 5: Binary column for price increase
        for fuel in ['ron95', 'ron97', 'diesel']:
            df[f'{fuel}_price_increase'] = (
                df[f'{fuel}_week_diff'] > 0).astype(int)

        # Additional col 6: Calculate percentage changes
        for fuel in ['ron95', 'ron97', 'diesel']:
            df[f'{fuel}_pct_change'] = df[f'{fuel}_price'].pct_change() * 100

        # Replace NaN with None
        df = df.fillna(np.nan).replace({np.nan: None})

        return df
