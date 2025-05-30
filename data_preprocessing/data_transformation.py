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
        7. Week column

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

        # Add additional week column for price date
        df['week'] = df['price_date'].dt.isocalendar().week

        return df

    ########### Individual Data Cleaning Functions ###########

    # Convert columns to datetime
    def convert_datetime_col(self, date_columns: List[str]) -> DataFrame:
        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col])
        return self.df

    # Fill default value as median for missing numerical values
    def fill_missing_numeric(self) -> DataFrame:
        numeric_cols = self.df.select_dtypes(
            include=['int64', 'float64']).columns
        for col in numeric_cols:
            self.df[col].fillna(self.df[col].median())
        return self.df

    # Fill default value as NA for missing categorical values
    def fill_missing_categorical(self) -> pd.DataFrame:
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            self.df[col] = self.df[col].fillna('NA')
        return self.df

    # Standardise specific categorical columns by converting to upper case
    def standardize_text_columns(self, columns: List[str]) -> DataFrame:
        for col in columns:
            if col in self.df.columns and self.df[col].dtype == 'object':
                self.df[col] = self.df[col].str.strip().str.upper()
        return self.df

    # Format column names to snake_case
    def format_column_names_to_snake_case(self) -> DataFrame:
        self.df.columns = [
            re.sub(r'\.', '',
                   re.sub(r'\s+', '_', c).lower()
                   if ' ' in c else re.sub(r'([a-z0-9])([A-Z])', r'\1_\2',
                                           re.sub(r'([a-z])([A-Z])', r'\1_\2', c)).lower()
                   )
            for c in self.df.columns
        ]
        return self.df

    # Trim trailing and leading spaces from categorical value
    def trim_spaces(self, columns: List[str] = []) -> DataFrame:
        # Select columns with object or string dtype if none specified
        columns_to_process = columns if columns else self.df.select_dtypes(
            include=['object', 'string']).columns

        for col in columns_to_process:
            if self.df[col].dtype in ['object', 'string']:
                # Force convert to string to avoid .str errors
                self.df[col] = self.df[col].astype(str).str.strip()
            else:
                print(
                    f"Column '{col}' is not of object or string dtype — trimming not required.")

        return self.df

    # Return clean df

    def get_transformed_data(self) -> DataFrame:
        return self.df
