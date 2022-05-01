import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from datetime import datetime


def prep_stores(df):
    '''
    Takes the df of items, store, sale info and converts index to datetime from sale_date, adds month and day of
    week columns, adds a sales_total column (sale_amount * item_price)
    '''
    # Convert to datetime
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y %H:%M:%S %Z')
    # Make index
    df = df.set_index('sale_date').sort_index()
    # Create new columns
    df['month'] = df.index.strftime('%m-%b')
    df['weekday'] = df.index.strftime('%A')
    df['sales_total'] = df.sale_amount * df.item_price
    # Return the df
    return df


def prep_opsd(df):
    '''
    Preparation of OPS data
    '''
    # Convert data to datetime and set to index
    df['Date'] = pd.to_datetime(df.Date)
    df = df.set_index('Date').sort_index()
    # Creation of month and year columns
    df['month'] = df.index.strftime('%m-%b')
    df['year'] = df.index.year
    # Filling nulls
    df['Wind'] = df.Wind.fillna(0)
    df['Solar'] = df.Solar.fillna(0)
    df['Wind+Solar'] = df.Wind + df.Solar
    # Return df
    return df
