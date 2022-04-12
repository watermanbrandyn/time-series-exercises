import os

import pandas as pd

import requests

#-------------------- Acquisition Function for API

def api_gather(domain, endpoint, target, use_cache=True):
    '''
    This function takes in our domain, (initial) endpoint, and target variable as strings. It returns the list, as 
    a dataframe, of the acquired target after cycling until the current endpoint is type 'None'.
    '''
    # Check if csv already exists locally by using OS and cache
    if os.path.exists(f'{target}.csv') and use_cache:
        print('Using cached csv')
        return pd.read_csv(f'{target}.csv')
    # Creation of domain and endpoint strings to use for api acquisition
    # Creation of new empty list to extend to when page is assigned to 'data'
    domain = domain
    endpoint = endpoint
    list_ = []
    # While loop until endpoint is 'None'
    while endpoint is not None:
        url = domain + endpoint
        response = requests.get(url)
        data = response.json()
        list_.extend(data['payload'][target])
        endpoint = data['payload']['next_page']
    # Creating a dataframe out of the list_
    df = pd.DataFrame(list_)
    # Creating csv file from dataframe
    df.to_csv(f'{target}.csv', index=False)
    # Return dataframe
    return df

# Creation of our domain for use with api_gather()
domain = 'https://python.zgulde.net'

# Creation of our endpoints and targets for use with api_gather()
# These would be called with our function to acquire the three dataframes

endpoint_items = '/api/v1/items'
target_items = 'items'

endpoint_stores = '/api/v1/stores'
target_stores = 'stores'

endpoint_sales = '/api/v1/sales'
target_sales = 'sales'

# Acquiring and merging all three dataframes from API
def acquire_merge_dfs():
    '''
    This function acquires our three dataframes and merges them on the proper columns using inner joins. Returns one large dataframe.
    '''
    # Acquisition of our three dataframes
    items = api_gather(domain, endpoint_items, target_items)
    stores = api_gather(domain, endpoint_stores, target_stores)
    sales = api_gather(domain, endpoint_sales, target_sales) 
    # Rename the store and item columns in sales for ease and posterity
    sales = sales.rename(columns={'store': 'store_id', 'item': 'item_id'})
    # Merging the dataframes on the store_id and item_id columns
    df = pd.merge(sales, items, how='left', on='item_id')
    df = pd.merge(df, stores, how='left', on='store_id')
    return df

# Germany OPSD acquire
def acquire_energy():
    '''
    This function acquires the Open Power Systems Data (OPSD) for Germany. (2006-2017)
    '''
    url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
    energy = pd.read_csv(url)
    return energy

