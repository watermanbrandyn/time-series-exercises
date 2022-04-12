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

# # Creation of our domain for use with api_gather()
# domain = 'https://python.zgulde.net'

# # Creation of our endpoints and targets for use with api_gather()
# # These would be called with our function to acquire the three dataframes

# endpoint_items = '/api/v1/items'
# target_items = 'items'

# endpoint_stores = '/api/v1/stores'
# target_stores = 'stores'

# endpoint_sales = '/api/v1/sales'
# target_sales = 'sales'

# Merging the three dataframes from API
def merge_dfs(items, stores, sales):
    '''
    This function takes in our three dataframes and merges them on the proper columns using inner joins. Returns one large dataframe.
    '''
    df = pd.merge(items, sales, how='inner', left_on='item_id', right_on='item')
    df = pd.merge(df, stores, how='inner', left_on='store', right_on='store_id')
    return df


def acquire_energy():
    '''
    This function acquires the Open Power Systems Data (OPSD) for Germany. (2006-2017)
    '''
    url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
    energy = pd.read_csv(url)
    return energy

