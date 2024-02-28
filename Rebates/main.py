
import pandas as pd
import numpy as np
import sys

#global vars
year = 2024
month = 2


#applies a 0.1 rebate for all the transactions > 500,000 
rebate_example = {
  "clientId": "C1",
  "rebates": [
    {
      "type": "yearly",
      "fullRebate":False,
      "threshold": 600000,
      "rebate_amount_per_contract": 0.1,
      "exchanges": ["E1", "E2","E3","E4"],
      "salesman_code": ["S1", "S2","S3"],
      "accounts": ["A1", "A2", "A3"],
      "product_name":["P1"]
    },
    {
      "type": "monthly",
      "fullRebate":True,
      "threshold": 300000,
      "rebate_amount_per_contract": 0.01,
      "exchanges": ["E1", "E2","E3","E4"],
      "salesman_code": ["S1", "S2","S3"],
      "accounts": ["A1", "A2", "A3"],
      "product_name":["P1"]
    },
    {
      "type": "default",
      "fullRebate":True,
      "threshold": 10000,
      "rebate_amount_per_contract": 0.001,
      "exchanges": ["E1", "E2","E3","E4"],
      "salesman_code": ["S1", "S2","S3"],
      "accounts": ["A1", "A2", "A3"],
      "product_name":["P1"]
    },   
  ]
}

rebate_example_monthly_threshold = {
  "clientId": "CONTE",
  "rebates": [
    {
      "type": "monthly",
      "fullRebate":True,
      "rebate_rules": [
          {"threshold": 700000, "rebate_amount": .2},
          {"threshold": 1050000, "rebate_amount": .3},
          {"threshold": 1250000, "rebate_amount": .5},
          {"threshold": 1200000, "rebate_amount": .4}
      ],
      "exchanges": [],
      "salesman_code": [],
      "accounts": [],
      "product_name":["OSEMINI"]
    }
  ]
}

rebate_fixed = {
  "clientId": "CONTE",
  "rebates": [
    {
      "type": "yearly",
      "fullRebate":True,
      "rebate_rules": [
          {"threshold": 700000, "rebate_amount": .2},
      ],
      "exchanges": [],
      "salesman_code": [],
      "accounts": [],
      "product_name":["OSEMINI"]
    },
    {
      "type": "yearly",
      "fullRebate":False,
      "rebate_rules": [
          {"threshold": 2000000, "rebate_amount": .2},
      ],
      "exchanges": [],
      "salesman_code": [],
      "accounts": [],
      "product_name":["OSEBIG"]
    }
  ]
}

#if criterias are not independant
yearly_rebate_example_2 = {
  "clientId": "C2",
  "rebates": [
    {
      "type": "yearly",
      "threshold": 200000,
      "rebate_amount_per_contract": 0.1,
      "filters":[
        {
            "exchanges": ["E1", "E2"]
        },
        {
            "salesman_code": ["S1"],
            "accounts": ["A1", "A2"]
        },
        {
            "salesman_code": ["S2"],
            "accounts": ["A1"]
        }
      ],

    }
  ]
}


examples = [rebate_example_monthly_threshold]#, rebate_fixed]

# Columns to exclude from the group by operation
#exclude_columns = ['B']
# Selecting columns to include
#included_columns = [col for col in df.columns if col not in exclude_columns]


def apply_yearly_rebate(rebate, client, df):
    # Calculate and apply yearly rebate logic here
    #print(f'==># Calculate and apply yearly rebate logic here {rebate}')

    #filter on client
    df = df[(df['clientId'] == client)]
    
    if not df.empty:
        #add info on function parameters:
        df['rebate_type'] = f'Type: {rebate.get("type")}'

        # Fill all empty values with 'N/A'
        df = df.fillna('N/A')

        #exclusion param
        exclusion_param = ''
        # Constructing the filter conditions
        exchange_condition = True if not rebate['exchanges'] else df['exchange'].isin(rebate['exchanges'])
        salesman_condition = True if not rebate['salesman_code'] else  df['salesman_code'].isin(rebate['salesman_code'])
        account_condition = True if not rebate['accounts'] else  df['account'].isin(rebate['accounts'])
        product_condition = True if not rebate['product_name'] else  df['product_name'].isin(rebate['product_name'])

        #
        #df['exchange_condition'] = exchange_condition 
        df[['exchange_condition', 'salesman_condition', 'account_condition','product_condition']] = exchange_condition & salesman_condition & account_condition & product_condition

        #create columms to help troubleshoot reason of exclusion
        df['exchange_condition'] = exchange_condition 
        df['salesman_condition'] = salesman_condition 
        df['account_condition'] = account_condition 
        df['product_condition'] = product_condition 

        # Calculate the YTD volume for each combination of clientId, exchange, salesman_code, and account
        df['ytd_volume'] = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].transform('sum')

        # Filter on active month global variable - we now have all the data that we need
        df = df[(df['month'] == month)]
        
        # Create a new column "eligibility" and set it to True for eligible rows
        df['eligibility'] = exchange_condition & salesman_condition & account_condition & product_condition

        # Create a new column "eligibility" and set it to True for eligible rows
        df['exclusion_comment'] = exclusion_param

        # Conditionally calculate the volume eligible, rebated and the actual amount
        df[['threshold_eligibility', 'threshold_rebate', 'volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate, rebate.get('fullRebate')), axis=1)

    return df

#Question - when it is monthly, is it applying on the fully volume when the threshold is met or on transactions above the threshold???
def apply_monthly_rebate(rebate, client, df):
    # Calculate and apply monthly rebate logic here
    #print(f'==># Calculate and apply monthly rebate logic here: {rebate}')

    #filter on client
    df = df[(df['clientId'] == client)]

    if not df.empty:
        # Filter on active month global variable as we do not need previous months data for monthly rebates calculation
        df = df[(df['month'] == month)]

        #add info on function parameters:
        df['rebate_type'] = f'Type: {rebate.get("type")}'

        # Fill all empty values with 'N/A'
        df = df.fillna('N/A')

        # Constructing the filter conditions
        exchange_condition = True if not rebate['exchanges'] else df['exchange'].isin(rebate['exchanges'])
        salesman_condition = True if not rebate['salesman_code'] else  df['salesman_code'].isin(rebate['salesman_code'])
        account_condition = True if not rebate['accounts'] else  df['account'].isin(rebate['accounts'])
        product_condition = True if not rebate['product_name'] else  df['product_name'].isin(rebate['product_name'])

        #create columms to help troubleshoot reason of exclusion
        df['exchange_condition'] = exchange_condition 
        df['salesman_condition'] = salesman_condition 
        df['account_condition'] = account_condition 
        df['product_condition'] = product_condition 

        # Create a new column "eligibility" and set it to True for eligible rows (aside threshold)
        df['eligibility'] = exchange_condition & salesman_condition & account_condition & product_condition

        # Conditionally calculate the volume eligible, rebated and the actual amount
        df[['threshold_eligibility', 'threshold_rebate', 'volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate, rebate.get('fullRebate')), axis=1)

    return df

def apply_default_rebate(rebate, client, df):
    # Calculate and apply default rebate logic here
    print(f'==># Calculate and apply default rebate logic here: {rebate}')

    #filter on client
    df = df[(df['clientId'] == client)]

    # Filter on active month global variable as we do not need previous months data for monthly rebates calculation
    df = df[(df['month'] == month)]

    #add info on function parameters:
    df['rebate_type'] = f'Type: {rebate.get("type")}'
    df['rebate_threshold'] = f'Threshold: {rebate.get("threshold")}'
    df['rebate_amount_per_contract'] = f'Rebate per contract: {rebate.get("rebate_amount_per_contract")}'

    # Constructing the filter conditions - no volume threshold so no eligibility criteria based on that
    exchange_condition = df['exchange'].isin(rebate['exchanges'])
    salesman_condition = df['salesman_code'].isin(rebate['salesman_code'])
    account_condition = df['account'].isin(rebate['accounts'])
    #There is no volume condition - it is a fixed rebate

    # volume_eligible is = volume in the case of fixed rebates
    df['volume_eligible'] =  df['volume']

    # volume_rebated is = volume in the case of fixed rebates
    df['volume_rebated'] =  df['volume']

    # volume_rebated_amount is equal to volume * rebate
    df['volume_rebated_amount'] =  df['volume'] * rebate.get("rebate_amount_per_contract")

    return df


# Function to calculate rebate
def calculate_rebate(row, rebate, fullRebate):
    """
    row: reference the row of the df
    rebate: rebate parameters
    fullRebate: if True, will apply the rebate to the full transactions as soon as threshold is breached, else only the subset of transactions that breached the threshold
    """
    threshold_eligibility = 0
    threshold_rebate = 0
    if row['eligibility']:
        for rule in rebate.get('rebate_rules'):
            if (row['volume']> rule.get('threshold')) and (threshold_eligibility < rule.get('threshold')):
                threshold_eligibility = rule.get('threshold')
                threshold_rebate = rule.get('rebate_amount')
        # if rebate impacts the full volume as soon as we hit the threshold
        if fullRebate:
            volume_eligible = row['volume']
            volume_rebated = row['volume']
        else:
            # if rebate starts after the threshold is breached
            volume_eligible = row['ytd_volume'] - threshold_eligibility if rebate.get("type") == "yearly" else row['volume'] - threshold_eligibility
            volume_rebated = min(volume_eligible, row['volume'])
        
        # amount calculation
        volume_rebated_amount = volume_rebated * threshold_rebate
        return pd.Series([threshold_eligibility, threshold_rebate, volume_eligible, volume_rebated, volume_rebated_amount])
    else:
        return pd.Series([None, None, None, None, None])

# Dictionary mapping rebate types to their corresponding functions
rebate_functions = {
    "monthly": apply_monthly_rebate,
    "yearly": apply_yearly_rebate,
    "default": apply_default_rebate
}


def trigger_rebate_process(rebateEntry):
    print(f'Triggering rebate process for client : {rebateEntry.get("clientId")}')
    rebates = rebateEntry.get("rebates")

    #get volume data
    df = get_volumes(year, month)

    # Create an empty DataFrame that will serve as our output
    dfs = []
    for rebate in rebates:
        # Apply rebate based on client's rebate type
        rebate_type = rebate.get('type')
        if rebate_type in rebate_functions:
            rebate_function = rebate_functions[rebate_type]
            df_output = rebate_function(rebate, rebateEntry.get("clientId"), df)
            dfs.append(df_output)
        else:
            print("Invalid rebate type")


    client_consolidated_df = pd.concat(dfs, ignore_index=True)
    return client_consolidated_df



def get_volumes(year: int, end_month_ref:int):
    """
    get volumes for given year until give month
    """
    file = sys.path[0]+'/AI/utils/monthly_volumes.csv'
    df = pd.read_csv(file)
    df_filtered = df[(df['year'] == year) & (df['month'] <= month)]
    return df_filtered


def main():
    dfs = []
    for rebateEntry in examples:
        dfs.append(trigger_rebate_process(rebateEntry))

    all_dfs = pd.concat(dfs, ignore_index=True)
    print(all_dfs)


if __name__ == "__main__":
    main()