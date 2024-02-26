
import pandas as pd
import numpy as np
import sys

#global vars
year = 2024
month = 2


#applies a 0.1 rebate for all the transactions > 500,000 
yearly_rebate_example = {
  "clientId": "C1",
  "rebates": [
    {
      "type": "yearly",
      "threshold": 600000,
      "rebate_amount_per_contract": 0.1,
      "exchanges": ["E1", "E2","E3","E4"],
      "salesman_code": ["S1", "S2","S3"],
      "accounts": ["A1", "A2", "A3"],
      "product_name":["P1"]
    },
    {
      "type": "monthly",
      "threshold": 300000,
      "rebate_amount_per_contract": 0.01,
      "exchanges": ["E1", "E2","E3","E4"],
      "salesman_code": ["S1", "S2","S3"],
      "accounts": ["A1", "A2", "A3"],
      "product_name":["P1"]
    },
    {
      "type": "default",
      "threshold": 10000,
      "rebate_amount_per_contract": 0.001,
      "exchanges": ["E1", "E2","E3","E4"],
      "salesman_code": ["S1", "S2","S3"],
      "accounts": ["A1", "A2", "A3"],
      "product_name":["P1"]
    },   
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


examples = [yearly_rebate_example]

# Columns to exclude from the group by operation
#exclude_columns = ['B']
# Selecting columns to include
#included_columns = [col for col in df.columns if col not in exclude_columns]


def apply_yearly_rebate(rebate, client, df):
    # Calculate and apply yearly rebate logic here
    print(f'==># Calculate and apply yearly rebate logic here {rebate}')

    #filter on client
    df = df[(df['clientId'] == client)]
    
    #add info on function parameters:
    df['rebate_type'] = f'Type: {rebate.get("type")}'
    df['rebate_threshold'] = f'Threshold: {rebate.get("threshold")}'
    df['rebate_amount_per_contract'] = f'Rebate per contract: {rebate.get("rebate_amount_per_contract")}'

    # Constructing the filter conditions
    # ==> are those filters mutually independant or not?? to be checked
    exchange_condition = df['exchange'].isin(rebate['exchanges'])
    salesman_condition = df['salesman_code'].isin(rebate['salesman_code'])
    account_condition = df['account'].isin(rebate['accounts'])
    
    # Calculate the YTD volume for each combination of clientId, exchange, salesman_code, and account
    df['ytd_volume'] = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].transform('sum')
    
    # Filter on active month global variable - we now have all the data that we need
    df = df[(df['month'] == month)]
    
    #new filter condition on ytd volume
    volume_condition = df['ytd_volume'] > rebate['threshold']
    
    # Create a new column "eligibility" and set it to True for eligible rows
    df['eligibility'] = exchange_condition & salesman_condition & account_condition & volume_condition

    # Conditionally calculate 'volume_eligible'
    #df['volume_eligible'] = df.apply(lambda row: row['ytd_volume'] - rebate['threshold'] if row['eligibility'] else None, axis=1)


    # Conditionally calculate the volume eligible, rebated and the actual amount
    df[['volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate, True), axis=1)

    return df

#Question - when it is monthly, is it applying on the fully volume when the threshold is met or on transactions above the threshold???
def apply_monthly_rebate(rebate, client, df):
    # Calculate and apply monthly rebate logic here
    print(f'==># Calculate and apply monthly rebate logic here: {rebate}')

    #filter on client
    df = df[(df['clientId'] == client)]

    # Filter on active month global variable as we do not need previous months data for monthly rebates calculation
    df = df[(df['month'] == month)]

    #add info on function parameters:
    df['rebate_type'] = f'Type: {rebate.get("type")}'
    df['rebate_threshold'] = f'Threshold: {rebate.get("threshold")}'
    df['rebate_amount_per_contract'] = f'Rebate per contract: {rebate.get("rebate_amount_per_contract")}'

    # Constructing the filter conditions
    # ==> are those filters mutually independant or not?? to be checked
    exchange_condition = df['exchange'].isin(rebate['exchanges'])
    salesman_condition = df['salesman_code'].isin(rebate['salesman_code'])
    account_condition = df['account'].isin(rebate['accounts'])
    volume_condition = df['volume'] > rebate['threshold']

    # Create a new column "eligibility" and set it to True for eligible rows
    df['eligibility'] = exchange_condition & salesman_condition & account_condition & volume_condition
    
    # Conditionally calculate 'volume_eligible'
    #df['volume_eligible'] = df.apply(lambda row: row['volume'] - rebate['threshold'] if row['eligibility'] else None, axis=1)

   # Conditionally calculate the volume eligible, rebated and the actual amount
    df[['volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate, True), axis=1)

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
    #when is volume eligible <> volume rebated ??

    if row['eligibility']:
        # if rebate impacts the full volume as soon as we hit the threshold
        if fullRebate:
            volume_eligible = row['volume']
            volume_rebated = row['volume']
        else:
            # if rebate starts after the threshold is breached
            volume_eligible = row['ytd_volume'] - rebate.get("threshold") if rebate.get("type") == "yearly" else row['volume'] - rebate.get("threshold")
            volume_rebated = min(volume_eligible, row['volume'])
        
        # amount calculation
        volume_rebated_amount = volume_rebated * rebate.get("rebate_amount_per_contract")
        return pd.Series([volume_eligible, volume_rebated, volume_rebated_amount])
    else:
        return pd.Series([None, None, None])

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
    df_output = pd.DataFrame()

    for rebate in rebates:
        # Apply rebate based on client's rebate type
        rebate_type = rebate.get('type')
        if rebate_type in rebate_functions:
            rebate_function = rebate_functions[rebate_type]
            df_rebate = rebate_function(rebate, rebateEntry.get("clientId"), df)
            df_output = pd.concat([df_output, df_rebate], ignore_index=True)
        else:
            print("Invalid rebate type")
    
    print(df_output)




def get_volumes(year: int, end_month_ref:int):
    """
    get volumes for given year until give month
    """
    file = sys.path[0]+'/AI/utils/monthly_volumes.csv'
    df = pd.read_csv(file)
    df_filtered = df[(df['year'] == year) & (df['month'] <= month)]
    return df_filtered


def main():
    for rebateEntry in examples:
        trigger_rebate_process(rebateEntry)


if __name__ == "__main__":
    main()