
import pandas as pd
import numpy as np
import sys

#global vars
year = 2024
month = 2

rebate_CONTE =  {
  "clientId": "CONTE",
  "rebates": [
    {
      "type": "monthly",
      "fullRebate":True,
      "rebate_rules": [
          {"threshold": 0, "rebate_amount": .1},
          {"threshold": 700000, "rebate_amount": .2},
          {"threshold": 1050000, "rebate_amount": .3},
          {"threshold": 1250000, "rebate_amount": .5},
          {"threshold": 1200000, "rebate_amount": .4}
      ],
      "filters": [
        {"exchanges": []},
        {"salesman_code": []},
        {"accounts": []},
        {"product_name":['OSEMINI','OSEBIG']}
      ]
    },
    {
      "type": "yearly",
      "fullRebate":False,
      "rebate_rules": [
          {"threshold": 1000000, "rebate_amount": .1},
      ],
      "filters": [
        {"exchanges": []},
        {"salesman_code": []},
        {"accounts": []},
        {"product_name":['OSEMINI']}
      ]
    },
    {
      "type": "monthly",
      "fullRebate":False,
      "rebate_rules": [
          {"threshold": 700000, "rebate_amount": .8}
      ],
      "filters": [
        {"exchanges": []},
        {"salesman_code": []},
        {"accounts": []},
        {"product_name":['OSEBIG']}
      ]
    },
    {
      "type": "monthly",
      "fullRebate":False,
      "rebate_rules": [
          {"threshold": 700000, "rebate_amount": .8}
      ],
      "filters": [
        {"exchanges": []},
        {"salesman_code": []},
        {"accounts": []},
        {"product_name":['HKFE']}
      ]
    },
  ]
}


rebate_C1 =  {
  "clientId": "C1",
  "rebates": [
    {
      "type": "monthly",
      "fullRebate":True,
      "rebate_rules": [
          {"threshold": 0, "rebate_amount": .1},
      ],
      "filters": [
        {"exchange": ['E3']},
        {"salesman_code": ['S1','SO']},
        {"accountId": []},
        {"product_name":[]}
      ]
    },
    {
      "type": "yearly",
      "fullRebate":False,
      "rebate_rules": [
          {"threshold": 300000, "rebate_amount": .1},
      ],
      "filters": [
        {"exchange": ['E1']},
        {"salesman_code": ['S2']},
        {"accountId": []},
        {"product_name":['P1','P6']}
      ]
    },
  ]
}

clientRebatesList = [rebate_CONTE, rebate_C1]#, rebate_fixed]

# Columns to exclude from the group by operation
#exclude_columns = ['B']
# Selecting columns to include
#included_columns = [col for col in df.columns if col not in exclude_columns]


def apply_yearly_rebate(rebate, client, df):
    # Calculate and apply yearly rebate logic here
    print(f'==># Calculate and apply yearly rebate logic here {rebate}')
    # Fill all empty values with 'N/A'
    #df = df.fillna('N/A')
    df = df.fillna(np.nan)

    # Calculate the YTD volume for each combination of clientId, exchange, salesman_code, and account
    #df['ytd_volume'] = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].transform('sum')

    # Sort DataFrame by clientId and period
    df.sort_values(by=['clientId','exchange','account','salesman_code','product_name','period'],inplace=True)

    # Group by clientId and calculate YTD
    df['ytd_volume'] = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].cumsum()

    # Filter on active month global variable - we now have all the data that we need
    #df = df[(df['month'] == month)]

    # Conditionally calculate the volume eligible, rebated and the actual amount
    df[['threshold_eligibility', 'threshold_rebate', 'volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate), axis=1)
    #print(df)
    return df

#Question - when it is monthly, is it applying on the fully volume when the threshold is met or on transactions above the threshold???
def apply_monthly_rebate(rebate, client, df):
    # Calculate and apply monthly rebate logic here
    print(f'==># Calculate and apply monthly rebate logic here: {rebate}')
    # Fill all empty values with 'N/A'
    #df = df.fillna('N/A')
    df = df.fillna(np.nan)
    # Conditionally calculate the volume eligible, rebated and the actual amount
    df[['threshold_eligibility', 'threshold_rebate', 'volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate), axis=1)
    return df

# Function to calculate rebate
def calculate_rebate(row, rebate):
    """
    row: reference the row of the df
    rebate: rebate parameters
    """
    #print(f'calculating rebate : {rebate}')
    print(rebate)
    threshold_eligibility = 0
    threshold_rebate = 0
    #calculating the maximum rebate by going through the rules
    for rule in rebate.get('rebate_rules'):
        if (row['volume']> rule.get('threshold')) and (threshold_eligibility <= rule.get('threshold')):
            threshold_eligibility = rule.get('threshold')
            threshold_rebate = rule.get('rebate_amount')
    

    print(f'Maximum rebate found: {threshold_rebate}')
    # if rebate impacts the full volume as soon as we hit the threshold
    if rebate.get('fullRebate'):
        volume_eligible = row['volume']
        volume_rebated = row['volume']
    else:
        # if rebate starts after the threshold is breached
        volume_eligible = row['ytd_volume'] - threshold_eligibility if rebate.get("type") == "yearly" else row['volume'] - threshold_eligibility
        volume_rebated = min(volume_eligible, row['volume'])
    
    # amount calculation
    volume_rebated_amount = volume_rebated * threshold_rebate

    return pd.Series([threshold_eligibility, threshold_rebate, volume_eligible, volume_rebated, volume_rebated_amount])

# Dictionary mapping rebate types to their corresponding functions - no default/fixed one as it is basically a monthly/yearly with 0 threshold
rebate_functions = {
    "monthly": apply_monthly_rebate,
    "yearly": apply_yearly_rebate,
}


def trigger_rebate_process(filtered_volumes: pd.DataFrame, clientId, rebate):
    print(f'Triggering rebate process for client : {clientId}')
    rebate_type = rebate.get('type')
    df_output = pd.DataFrame
    print(rebate_type)
    if rebate_type in rebate_functions:
        rebate_function = rebate_functions[rebate_type]
        df_output = rebate_function(rebate, clientId, filtered_volumes)
    else:
        print("Invalid rebate type")
    return df_output

# Mimick querying in the database
def get_volumes(year: int, end_month_ref:int):
    """
    get volumes for given year until give month for all clients/product types etc..
    """
    file = sys.path[0]+'/AI/utils/monthly_volumes.csv'
    df = pd.read_csv(file)
    #print(df)
    df_filtered = df[(df['year'] == year) & (df['month'] <= month)]
    return df_filtered

def get_volumes_not_matched_to_a_rebate(df1:pd.DataFrame, df2:pd.DataFrame):
    merged_df = df2.merge(df1, how='left', indicator=True)
    return merged_df[merged_df['_merge'] == 'left_only']

def get_rebate_summary_for_given_month(df: pd.DataFrame, month:int):
    df = df[(df['month'] == month)]
    #df = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].cumsum()
    return df

def get_rebate_summary_by_period(df: pd.DataFrame, groupby:list, sumby: str):
    df = df.groupby(groupby)[sumby].sum().unstack('period')
    df.columns = pd.to_datetime(df.columns)
    df = df.sort_index(axis=1,ascending=False)
        # Add a total row
    total_row = df.sum(axis=0)  # Sum along the rows (vertically)
    #df = pd.concat([df, total_row.to_frame().T], ignore_index=True)

    # Calculate the percentage change between consecutive periods
    df['evolution%_last2periods'] = ((df[df.columns[0]] - df[df.columns[1]]) / df[df.columns[1]]) * 100

    return df


def main():
    #get volume data
    df = get_volumes(year, month)
    # rebates output
    rebates = []
    discarded_filters = []
    # trigger rebate process for each client
    for client in clientRebatesList:
        client_filtered_df = df[(df['clientId'] == client.get('clientId'))]
        for rebate in client.get('rebates'):
            type = rebate.get('type')
            full_rebate = rebate.get('fullRebate')
            rebate_rules = rebate.get('rebate_rules')
            filters = rebate.get('filters')
            
            rebate_filtered_df = client_filtered_df
            rebate_filtered_df['type'] = type
            #print(f'{type} - {full_rebate} - {rebate_rules} - {filters}')

            for filter in filters:
                for key, value in filter.items():

                    #checking that the filter is not empty = []
                    if value:
                        # Check if the key exists in the DataFrame's columns
                        print(value)
                        if key in rebate_filtered_df.columns:
                            rebate_filtered_df = rebate_filtered_df[rebate_filtered_df[key].isin(value)]
                            # Getting the values that were not found
                            not_found_values = [{key,value} for value in value if value not in rebate_filtered_df[key].tolist()]
                            #not_found_pairs = [(key, value) for key, value in filter.items() if key not in rebate_filtered_df[key].tolist()]

                            if not_found_values: discarded_filters.append(not_found_values)
                        else:
                            # Handle the case when the key doesn't exist
                            print(f"The key '{key}' does not exist in the DataFrame.")
                    else:
                        pass
            
            if not rebate_filtered_df.empty:
                #print('Data was found')
                rebate_filtered_df['eligible'] = True
                #print(filtered_df)
                rebates.append(trigger_rebate_process(rebate_filtered_df,client.get('clientId'),rebate))
            else:
                print('No data following the application of the filters')
                discarded_filters.append(filters)
            #print(filtered_df)

    all_rebates = pd.concat(rebates, ignore_index=True)
    print("*****Rebates******")
    print(all_rebates)
    print("*****Not Eligible******")
    volumes_not_eligible = get_volumes_not_matched_to_a_rebate(all_rebates[df.columns],df)
    print(volumes_not_eligible)
    print("*****Discarded filters*****")
    print(discarded_filters)
    print("*****Given month*****")
    print(get_rebate_summary_for_given_month(all_rebates, month))
    print("*****summary custom*****")
    print(get_rebate_summary_by_period(all_rebates,['clientId','period'],'volume_rebated_amount'))
    print("*****summary custom*****")
    print(get_rebate_summary_by_period(all_rebates,['clientId','period','product_name'],'volume_rebated_amount'))


if __name__ == "__main__":
    main()