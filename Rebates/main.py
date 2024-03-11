
import pandas as pd
import numpy as np
import sys
import warnings
import pandas.core.common

#global vars
year = 2024
month = 2

# Suppress pandas warnings
# Suppress SettingWithCopyWarning
#warnings.simplefilter(action='ignore', category=pandas.core.common.SettingWithCopyWarning)


# should there be controls in case some of the rebates are applied twice?
# you  should not have more than one monthly rebate or yearly rebate
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
        {"product_name":[]}
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

conte =  {
  "clientId": "CONTE",
  "rebates": [
    {
      "type": "yearly",
      "fullRebate":True,
      "rebate_rules": [
          {"threshold": 300000, "rebate_amount": .1},
          {"threshold": 700000, "rebate_amount": .2},
          {"threshold": 1050000, "rebate_amount": .3},
          {"threshold": 1400000, "rebate_amount": .5},
          {"threshold": 1200000, "rebate_amount": .4}
      ],
      "filters": [
        {"exchanges": []},
        {"salesman_code": []},
        {"accounts": []},
        {"product_name":['OSEMINI']}
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

clientRebatesList = [conte]#, rebate_fixed]


def apply_yearly_rebate(rebate, client, df):
    """
    Applies yearly rebate: only difference is the ytd_volume column that is created
    """
    # Calculate and apply yearly rebate logic here
    #print(f'==># Calculate and apply yearly rebate logic here {rebate}')

    # Calculate the YTD volume for each combination of clientId, exchange, salesman_code, and account
    #df['ytd_volume'] = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].transform('sum')

    # Sort DataFrame by clientId and period
    df.sort_values(by=['clientId','exchange','account','salesman_code','product_name','period'],inplace=True)

    # Group by clientId and calculate YTD
    df['ytd_volume'] = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].cumsum()

    # Conditionally calculate the volume eligible, rebated and the actual amount
    df[['threshold_eligibility', 'threshold_rebate', 'volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate), axis=1)
    #print(df)
    return df

#Question - when it is monthly, is it applying on the fully volume when the threshold is met or on transactions above the threshold???
def apply_monthly_rebate(rebate, client, df):
    """
    Applies monthly rebate
    """
    # Calculate and apply monthly rebate logic here
    #print(f'==># Calculate and apply monthly rebate logic here: {rebate}')
    # Conditionally calculate the volume eligible, rebated and the actual amount
    df[['threshold_eligibility', 'threshold_rebate', 'volume_eligible','volume_rebated', 'volume_rebated_amount']] = df.apply(lambda row: calculate_rebate(row, rebate), axis=1)
    return df

# Dictionary mapping rebate types to their corresponding functions - no default/fixed one as it is basically a monthly/yearly with 0 threshold
rebate_functions = {
    "monthly": apply_monthly_rebate,
    "yearly": apply_yearly_rebate,
}

# Function to calculate rebate
def calculate_rebate(row, rebate):
    """
    row: reference the row of the df
    rebate: rebate parameters
    """
    #print(f'calculating rebate : {rebate}')
    
    # if yearly rebate, use ytd_volume instead of the row's volume for computation
    row_volume = row['ytd_volume'] if rebate.get("type") == "yearly" else row['volume']

    # vars
    total_rebate = 0
    total_volume = 0
    threshold_eligibility = 0
    threshold_rebate = 0

    #sort the rules by highest threshold    
    sorted_rules = sorted(rebate.get('rebate_rules'), key=lambda x: x['threshold'], reverse=True)

    # if rebate impacts the full volume as soon as we hit the threshold
    if rebate.get('fullRebate'): 
        # we need to find the highest rate breached (in case there are multiple layers)
        for rule in sorted_rules:
            if (row_volume> rule.get('threshold')):
                threshold_rebate = rule.get('rebate_amount')
                threshold_eligibility = rule.get('threshold')
                break
        # since it is a full rebate, full volume is eligible
        volume_eligible = row_volume
        # and therefore full volume rebated
        volume_rebated = row_volume
        # the amount is the total volume * the highest threshold breached
        volume_rebated_amount = volume_rebated * threshold_rebate
    else:
        # if not full rebate, it means that the rebate only applies to the portion above the threshold.
        # in case of multiple layers of rebate, we need to calculate each portion of the volume above threshold * rebate amount
        for rule in sorted_rules:
            if (row_volume > rule.get('threshold')):
                
                total_rebate += (row_volume - rule['threshold']) * rule['rebate_amount']
                total_volume += (row_volume- rule['threshold'])
                row_volume = rule.get('threshold')
            
        threshold_eligibility = rule.get('threshold') 
        volume_eligible = total_volume#row['ytd_volume'] - threshold_eligibility if rebate.get("type") == "yearly" else total_volume#row['volume'] - threshold_eligibility
        volume_rebated = total_volume#min(volume_eligible, row['volume'])
        volume_rebated_amount = total_rebate
        threshold_rebate = volume_rebated_amount/volume_rebated

    return pd.Series([threshold_eligibility, threshold_rebate, volume_eligible, volume_rebated, volume_rebated_amount])


def trigger_rebate_process(filtered_volumes: pd.DataFrame, clientId, rebate):
    """
    Trigger rebate process depending on the rebate type yearly or monthly
    """
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
    """
    returns all the volume entries not matched to rebates
    """
    merged_df = df2.merge(df1, how='left', indicator=True)
    return merged_df[merged_df['_merge'] == 'left_only']

def get_rebate_summary_for_given_month(df: pd.DataFrame, month:int):
    """
    Returns the dataframe filtered on a specific month
    """
    df = df[(df['month'] == month)]
    #df = df.groupby(['clientId','exchange','account','salesman_code','product_name','year'])['volume'].cumsum()
    return df

def get_rebate_summary_by_period(df: pd.DataFrame, groupby:list, sumby: str):
    """
    Custom summary that calculates MoM changes
    """
    df = df.groupby(groupby)[sumby].sum().unstack('period')
    df.columns = pd.to_datetime(df.columns)
    df = df.sort_index(axis=1,ascending=False)
        # Add a total row
    total_row = df.sum(axis=0)  # Sum along the rows (vertically)
    #df = pd.concat([df, total_row.to_frame().T], ignore_index=True)

    # Calculate the percentage change between consecutive periods
    df['evolution%_last2periods'] = ((df[df.columns[0]] - df[df.columns[1]]) / df[df.columns[1]]) * 100

    return df

# Dictionary mapping of reporting functions - not used
reporting_functions = {
    "volumes_unmatched": get_volumes_not_matched_to_a_rebate,
    "rebate_month": get_rebate_summary_for_given_month,
    "rebate_summary": get_rebate_summary_by_period,
}

def main():
    """
    Main function: get volume data and matches it against the client rebates array
    """
    #get volume data
    df = get_volumes(year, month)

    # Fill NaN values with 'Unknown'
    df = df.fillna('N/A')

    # check if volumes retrieved
    if not df.empty:
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

        if not len(rebates) == 0:
            #print(rebates)
            all_rebates = pd.concat(rebates, ignore_index=True)
            print("*****Rebates******")
            print(all_rebates)
            print("*****Not Eligible******")
            volumes_not_eligible = get_volumes_not_matched_to_a_rebate(all_rebates[df.columns],df)
            #print(volumes_not_eligible)
            print("*****Discarded filters*****")
            #print(discarded_filters)
            print("*****Given month*****")
            print(get_rebate_summary_for_given_month(all_rebates, month))
            print("*****summary custom*****")
            #print(get_rebate_summary_by_period(all_rebates,['clientId','period'],'volume_rebated_amount'))
            print("*****summary custom*****")
            #print(get_rebate_summary_by_period(all_rebates,['clientId','period','product_name'],'volume_rebated_amount'))
        else:
            print(f'No rebates applied to the volume dataset')
    else:
        print(f'No data found for year {year} going to month {month}')


if __name__ == "__main__":
    main()
   