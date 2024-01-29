import pandas as pd
from utils.typing import *


columns_selection = ['Survived']
filtering_rules = [('Survived',0)]
recipients = ['x']
recipients_matrix = [{'email':'x@x.com','col1:':'value','col2':'value2'},{'email':'y@y.com','col1:':'value','col2':'value2'}]

def generate_statistics(df: pd.DataFrame):
    """
    describe function from pandas
    """
    return df.describe()

def apply_filtering(df:pd.DataFrame, filtering_rules: list):
    """
    #1 Applies first layer of filtering
    """
    df = df
    for key, value in filtering_rules:
        print(f"filtering data on Key: {key} and value: {value}")
        df = df[df[key] == value]
    
    return df

def apply_column_selection(columns: list):
    """
    Filters the columns that needs to be kept
    if empty - no filtering
    """

def generate_emails(recipients: list, recipients_matrix: list):
    """
    generate email notifications: 
    """

def main():
    data = pd.read_csv("./utils/fake_data.csv")
    data_filtered = apply_filtering(data,filtering_rules)

    stats = generate_statistics(data)
    print(stats)

    stats2 = generate_statistics(data_filtered)
    print(stats2)


if __name__ == "__main__":
    main()