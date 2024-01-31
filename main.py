import pandas as pd
import numpy as np
import os
import json
from utils.typing import *
from utils.api_connectivity import *
from jinja2 import Template

#api parameters (optional)
client_id = 'your_client_id'
client_secret = 'your_client_secret'
token_url = 'https://example.com/token'  
api_url = 'https://example.com/token'  

#data manipulation parameters (optional)
filtering_rules = [('Cabin','A6')]
columns_selection = ['Name','Survived','Sex','Cabin']
data_enrichment_json_string = '[{"PassengerId":1,"favorite_color":"red"}]'

#emmail parameters
recipients = ['x']
recipients_matrix = '[{"email":"roman.medioni","filtering_rules":[["Survived", 1],["Sex","male"],["Cabin","A6"]]}]'
from_addr = ""
email_title = ""

def get_data(file_path: str, file_type: str):
    """
    Get data from different file formats (csv, text, excel).

    Parameters:
    - file_path (str): The path to the file.
    - file_type (str): The type of the file (csv, text, excel).

    Returns:
    - pd.DataFrame: The DataFrame containing the data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_type == 'api':
        #api connectivity
        access_token = get_access_token(client_id, client_secret, token_url)
        if access_token:
            # call api
            make_authorized_request(api_url, access_token)
            print("")
        return ""
    if file_type == 'csv':
        return pd.read_csv(file_path)
    elif file_type == 'text':
        # Assuming tab-separated values (you can adjust the delimiter)
        return pd.read_csv(file_path, delimiter='\t')
    elif file_type == 'excel':
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
def generate_statistics(df: pd.DataFrame):
    """
    describe function from pandas
    """
    return df.describe()

def apply_row_filtering(df:pd.DataFrame, filtering_rules: list):
    """
    #1 Applies first layer of filtering
    """
    df = df
    for key, value in filtering_rules:
        print(f"filtering data on Key: {key} and value: {value}")
        # Check if the key exists in the DataFrame
        if key in df.columns:
            # Check if the value exists in the corresponding column
            if df[key].isin([value]).any():
                df = df[df[key] == value]
            else:
                print(f"Value {value} does not exist in column {key}")
        else:
            print(f"Column {key} does not exist in the DataFrame")    
    return df

def apply_column_selection(df:pd.DataFrame,columns: list):
    """
    Filters the columns that needs to be kept
    if empty - no filtering
    """
    #columns are empty - do not filter
    if columns:
        columns_found = [column for column in columns if column in df.columns]
        print(f"filtering on : Columns {columns_found} do not exist in the DataFrame")
        columns_missing = [column for column in columns if column not in df.columns]
        print(f"Error: Columns {columns_missing} do not exist in the DataFrame so we removed those from your input")
        df = df[columns_found]
    return df

def apply_enrichment(df:pd.DataFrame, data_enrichment_json_string:str):
    """
    Enrich with additionnal dataset
    """
    # Convert JSON string to a Python dictionary
    data_enrichment_dict = json.loads(data_enrichment_json_string)
    # Create a DataFrame from the dictionary
    df_enrichment = pd.DataFrame(data_enrichment_dict, index=[0])
    
    try:
        if ('PassengerId' in df.columns) and ('PassengerId' in df_enrichment.columns):
            df = df.merge(df_enrichment, on='PassengerId', how='outer')
        else: print("not in columns")
    except pd.errors.MergeError as e:
        print(f"Error during merge: {e}")
    return df

def generate_template(df: pd.DataFrame):
    # Jinja2 template
    template_str = """
    <html>
    <head>
        <title>{{ title }}</title>
    </head>
    <body>
        <h1>Hello, {{ name }}!</h1>
        <p>{{ message }}</p>

        <table border="1">
            <thead>
                <tr>
                    {% for column in columns %}
                        <th>{{ column }}</th>
                    {% endfor %}
                </tr>
            </thead>
            <tbody>
                {% for row in table_data %}
                    <tr>
                        {% for column in columns %}
                            <td>{{ row[column] }}</td>
                        {% endfor %}
                    </tr>
                {% endfor %}
            </tbody>
        </table>
    </body>
    </html>
    """

    template = Template(template_str)
    
    # Example data
    data = {
        'title': 'My Email',
        'name': 'John',
        'message': 'Welcome to the community!',
        'columns': df.columns.tolist(),
        'table_data': df.to_dict(orient='records'),
    }

    rendered_html = template.render(**data)
    return rendered_html

def generate_emails(recipients: list, recipients_matrix: list, df:pd.DataFrame):
    """
    generate email notifications:
    #1 all recipients receive the full dataframe
    #2 all matrix recipents receive a dataframe that goes through an additionnal layer of filtering
    """
    for recipient in recipients:
        send_email(recipient,df)
    
    # Convert JSON string to a Python dictionary
    recipients_matrix = json.loads(recipients_matrix)
    for recipient in recipients_matrix:
            filtering_tuples = [tuple(inner_list) for inner_list in recipient['filtering_rules']]
            print(filtering_tuples)
            df = apply_row_filtering(df,filtering_tuples)
            send_email(recipient['email'], df)


def send_email(recipient:str, df:pd.DataFrame):
    print(f'sending email to {recipient}')
    print(df)

def main():
    data = get_data('./utils/fake_data.csv','csv') 
    
    #apply pre-filter on rows (optional)
    data_filtered = apply_row_filtering(data,filtering_rules)
    #apply selection on columns (optional)
    data_filtered = apply_column_selection(data_filtered, columns_selection)
    #apply enrichment (optional)
    data_filtered = apply_enrichment(data_filtered,data_enrichment_json_string)
    #print(data_filtered)
    generate_emails(recipients,recipients_matrix,data_filtered)

    stats2 = generate_statistics(data_filtered)
    print(stats2)

    test_html = generate_template(data_filtered)
    print(test_html)


if __name__ == "__main__":
    main()