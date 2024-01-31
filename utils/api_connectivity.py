import requests

def get_access_token(client_id, client_secret, token_url):
    # Form the request payload
    data = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
    }

    # Make a POST request to get the access token
    response = requests.post(token_url, data=data)

    if response.status_code == 200:
        # Parse the JSON response
        json_response = response.json()
        access_token = json_response.get('access_token')
        return access_token
    else:
        print(f"Failed to get access token. Status code: {response.status_code}")
        return None

def make_authorized_request(api_url, access_token):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed API request. Status code: {response.status_code}")
        return None
    
__all__ = ['make_authorized_request','get_access_token']
