import requests
import time


time.sleep(5)

# Record the start time
start_time = time.time()
# Replace the URL with the actual URL of your FastAPI endpoint
api_url = "http://localhost:8000/api/v1/transactions/1d5582a7-4a37-4ca6-a37a-388e8689c33e/eyJuYW1lIjoiUm9tYW4ifQ%3D%3D"

# Replace the payload with the data you want to send in the POST request
payload = {
    "key1": "value1",
    "key2": "value2",
}

# Make the POST request
# Number of times to send the POST request
num_requests = 100

for _ in range(num_requests):
    response = requests.post(api_url, json=payload)
    #time.sleep(0.05)


    # Check the response
    """if response.status_code == 200:
        print("POST request successful")
        print("Response JSON:", response.json())
    else:
        print("POST request failed with status code:", response.status_code)
        print("Response text:", response.text)"""

    # Record the end time
end_time = time.time()

# Calculate the processing time
processing_time = end_time - start_time

print(f"Processing time: {processing_time} seconds")

