import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime

random.seed(100)

class AWSDBConnector:

    def __init__(self, creds_file="db_creds.yaml"):
        
        with open(creds_file, "r") as file:
            creds = yaml.safe_load(file)
            self.HOST = creds["database"]["host"]
            self.USER = creds["database"]["user"]
            self.PASSWORD = creds["database"]["password"]

        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

API_URL = "https://jdneb8yyrb.execute-api.us-east-1.amazonaws.com/prod"
user_topic_endpoint = f"{API_URL}/12e8a20c3827.user"
pin_topic_endpoint = f"{API_URL}/12e8a20c3827.pin"
geo_topic_endpoint = f"{API_URL}/12e8a20c3827.geo"

# Custom function to handle datetime serialization
def custom_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to string
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")

# Function to send data to the API
def send_data_to_api(endpoint, data):
    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
    try:
        json_data = json.dumps(data, default=custom_json_serializer)  # Use the custom serializer
        response = requests.post(endpoint, data=json_data, headers=headers)
        if response.status_code == 200:
            print(f"Data successfully sent to {endpoint}")
        else:
            print(f"Failed to send data {endpoint}. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error sending data to {endpoint}: {e}")

# Function to run the infinite loop and send data to API
def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))  # Sleep for a random interval
        random_row = random.randint(0, 11000)  # Get a random row index
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Fetch a random pin data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                print(f"Pin result: {pin_result}")
                # Send the pin data to the pin topic
                send_data_to_api(pin_topic_endpoint, pin_result)

            # Fetch a random geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                print(f"Geo result: {geo_result}")
                # Send the geo data to the geo topic
                send_data_to_api(geo_topic_endpoint, geo_result)

            # Fetch a random user data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                print(f"User result: {user_result}")
                # Send the user data to the user topic
                send_data_to_api(user_topic_endpoint, user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
