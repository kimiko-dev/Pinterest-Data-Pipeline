import json
import random
from time import sleep

import requests
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)

class AWSDBConnector:

    def __init__(self):

        pass


    class NotYAMLFileError(Exception):
        "definining a custom exception for when the file isnt a YAML file."
        pass

    def read_db_creds(self, file):
        ''' This method is used to read the credentials from the YAML file.

        Here, we first check if the filepath is correct. If it is, the file is read and loaded. If not, an error is thrown accordingly.
        
            Raises:
                NotYAMLFileError: If the file is not in YAML format.
                FileNotFoundError: If the specified file cannot be found.
                TypeError: If the `file` argument is not a string.
                ValueError: If there is an issue with loading the YAML file.
                yaml.YAMLError: If there is a YAML syntax error in the file.
                AttributeError: If there is an issue with the data type.

        Returns:
            cred_dict (`dict`): This is a dictionary which contains the credentials to be used to connect to the RDS database.

        '''

        try:
            if not file.endswith('.yaml') and not file.endswith('.yml'):
                raise self.NotYAMLFileError("File is not a YAML file.")

            with open(file, 'r') as creds_file:
                cred_dict = yaml.load(creds_file, Loader=yaml.FullLoader)
            return cred_dict

        except (FileNotFoundError, TypeError, self.NotYAMLFileError, ValueError, yaml.YAMLError, AttributeError) as e:
            print(f'Error: {e}, please check your file path, format, and content.')

    def create_db_connector(self):
        '''
        This function is used to create an engine which connects the the source data
        '''
        
        # here we are extracting the credentials from the credential file.
        credentials = self.read_db_creds('source_creds.yaml')
        
        # setting them as variables to be used in the sqlalchemy engine.
        HOST = credentials["HOST"]
        USER = credentials["USER"]
        PASSWORD = credentials["PASSWORD"]
        DATABASE = credentials["DATABASE"]
        PORT = credentials["PORT"]
    
        #connecting to the source database.    
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


# Function to send data to Kafka topic
def send_data(Invoke_URL, topic, payload):
    ''' This function is used to send data to the appropriate topic.

    Args:
        Invoke_URL (`string`): The Invoke URL of thr API the user wishes to send the data to. 
        topic (`string`): This is the name of the topic we wish to send the data too.
        payload (`json serialised dictionary`): The desired data to be sent.

    Here, we simply take in the topic name and the data payload which is sent to the API. 
    If successful, a message is printed indicating the user of the success.

        Raises:
            HTTPError: If there is any error sending the data to the API.
    
    '''

    invoke_url = f'{Invoke_URL}/topics/{topic}'
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload) 

    try:
        response.raise_for_status()
        print(f"Data sent to the {topic} topic successfully!")

    except requests.exceptions.HTTPError as e:
        print(f"An error has occurred: {e}")
        exit()


def run_infinite_post_data_loop():
    '''
    This function runs an indefinite loop which simulates a real-time stream of data. It also creates 3 json serialised dictionaries, which are then sent to the API.
    '''

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            

            # adding the 3 topic names and corresponding payloads inside of a dictionary.
            combined_data = {
                '<topic_name_for_pin>': json.dumps({
                    'records': [
                        {
                            'value': {'index': pin_result['index'], 'unique_id': pin_result['unique_id'], 'title': pin_result['title'], 'description': pin_result['description'], 'poster_name': pin_result['poster_name'], 'follower_count': pin_result['follower_count'], 'tag_list': pin_result['tag_list'], 'is_image_or_video': pin_result['is_image_or_video'], 'image_src': pin_result['image_src'], 'downloaded': pin_result['downloaded'], 'save_location': pin_result['save_location'], 'category': pin_result['category']}
                        }
                    ]
                }),
                '<topic_name_for_geo>': json.dumps({
                    'records': [
                        {
                            'value': {'ind': geo_result['ind'], 'timestamp': geo_result['timestamp'].isoformat(), 'latitude': geo_result['latitude'], 'longitude': geo_result['longitude'], 'country': geo_result['country']}
                        }
                    ]
                }),
                '<topic_name_for_user>': json.dumps({
                    'records': [
                        {
                            'value': {'ind': user_result['ind'], 'first_name': user_result['first_name'], 'last_name': user_result['last_name'], 'age': user_result['age'], 'date_joined': user_result['date_joined'].isoformat()}
                        }
                    ]
                })
            }

            # now sending the data to the API
            for topic, payload in combined_data.items():
                send_data(<Invoke_URL>, topic, payload) 

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')