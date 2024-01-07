import json
import random
from time import sleep

import requests
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)

class AWSDBConnector:

    class NotYAMLFileError(Exception):
        "definining a custom exception for when the file isnt a YAML file."
        pass

    def read_creds(self, file):
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
        credentials = self.read_creds('source_creds.yaml')
        
        # setting them as variables to be used in the sqlalchemy engine.
        HOST = credentials["HOST"]
        USER = credentials["USER"]
        PASSWORD = credentials["PASSWORD"]
        DATABASE = credentials["DATABASE"]
        PORT = credentials["PORT"]
    
        #connecting to the source database.    
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine

def extract_data(data_set, random_row, connection):
    '''This function simply extracts data from the RDS database

    Args:
        data_set (`string`): The name of the table we wish to extract from.
        random_row (`integer`): The integer of the row one wishes to extract the data from.
        connection (`sqlalchemy.engine.base.Connection`): The connection used to connect to the RDS database.

    Returns:
        data_result (`dictionary`): Returns a dictionary to be used in an API payload.

    Raises:
        ProgrammingError: when there is an invalid RDS table name.

    
    '''
    try:
        data_string = text(f"SELECT * FROM {data_set} LIMIT {random_row}, 1")

        selected_row = connection.execute(data_string)

        for row in selected_row:

            data_result = dict(row._mapping)

            if data_set == 'pinterest_data':
                data_result = { 
                                'index': data_result['index'], 'unique_id': data_result['unique_id'], 'title': data_result['title'], 'description': data_result['description'], 'poster_name': data_result['poster_name'], 'follower_count': data_result['follower_count'], 'tag_list': data_result['tag_list'], 'is_image_or_video': data_result['is_image_or_video'], 'image_src': data_result['image_src'], 'downloaded': data_result['downloaded'], 'save_location': data_result['save_location'], 'category': data_result['category']
                                }
            elif data_set == 'geolocation_data':
                data_result = {
                                'ind': data_result['ind'], 'timestamp': data_result['timestamp'].isoformat(), 'latitude': data_result['latitude'], 'longitude': data_result['longitude'], 'country': data_result['country']
                                }
            else:
                data_result = {                        
                                'ind': data_result['ind'], 'first_name': data_result['first_name'], 'last_name': data_result['last_name'], 'age': data_result['age'], 'date_joined': data_result['date_joined'].isoformat()
                                }
                
    except  sqlalchemy.exc.ProgrammingError as e:
        print(f'Error: {e}')
        print(f'{data_set} is most likely an invalid RDS table name, please correct this and try again')
        exit()
    
    return data_result

def stream_data(Invoke_URL, stream, payload):
    ''' This function is used to send data to the appropriate stream.

    Args:
        Invoke_URL (`string`): The Invoke URL of thr API the user wishes to send the data to. 
        stream (`string`): This is the name of the stream we wish to send the data too.
        payload (`json serialised dictionary`): The desired data to be sent.

    Here, we simply take in the stream name and the data payload which is sent to the API. 
    If successful, a message is printed indicating the user of the success.

    Raises:
        HTTPError: If there is any error sending the data to the API.
    
    '''

    invoke_url = f'{Invoke_URL}/streams/{stream}/record'
    headers = {'Content-Type': 'application/json'}
    response = requests.request("PUT", invoke_url, headers=headers, data=payload) 

    try:
        response.raise_for_status()
        print(f"Data sent to the {stream} stream successfully!")

    except requests.exceptions.HTTPError as e:
        print(f"An error has occurred: {e}")
        exit()

def batch_data(Invoke_URL, topic, payload):
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

def run_infinite_post_data_loop(processing_type, new_connector):
    ''' A function which extracts and sends data to the appropriate source, emulating real-time data.

    Args:
        processing_type (`string`): The processing type the user wishes to use, either batch or streaming.
        new_connector (`AWSDBConnector`): An instance of the AWSDBConnector class used for database connections.

    Firstly, we read the stream and topic names from the appropriate file. Next, we connect to the RDS database to extract each corresponding row from the different tables. Then, the data is sent to the specified source, either the streaming or batch source.

    '''

    # extracting the Invoke URL, topic names and stream names.
    # here we are using the read_creds method from the AWSDBConnector class.
    topics_streams = new_connector.read_creds('topics_and_streams.yaml')

    # setting variables for the invoke URL, topic and stream names so they can be used later on.
    Invoke_URL = topics_streams['Invoke_URL']
    pin_topic = topics_streams['pin_topic']
    geo_topic = topics_streams['geo_topic']
    user_topic = topics_streams['user_topic']
    pin_stream = topics_streams['pin_stream']
    geo_stream = topics_streams['geo_stream']
    user_stream = topics_streams['user_stream']

    while True:
        #adds a random delay
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        # connecting to the source database.
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_result = extract_data('pinterest_data', random_row, connection)
            geo_result = extract_data('geolocation_data', random_row, connection)
            user_result = extract_data('user_data', random_row, connection)
            
            if processing_type == 'streaming':

                # adding the 3 stream names and corresponding payloads inside of a dictionary.
                combined_data = {
                    pin_stream : json.dumps({
                        'StreamName': pin_stream,
                        'Data': pin_result,
                        'PartitionKey': 'pin_data'
                    }),
                    geo_stream: json.dumps({
                        'StreamName': geo_stream,
                        'Data': geo_result,
                        'PartitionKey': 'geo_data'
                    }),
                    user_stream: json.dumps({
                        'StreamName': user_stream,
                        'Data': user_result,
                        'PartitionKey': 'user_data'
                    })
                }

                # now sending the streaming data to the API
                for stream, payload in combined_data.items():
                    stream_data(Invoke_URL, stream, payload) 

            else:
                # adding the 3 topic names and corresponding payloads inside of a dictionary.
                combined_data = {
                    pin_topic: json.dumps({
                        'records': [
                            {
                                'value': pin_result
                            }
                        ]
                    }),
                    geo_topic: json.dumps({
                        'records': [
                            {
                                'value': geo_result
                            }
                        ]
                    }),
                    user_topic: json.dumps({
                        'records': [
                            {
                                'value': user_result
                            }
                        ]
                    })
                }

                # now sending the batch data to the API
                for topic, payload in combined_data.items():
                    batch_data(Invoke_URL, topic, payload)                

if __name__ == "__main__":

    user_input = input('streaming or batch? ').lower()
    
    # validate user input
    if user_input not in ['streaming', 'batch']:
        print('Invalid input. Please enter either "streaming" or "batch".')
    else:
        connector = AWSDBConnector()
        run_infinite_post_data_loop(user_input, connector)