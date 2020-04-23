from kafka import KafkaProducer
import json
import time


class ProducerServer:

    def __init__(self, input_file, topic, bootstrap_servers, **kwargs):
        super().__init__()
        self.input_file = input_file
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=[bootstrap_servers])

    # done TODO we're generating a dummy data
    def generate_data(self):
        print(self.topic)
        with open(self.input_file) as f:
            data = json.load(f)
            for line in data:
                
                message = self.dict_to_binary(line)
                #print(message)
                
                # done TODO send the correct data
                self.producer.send(self.topic, message)
                time.sleep(1)

    # done TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        data = json.dumps(json_dict)
        encoded_data = data.encode('utf-8')
        return encoded_data 
        