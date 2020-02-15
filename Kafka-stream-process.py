from abc import ABC, abstractmethod
from kafka import KafkaConsumer, KafkaProducer
import json
import os
import LEWSJsonUtil as util
from pprint import pprint
import csv
import re


#--------------- Template Code, Avoid changing anything in this section --------------------------# 
class AbstractKafkaInStreamProcessor(ABC):
        
    def produce_data_kafka(self,record) -> None:
      
      self.producer.send(topic=self.target_topic,value=record)
      #print("Processed Record Sent")



    @abstractmethod
    def process_data(self,record) -> None:
        
        return record



    def kafka_in_stream_processor(self) -> None:

        for message in self.consumer:
            
            try:
                self.processed_record = self.process_data(message)
                self.produce_data_kafka(self.processed_record)
            except:
                print("Skipping Record..")

        
            




    def __init__(self,processor_name,source_topic,target_topic):

        self.processor_name = processor_name
        
        self.source_topic = source_topic
        
        self.target_topic = target_topic
        
        self.bootstrap_servers = os.getenv('KAFKA_BROKER','host.docker.internal:9092')
        #self.bootstrap_servers = 'localhost:9092'
        
        print("Initializing Kafka In-Stream Processor Module")
        
        self.consumer = KafkaConsumer(source_topic,group_id = self.processor_name, bootstrap_servers = self.bootstrap_servers,value_deserializer=lambda m: json.loads(m.decode('utf-8')))

       # self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers, value_serializer = lambda v: json.dumps(v).encode('utf-8'))




def run(abstract_class: AbstractKafkaInStreamProcessor) -> None:
    """
    The client code calls the template method to execute the algorithm. Client
    code does not have to know the concrete class of an object it works with, as
    long as it works with objects through the interface of their base class.
    """

    # ...
    abstract_class.kafka_in_stream_processor()
    # ...


#-------------------------Template Code Ends Here ------------------------------------#


class ConKafkaInStreamProcessor(AbstractKafkaInStreamProcessor):

     def process_data(self,message) -> None:
#------------------- Add module Logic in this section ---------------------#
        try:
            list1 = []
            list2 = []
            list3 = []
            list4 = []
            with open('User classification data.csv','r')as f:
                reader = csv.reader(f, delimiter=",")
                for i, line in enumerate(reader):
                    if(i==1):
                        list1 = line
                    elif(i==2):
                        list2 = line
                    elif(i==3):
                        list3 = line
                    else:
                        list4 = line


            # To get value from a field (Example)
            util.json_util = util.JsonDataUtil(message.value)
            

            tweet_text = util.json_util.get_value("text")
            get_user = util.json_util.get_value("user")
            data = get_user["screen_name"]
            #Do Processing
            if(data in list1):
                util.json_util.add_metadata("User classification","news_agencies")
            elif(data in list2):
                util.json_util.add_metadata("User classification","govt_agencies")
            elif(data in list3):
                util.json_util.add_metadata("User classification","ngo")
            else:
                util.json_util.add_metadata("User classification","other")

        except:
            print("Invalid Tweet Record.. Skipping")
            raise

        #Get the processed record with metadata added
        processes_message = util.json_util.get_json() 
#---------------------- Add module logic in this section (End) ----------------------#
        return processes_message





if __name__ == "__main__":

    #processor_name: Unique processor name for the module, 
    #source_topic: Topic from which the module should accept the record to be processed, 
    # target_topic: Topic to which the module publishes the processed record
   s_topic = os.getenv('MODULE_SRC_TOPIC','lews-twitter')
   t_topic = os.getenv('MODULE_TGT_TOPIC','t_topic')
   proc_name = os.getenv('MODULE_NAME','Module01')  
   
   run(ConKafkaInStreamProcessor(processor_name=proc_name, source_topic=s_topic, target_topic=t_topic))
