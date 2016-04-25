__author__ = 'user'

from kafka import KafkaClient
#from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from datetime import datetime

#kafka =  KafkaClient("localhost:9092")
#kafka =  KafkaClient()
kafka = KafkaClient('localhost:9092')

producer = SimpleProducer(kafka)
for num in range(10000):
	producer.send_messages("v-topic", "This is message sent from python client " + str(datetime.now().time()) )
