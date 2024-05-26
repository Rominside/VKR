# produce_message.py
import time

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


i = 0
list_ = list(range(30))
print(list_)
while True:
    producer.send('temperature', value=list_[i])
    producer.flush()
    time.sleep(1)
    i += 1
    if i == 30:
        break
