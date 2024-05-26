import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json

# Создаем Kafka consumer
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')

# Указываем топик и партицию, для которой мы хотим прочитать последнее сообщение
topic = 'temperature'
partition = 0  # Ваша партиция

# Получаем количество сообщений в партиции
def test():
    consumer.topics()
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    last_offset = consumer.position(tp)

    # Читаем последнее сообщение
    consumer.seek(tp, last_offset - 1)
    msg = next(consumer)
    print(msg[2], msg[6])


while True:
    test()
    time.sleep(1)
