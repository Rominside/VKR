from flask import Flask, render_template, jsonify
import random
from kafka import KafkaConsumer, TopicPartition
import json

app = Flask(__name__)


# Создаем Kafka consumer
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')

# Указываем топик и партицию, для которой мы хотим прочитать последнее сообщение
partition = 0  # Ваша партиция
position_last_temperature = 0
position_last_gas_concentration = 0


def generate_new_data():
    x = list(range(10))
    y1 = [random.randint(10, 15) for _ in range(1)]
    y2 = [random.randint(1, 10) for _ in range(1)]
    y3 = [random.randint(1, 2) for _ in range(1)]
    return x, y1, y2, y3


def generate_data():
    # Замените это на вашу логику генерации данных
    x = [0]
    y1 = [0]
    y2 = [0]
    y3 = [0]
    return x, y1, y2, y3


def start_consumers(topic):
    consumer.topics()
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    last_offset = consumer.position(tp)
    # Читаем последнее сообщение
    consumer.seek(tp, last_offset - 1)
    msg = next(consumer)
    value = 0
    global position_last_temperature
    global position_last_gas_concentration
    if topic == "gas_concentration":
        if msg[2] != position_last_gas_concentration:
            value = msg[6]
        position_last_gas_concentration = msg[2]
    else:
        if msg[2] != position_last_temperature:
            value = msg[6]
        position_last_temperature = msg[2]
    return value


@app.route('/get_new_data', methods=['GET'])
def get_new_data():
    x, y1, y2, y3 = generate_new_data()
    data = {'x': x, 'y1': y1, 'y2': y2, 'y3': y3}
    return jsonify(data)


@app.route('/graphs')
def show_graphs():
    global x, y1, y2, y3
    x = list(range(len(x) + 1))
    y1_local = start_consumers("gas_concentration")
    y2_local = start_consumers("temperature")
    print([int(y1_local)], [int(y2_local)])
    y1 = y1 + [int(y1_local)]
    y2 = y2 + [int(y2_local)]
    return render_template('graphs.html', x=x, y1=y1, y2=y2)


if __name__ == '__main__':
    x = []
    y1 = []
    y2 = []
    y3 = []
    app.run(debug=True)
