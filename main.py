from kafka import KafkaProducer, KafkaConsumer
from flask import Flask, request, jsonify, render_template
import plotly.graph_objs as go
import json, time, threading, random
import plotly.express as px
import pandas as pd
from jinja2 import Template


class KafkaHandler:
    def __init__(self, bootstrap_servers, topics, api_version):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.api_version = api_version
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers, api_version=api_version,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumers = [KafkaConsumer(topic, bootstrap_servers=self.bootstrap_servers,
                                        api_version=api_version, auto_offset_reset='earliest',
                                        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
                          for topic in self.topics]

    def produce_message(self, topic, data):
        self.producer.send(topic, value=data)
        self.producer.flush()

    def consume_message(self, topic):
        for message in self.consumers[self.topics.index(topic)]:
            yield message.value


app = Flask(__name__)
kafka_handler = KafkaHandler(['127.0.0.1:9092', '127.0.0.1:9092', '127.0.0.1:9092'],
                             ['topic1', 'topic2', 'topic3'], (0, 11, 5))


@app.route('/send', methods=['POST'])
def send_message():
    data = request.get_json()
    topic = data.get('topic')
    message = data.get('message')
    kafka_handler.produce_message(topic, message)
    return jsonify({'status': 'Message sent successfully'})


@app.route('/receive/<topic>', methods=['GET'])
def receive_message(topic):
    return jsonify(list(kafka_handler.consume_message(topic)))


@app.route('/graphs', methods=['GET', 'POST'])
def show_graphs():
    # graph1 = json.dumps([{
    #     'x': x,
    #     'y': y1,
    #     'type': 'scatter',
    #     'mode': 'lines+markers',
    #     'line': dict(color='blue'),
    #     'name': 'Graph 1'
    # }])
    # graph2 = json.dumps([{
    #     'x': x,
    #     'y': y2,
    #     'type': 'scatter',
    #     'mode': 'lines+markers',
    #     'line': dict(color='green'),
    #     'name': 'Graph 2'
    # }])
    # graph3 = json.dumps([{
    #     'x': y3,
    #     'y': 90,
    #     'type': 'scatter',
    #     'mode': 'lines+markers',
    #     'line': dict(color='red'),
    #     'name': 'Graph 3'
    # }])
    df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')

    fig = px.line(df, x='Date', y='AAPL.High', title='Time Series with Range Slider and Selectors')

    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ])
        )
    )

    # return render_template('graphs.html', graph1=graph1, graph2=graph2, graph3=graph3, x=x, y1=y1, y2=y2, y3=y3)
    return render_template('index.html', fig=Template.render({"fig":fig.to_html(full_html=False)}))


def generate_data():
    x = list(range(10))
    y1 = [random.randint(1, 10) for _ in range(10)]
    y2 = [random.randint(1, 10) for _ in range(10)]
    y3 = [random.randint(1, 10) for _ in range(10)]
    return x, y1, y2, y3


def update_graphs(interval=1):
    global x, y1, y2, y3
    while True:
        x, y1, y2, y3 = generate_data()
        time.sleep(interval)


# Запуск потока для обновления графиков
update_thread = threading.Thread(target=update_graphs)
update_thread.daemon = True
update_thread.start()


if __name__ == '__main__':
    app.run(debug=True)
