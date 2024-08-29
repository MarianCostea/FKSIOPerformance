from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from aiokafka import AIOKafkaConsumer
import asyncio


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='threading')

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('my response', {'data': 'Connected'})
    socketio.start_background_task(start_kafka_listener)

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

async def kafka_listener():
    consumer = AIOKafkaConsumer(
        'my_topic',
        bootstrap_servers='127.0.0.1:19092',
        group_id='my_group'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            socketio.emit('my response', {'data': msg.value.decode('utf-8')})
    finally:
        await consumer.stop()

def start_kafka_listener():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(kafka_listener())

if __name__ == '__main__':
    socketio.run(app, debug=True)