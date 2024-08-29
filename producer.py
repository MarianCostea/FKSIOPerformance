from confluent_kafka import Producer
import socket

conf = {
    'bootstrap.servers': '127.0.0.1:19092',
    'client.id': socket.gethostname(),
    'batch.num.messages': 1000,  # Number of messages to batch together
    'linger.ms': 5,  # Time to wait before sending a batch
    'compression.type': 'snappy'  # Compression type for better performance
}

producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

for i in range(10000):
    producer.produce('my_topic', key=str(i), value='my_value_{}'.format(i), callback=delivery_report)
    producer.poll(0)

producer.flush()
