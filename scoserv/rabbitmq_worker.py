#!venv/bin/python

"""SCO Workflow Engine Server - Implementation of the workflow server as a
RabbitMQ worker.
"""
import json
import pika

from workflow import sco_run
from engine.rabbitmq import HOST, QUEUE

def callback(ch, method, properties, body):
    """Callback handler for client requests. Reads the request and runs the
    predictive model.
    """
    try:
        request = json.loads(body)
    except Exception as ex:
        ch.basic_ack(delivery_tag = method.delivery_tag)
    print '[x] Start model run ' + str(request)
    sco_run(request)
    print '[x] Done'
    ch.basic_ack(delivery_tag = method.delivery_tag)


if __name__ == '__main__':
    con = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = con.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    # Fair dispatch. Never give a worker more than one message
    channel.basic_qos(prefetch_count=1)
    # Set callback handler to read requests and run the predictive model
    channel.basic_consume(callback, queue=QUEUE)
    # Done. Start by waiting for requests
    print '[*] Waiting for requests. To exit press CTRL+C'
    channel.start_consuming()
