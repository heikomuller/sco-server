"""SCO Workflow Engine Client using RabbitMQ. Communication between client and
server is done by sending messages via RabbitMQ queues."""

import json
import pika

from engine import EngineException, SCOEngineClient, REQUEST_RUN_ID, REQUEST_EXPERIMENT_ID
from rabbitmq import HOST, QUEUE

class RabbitMQClient(SCOEngineClient):
    """SCO Workflow Engine client using RabbitMQ. Sends Json messages containing
    run identifier (and experiment identifier) to run model.
    """
    def run_model(self, model_run):
        """Run model by sending message to RabbitMQ queue containing the
        run end experiment identifier. Messages are persistent to ensure that
        a worker will process process the run request at some point.

        Throws a EngineException if communication with the server fails.

        Parameters
        ----------
        model_run : ModelRunHandle
            Handle to model run
        """
        # Open connection to RabbitMQ server. Will raise an exception if the
        # server is not running. In this case we raise an EngineException to
        # allow caller to delete model run.
        try:
            con = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
            channel = con.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
        except pika.exceptions.AMQPError as ex:
            raise EngineException(str(ex), 500)
        # Create Json object for model run request
        json_obj = {
            REQUEST_RUN_ID : model_run.identifier,
            REQUEST_EXPERIMENT_ID : model_run.experiment
        }
        # Send request
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE,
            body=json.dumps(json_obj),
            properties=pika.BasicProperties(
                delivery_mode = 2, # make message persistent
            )
        )
