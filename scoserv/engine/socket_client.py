import json
import socket

from engine import EngineException, SCOEngineClient, REQUEST_RUN_ID, REQUEST_EXPERIMENT_ID

# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

# Response fields for default client and backend
RESPONSE_MESSAGE = 'message'
RESPONSE_STATUS = 'status'


class DefaultSCOEngineClient(SCOEngineClient):
    """Default Client for SCO engine. Communicate with server via sockets."""
    def __init__(self, server_host, server_port):
        """Initialize the servers host name and port for socket communication.

        Raises socket.gaierror if host name cannot be resolved.

        Parameters
        ----------
        server_host : string
            Name of host running the SCO engine
        server_port : int
            Port SCO engine is listening on
        """
        self.host = socket.gethostbyname(server_host)
        self.port = server_port

    def run_model(self, model_run):
        """Execute the given model run. Comminicates with the SCO engine to run
        the model.

        Throws a EngineException if running the model fails.

        Parameters
        ----------
        model_run : ModelRunHandle
            Handle to model run
        """
        # Communication protocoll uses Json. Create run request containing
        # run identifier and experiment identifier.
        request = {
            REQUEST_RUN_ID : model_run.identifier,
            REQUEST_EXPERIMENT_ID : model_run.experiment
        }
        # Connect to server
        try:
            s = socket.create_connection((self.host , self.port), timeout=10)
        except socket.error as ex:
            raise EngineException(str(ex), 500)
        # Send request
        try:
            s.sendall(json.dumps(request))
        except socket.error as ex:
            raise EngineException(str(ex), 500)
        # Read response from server. Expect Json object with at least status
        # field. If status code is not equal to 200 an exception occurred and
        # the server response is expected to contain an additional message field
        try:
            reply = json.loads(s.recv(4096))
        except Exception as ex:
            raise EngineException(str(ex), 500)
        if reply[RESPONSE_STATUS] != 200:
            raise EngineException(
                reply[RESPONSE_MESSAGE],
                reply[RESPONSE_STATUS]
            )
        s.close()
