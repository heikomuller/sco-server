"""Standard Cortical Observer - Workflow Engine API.

The workflow engine is used to run predictive models for experiments that are
defined in the SCO Data Store. The classes in this module define client
interfaces to the engine. The clients help to decouple the RESTful web server
from the predictive model.
"""
from abc import abstractmethod
import json
import socket

# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

# Client request fields
REQUEST_EXPERIMENT_ID = 'experiment_id'
REQUEST_RUN_ID = 'run_id'


# ------------------------------------------------------------------------------
#
# Client
#
# ------------------------------------------------------------------------------

class SCOEngineClient(object):
    """Client for SCO engine. Communicates with workflow backend via simple
    messages. Different implementations of this client-server architecture
    are possible.
    """
    @abstractmethod
    def run_model(self, model_run):
        """Execute the given model run.

        Throws a EngineException if running the model fails.

        Parameters
        ----------
        model_run : ModelRunHandle
            Handle to model run
        """
    pass


# ------------------------------------------------------------------------------
#
# Exception
#
# ------------------------------------------------------------------------------

class EngineException(Exception):
    """Base class for SCO engine exceptions."""
    def __init__(self, message, status_code):
        """Initialize error message and status code.

        Parameters
        ----------
        message : string
            Error message.
        status_code : int
            Http status code.
        """
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code

    def to_dict(self):
        """Dictionary representation of the exception.

        Returns
        -------
        Dictionary
        """
        return {'message' : self.message}
