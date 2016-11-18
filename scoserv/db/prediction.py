"""Predictions - Collection of methods to store and manipulate SCO model runs
and their outputs (predictions).
"""

import datetime
import os
import uuid

import attribute
import datastore


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

"""Model run error states."""

STATE_FAILED = 'FAILED'
STATE_RUNNING = 'RUNNING'
STATE_SUCCESS = 'SUCCESS'

# Set of all possible model run states
RUN_STATES = [STATE_FAILED, STATE_RUNNING, STATE_SUCCESS]


# ------------------------------------------------------------------------------
#
# Database Objects
#
# ------------------------------------------------------------------------------

class PredictionHandle(datastore.DataObject):
    """Model Run Handle - Handle to access and manipulate an object
    representing a model run and its result (prediction).

    The status of the model run is maintained as an attribute of the handle.
    A run that has completed successfully will have a prediction result attached
    to it. In case of failure, there will be an error object containing
    information about the error condition.

    Model runs are weak entities, i.e., their object identifier contains the
    experiment identifier.

    Attributes
    ----------
    state : string
        Text representation of the model run state
    arguments: Dictionary(attribute.Attribute)
        Dictionary of typed attributes defining the image group options
    prediction: datastore.ObjectId, optional
        Unique identifier of associated prediction result (only if state is
        SUCCESS)
    errors : list(string), optional
        List of error messages (only if state is FAILED)
    """
    def __init__(
        self,
        identifier,
        properties,
        directory,
        state,
        arguments,
        prediction=None,
        errors=None,
        timestamp=None,
        is_active=True):
        """Initialize the subject handle. The directory references a directory
        on the local disk that contains the anatomy data files necessary as
        input when running a model locally.

        Parameters
        ----------
        identifier : datastore.ObjectId
            Unique object identifier
        properties : Dictionary
            Dictionary of experiment specific properties
        directory : string
            Directory on local disk that contains model run output files
        state : string
            Text representation of the model run state
        arguments: Dictionary(attribute.Attribute)
            Dictionary of typed attributes defining the model run arguments
        prediction: datastore.ObjectId, optional
            Unique identifier of associated prediction result (only if state is
            SUCCESS)
        errors : list(string), optional
            List of error messages (only if state is FAILED)
        timestamp : datetime, optional
            Time stamp of object creation (UTC).
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Raise a value error if given state is not in the list of defined
        # states
        if not state in RUN_STATES:
            raise ValueError('invalud model run state: ' + repr(state))
        # Initialize super class
        super(PredictionHandle, self).__init__(
            identifier,
            datastore.OBJ_PREDICTION,
            timestamp,
            properties,
            directory,
            is_active=is_active
        )
        # Initialize class specific Attributes
        self.state = state
        self.arguments = arguments
        self.prediction = prediction
        self.errors = errors

    @property
    def is_failed(self):
        """Flag indicating if the model run has exited in a failed state.

        Returns
        -------
        Boolean
            True, if model run is in falied state.
        """
        return self.state == STATE_FAILED

    @property
    def is_running(self):
        """Flag indicating if the model run is in a running state.

        Returns
        -------
        Boolean
            True, if model run is in running state.
        """
        return self.state == STATE_RUNNING

    @property
    def is_success(self):
        """Flag indicating if the model run has finished with success.

        Returns
        -------
        Boolean
            True, if model run is in success state.
        """
        return self.state == STATE_SUCCESS


# ------------------------------------------------------------------------------
#
# Object Stores
#
# ------------------------------------------------------------------------------

class DefaultPredictionManager(datastore.DefaultObjectStore):
    """Default Prediction Manager - Manager for model runs and their outputs.

    This is a default implentation that uses MongoDB as storage backend.

    Attributes
    ----------
    directory : string
        Base directory on local disk for model run output files.
    parameters : list(attribute.AttributeDefinition)
        List of parameters that can be passed to the model when running it.
    """
    def __init__(self, mongo_collection, base_directory):
        """Initialize the MongoDB collection and base directory where to store
        model runs and ouput files. Set immutable and mandatory properties.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB storing model run information
        base_directory : string
            Base directory on local disk for model run output files. Files are
            stored in sub-directories named by the model run identifier.
        parameters : list(attribute.AttributeDefinition)
            List of parameters that can be passed to the model when running it.
        """
        # Initialize the super class
        super(DefaultPredictionManager, self).__init__(
            mongo_collection,
            base_directory
        )
        # Initialize the definition of image group options attributes
        self.parameters = {
            'gabor_orientations' : attribute.AttributeDefinition(
                'gabor_orientations',
                attribute.IntegerType(),
                default_value=8
            ),
            'max_eccentricity' : attribute.AttributeDefinition(
                'max_eccentricity',
                attribute.FloatType(),
                default_value=12
            ),
            'stimulus_aperture_edge_value' : attribute.AttributeDefinition(
                'stimulus_aperture_edge_value',
                attribute.FloatType()
            ),
            'normalized_pixels_per_degree' : attribute.AttributeDefinition(
                'normalized_pixels_per_degree',
                attribute.FloatType()
            )
        }

    def create_object(self, experiment, name, arguments):
        """Create a model run object with the given list of arguments. The
        initial state of the object is RUNNING.

        Parameters
        ----------
        experiment : datastore.ObjectId
            Unique identifier of associated experiment object
        name : string
            User-provided name for the model run
        arguments : list(attribute.Attribute)
            List of attribute instances

        Returns
        -------
        PredictionHandle
            Object handle for created model run
        """
        # Create a new object identifier.
        identifier = str(uuid.uuid4())
        # Create the initial set of properties.
        properties = {datastore.PROPERTY_NAME: name}
        # Directories are simply named by object identifier
        directory = os.path.join(self.directory, identifier)
        # Create the directory if it doesn't exists
        if not os.access(directory, os.F_OK):
            os.makedirs(directory)
        # The initial set of arguments is given by those parameter for which
        # a default value is defined.
        run_arguments = attribute.get_default_attributes(self.parameters)
        # Go through the list of user-provided arguments and add/replace them
        # in the set of default arguments. Raise a ValueError for arguments
        # that do not appear in the parameter definition set.
        for attr in arguments:
            # Ensure that attribute has known name
            if not attr.name in self.parameters:
                raise ValueError('unknown model run parameter: ' + attr.name)
            # Ensure that attribute value is of expected type
            attr_def = self.parameters[key]
            if not attr_def.validate(attr.value):
                raise ValueError('invalid value for attribute: ' + attr.name)
            run_arguments[attr.name] = attr

        # Create the image group object and store it in the database before
        # returning it.
        obj = PredictionHandle(
            datastore.ObjectId(experiment.keys + [identifier]),
            properties,
            directory,
            STATE_RUNNING,
            run_arguments
        )
        self.insert_object(obj)
        return obj

    def from_json(self, document):
        """Create model run object from JSON document retrieved from database.

        Parameters
        ----------
        document : JSON
            Json document in database

        Returns
        -------
        PredictionHandle
            Handle for model run object
        """
        # Get object identifier from Json document
        identifier = datastore.ObjectId(document['identifier'])
        # Directories are named by the last element of the object identifier
        directory = os.path.join(self.directory, identifier.keys[-1])
        # Create model run handle.
        return PredictionHandle(
            identifier,
            document['properties'],
            directory,
            document['state'],
            attribute.attributes_from_json(document['arguments']),
            timestamp=datetime.datetime.strptime(
                document['timestamp'], '%Y-%m-%dT%H:%M:%S.%f'
            ),
            is_active=document['active']
        )

    def to_json(self, model_run):
        """Create a Json-like dictionary for a model run object. Extends the
        basic object with run state, arguments, and optional prediction results
        or error descriptions.

        Parameters
        ----------
        model_run : PredictionHandle

        Returns
        -------
        (JSON)
            Json-like object, i.e., dictionary.
        """
        # Get the basic Json object from the super class
        json_obj = super(DefaultPredictionManager, self).to_json(model_run)
        # Add run state
        json_obj['state'] = model_run.state
        # Transform dictionary of attributes into list of key-value pairs.
        json_obj['arguments'] = attribute.attributes_to_json(model_run.arguments)
        return json_obj
