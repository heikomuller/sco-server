#!venv/bin/python
import json
import os
import shutil
import tempfile

from flask import Flask, jsonify, make_response, request, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename

import scodata.api
import scodata.attribute as attribute
import scodata.datastore as datastore
import scodata.prediction as prediction
import scodata.mongo as mongo
from engine import EngineException
from engine import RabbitMQClient
import hateoas
import serialize


# -----------------------------------------------------------------------------
#
# App Configuration
#
# -----------------------------------------------------------------------------

# App Path and Url
APP_PATH = '/sco-server/api/v1'
SERVER_URL = 'http://cds-swg1.cims.nyu.edu'
SERVER_PORT = 5000
BASE_URL = SERVER_URL + ':' + str(SERVER_PORT) + APP_PATH + '/'
# Flag to switch debugging on/off
DEBUG = True
# Service description file (JSON)
SERVICE_DESCRIPTION_FILE = './service.json'
# Local folder for data files
DATA_DIR = os.path.abspath('../resources/data')
# Local folder for SCO subject files
ENV_DIR = os.path.abspath('../resources/env/subjects')
# Log file
LOG_FILE = os.path.abspath(DATA_DIR + 'scoserv.log')

# ------------------------------------------------------------------------------
# Initialization
# ------------------------------------------------------------------------------

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = APP_PATH
app.config['DEBUG'] = DEBUG
CORS(app)
# Instantiate the Standard Cortical Observer Data Store.
db = scodata.api.SCODataStore(mongo.MongoDBFactory(), DATA_DIR)
# Instantiate the SCO workflow engine. By default, we use the RabbitMQ engine
# implementation
engine = RabbitMQClient(
    'localhost',
    'sco',
    hateoas.HATEOASReferenceFactory(BASE_URL)
)
# Serializer for resources. Serializer follows REST architecture constraint to
# include hypermedia links with responses.
serializer = serialize.JsonWebAPISerializer(BASE_URL)
# Read service description from file
with open(SERVICE_DESCRIPTION_FILE, 'r') as f:
     service = json.load(f)

# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------

# Number of elements in object listings if limit is not specified in request
DEFAULT_LISTING_SIZE = 10


# ------------------------------------------------------------------------------
#
# API
#
# ------------------------------------------------------------------------------

@app.route('/')
def index():
    """Overview (GET) - Returns object containing web service name and a list
    of references to various resources.
    """
    return jsonify(
        serializer.service_description(service['name'], service['descriptors'])
    )


# ------------------------------------------------------------------------------
# Experiments
# ------------------------------------------------------------------------------

@app.route('/experiments')
def experiments_list():
    """List experiments data (GET) - List of all experiment objects in the
    database.
    """
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate experiment listing and return Json object
    return jsonify(
        serializer.experiments_to_json(
            db.experiments_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/experiments/<string:experiment_id>', methods=['GET'])
def experiments_get(experiment_id):
    """Get experiment (GET) - Retrieve an experiment object from the database.
    """
    # Get experiment object from database.
    experiment = db.experiments_get(experiment_id)
    if experiment is None:
        # Raise exception if experiment does not exist.
        raise ResourceNotFound(experiment_id)
    else:
        # Retrieve associated subject, image_group, and fMRI data (if present)
        # TODO: Handle cases where either of the objects has been deleted
        subject = db.subjects_get(experiment.subject)
        image_group = db.image_groups_get(experiment.images)
        fmri = None
        if not experiment.fmri_data is None:
            fmri = db.experiments_fmri_get(experiment.identifier)
        # Return Json serialization of object.
        return jsonify(
            serializer.experiment_to_json(
                experiment,
                subject,
                image_group,
                fmri=fmri
            )
        )


@app.route('/experiments', methods=['POST'])
def experiments_create():
    """Create experiment (POST) - Create a new experiment object.
    """
    # Make sure that the post request has a json part
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # Make sure that all required keys are present in the given Json object
    for key in ['subject', 'images', 'properties']:
        if not key in json_obj:
            raise InvalidRequest('missing element in Json body: ' + key)
    # Call API method to create a new experiment object
    try:
        experiment = db.experiments_create(
            json_obj['subject'],
            json_obj['images'],
            get_properties_list(json_obj['properties'], True)
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    # Return result including list of references for new experiment
    return jsonify(serializer.response_success(experiment)), 201


@app.route('/experiments/<string:experiment_id>', methods=['DELETE'])
def experiments_delete(experiment_id):
    """Delete experiment (DELETE) - Delete an experiment object from the
    database.
    """
    # Delete experiment object with given identifier. Returns 204 if expeirment
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not db.experiments_delete(experiment_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(experiment_id)


@app.route('/experiments/<string:experiment_id>/properties', methods=['POST'])
def experiments_upsert_property(experiment_id):
    """Upsert experiment property (POST) - Upsert a property of an experiment
    object in the database.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert experiment properties. The response indicates if the experiment
    # exists. Will throw ValueError if property set results in illegal update.
    try:
        if db.experiments_upsert_property(experiment_id, properties) is None:
            raise ResourceNotFound(experiment_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Functional Data
# ------------------------------------------------------------------------------

@app.route('/experiments/<string:experiment_id>/fmri', methods=['GET'])
def experiments_fmri_get(experiment_id):
    """Get functional MRI data (GET) - Retrieve a functional MRI data object
    from the database.
    """
    # Get experiments fMRI object from database.
    fmri = db.experiments_fmri_get(experiment_id)
    if fmri is None:
        # Raise exception if experiments fMRI does not exist.
        raise ResourceNotFound(experiment_id + ':fmri')
    else:
        # Return Json serialization of object.
        return jsonify(serializer.experiment_fmri_to_json(fmri))


@app.route('/experiments/<string:experiment_id>/fmri', methods=['POST'])
def experiments_fmri_create(experiment_id):
    """Upload functional MRI data (POST) - Upload a functional MRI data archive
    file that is associated with a experiment.
    """
    # Get the uploaded file. Method raises InvalidRequest if no file was given
    tmp_dir, upload_file = get_upload_file(request)
    # Upload the fMRI data and associate it with the experiment.
    fmri = db.experiments_fmri_create(
        experiment_id,
        upload_file
    )
    # Delete temporary directory
    shutil.rmtree(tmp_dir)
    # If fMRI is None the given experiment does not exist
    if fmri is None:
        raise ResourceNotFound(experiment_id + ':fmri')
    # Return result including a list of references to updated experiment
    return jsonify(serializer.response_success(fmri)), 201


@app.route('/experiments/<string:experiment_id>/fmri', methods=['DELETE'])
def experiments_fmri_delete(experiment_id):
    """Delete experiment fMRI data (DELETE) - Delete fMRI data associated with
    an experiment object from the database.
    """
    # Delete experiments fMRI object with given identifier. Returns 204 if
    # experiment had fMRI data associated with it or 404 if result of delete is
    # None (by raising ResourceNotFound)
    if not db.experiments_fmri_delete(experiment_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(experiment_id + ':fmri')


@app.route('/experiments/<string:experiment_id>/fmri/data')
def experiments_fmri_download(experiment_id):
    """Download functional MRI data (GET) - Download data of previously uploaded
    functional MRI data.
    """
    # Get download information for experiments fMRI data object. The result is
    # None if experiment of fMRI obejct does not exist.
    file_info = db.experiments_fmri_download(experiment_id)
    if file_info is None:
        raise ResourceNotFound(experiment_id + ':fmri')
    # Send file in the object's upload folder
    return send_file(
        file_info.file,
        mimetype=file_info.mime_type,
        as_attachment=True,
        attachment_filename=file_info.name
    )


@app.route('/experiments/<string:experiment_id>/fmri/properties', methods=['POST'])
def experiments_fmri_upsert_property(experiment_id):
    """Upsert functional MRI data object (POST) - Upsert a property of a
    functional MRI data object in the database.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert fMRI data object's properties. The response indicates if the object
    # exists. Will throw ValueError if property set results in illegal update.
    try:
        if db.experiments_fmri_upsert_property(experiment_id, properties) is None:
            raise ResourceNotFound(experiment_id + ':fmri')
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Prediction Data
# ------------------------------------------------------------------------------

@app.route('/experiments/<string:experiment_id>/predictions', methods=['GET'])
def experiments_predictions_list(experiment_id):
    """List predictions (GET) - Get a list of all model runs and their
    prediction results that are associated with a given experiment.
    """
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate prediction listing and return Json object
    return jsonify(
        serializer.experiment_predictions_to_json(
            db.experiments_predictions_list(
                experiment_id,
                limit=limit,
                offset=offset),
            prop_set,
            experiment_id
        )
    )


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>', methods=['GET'])
def experiments_predictions_get(experiment_id, prediction_id):
    """Get prediction (GET) - Retrieve a model run and its prediction result
    for a given experiment.
    """
    # Get prediction object from database.
    prediction = db.experiments_predictions_get(experiment_id, prediction_id)
    if prediction is None:
        # Raise exception if prediction does not exist.
        raise ResourceNotFound(experiment_id + ':' + prediction_id)
    else:
        # Return Json serialization of object.
        return jsonify(serializer.experiment_prediction_to_json(
            prediction,
            db.experiments_get(experiment_id)
        ))


@app.route('/experiments/<string:experiment_id>/predictions', methods=['POST'])
def experiments_predictions_create(experiment_id):
    """Create model run (POST) - Start a new model run for an experiment using a
    user provided set of arguments.
    """
    # Make sure that the post request has a json part
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # Make sure that all required keys are present in the given Json object
    for key in ['name', 'arguments']:
        if not key in json_obj:
            raise InvalidRequest('missing element in Json body: ' + key)
    # Get dictionary of properties ifpresent in request
    if 'properties' in json_obj:
        properties = get_properties_list(json_obj['properties'], False)
    else:
        properties = None
    # Call create method of API to get a new model run object handle.
    model_run = db.experiments_predictions_create(
        experiment_id,
        json_obj['name'],
        get_attributes(json_obj['arguments']),
        properties=properties
    )
    # The result is None if experiment does not exists
    if model_run is None:
        raise ResourceNotFound(experiment_id)
    # Start the model run
    try:
        engine.run_model(model_run)
    except EngineException as ex:
        # Delete model run from database if running the model failed.
        db.experiments_predictions_delete(
            model_run.experiment,
            model_run.identifier,
            erase=True
        )
        raise APIRequestException(ex.message, ex.status_code)
    # Return result including list of references for new model run.
    return jsonify(serializer.response_success(model_run)), 201


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>', methods=['DELETE'])
def experiments_predictions_delete(experiment_id, prediction_id):
    """Delete prediction (DELETE) - Delete model run and potential prediction
    results associated with a given experiment.
    """
    # Delete prediction object with given identifier. Returns 204 if prediction
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not db.experiments_predictions_delete(experiment_id, prediction_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(experiment_id + ':' + prediction_id)


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>/data')
def experiments_predictions_download(experiment_id, prediction_id):
    """Download prediction (GET) - Download prediction result generated by a
    successfully finished model run that is associated with a given experiment.
    """
    # Get download information for given object.
    file_info = db.experiments_predictions_download(experiment_id, prediction_id)
    # The result is None if object does not exist
    if file_info is None:
        raise ResourceNotFound(experiment_id + ':' + prediction_id)
    # Send file in the object's upload folder
    return send_file(
        file_info.file,
        mimetype=file_info.mime_type,
        as_attachment=True,
        attachment_filename=file_info.name
    )


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>/properties', methods=['POST'])
def experiments_predictions_upsert_property(experiment_id, prediction_id):
    """Upsert prediction (POST) - Upsert a property of a model run object
    associated with a given experiment.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert model run properties. The response indicates if the model run
    # exists. Will throw ValueError if property set results in illegal update.
    try:
        result = db.experiments_predictions_upsert_property(
            experiment_id,
            prediction_id,
            properties)
        if result is None:
            raise ResourceNotFound(prediction_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>/state', methods=['POST'])
def experiments_predictions_update_state(experiment_id, prediction_id):
    """Update run state (POST) - Update the state of an existing model run."""
    # Get state object from request
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    if not 'type' in json_obj:
        raise InvalidRequest('missing element: type')
    state = prediction.ModelRunState.from_json(json_obj)
    if state is None:
        raise InvalidRequest('invalid state object')
    # Update state. If result is None (i.e., experiment of model run does not
    # exists) return 404. Otherwise, return 200. If a ValueError is raised the
    # intended update violates a valid model run time line.
    try:
        result = db.experiments_predictions_update_state(
            experiment_id,
            prediction_id,
            state
        )
        if result is None:
            raise ResourceNotFound(prediction_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Images
# ------------------------------------------------------------------------------

@app.route('/images/files')
def image_files_list():
    """List images (GET) - List of all image objects in the database."""
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate image file listing and return Json object
    return jsonify(
        serializer.image_files_to_json(
            db.image_files_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/images/files/<string:image_id>', methods=['GET'])
def image_files_get(image_id):
    """Get image (GET) - Retrieve an image object from the database."""
    # Get image file object from database.
    img = db.image_files_get(image_id)
    if img is None:
        # Raise exception if image does not exist.
        raise ResourceNotFound(image_id)
    else:
        # Return Json serialization of object.
        return jsonify(serializer.image_file_to_json(img))


@app.route('/images/files/<string:image_id>', methods=['DELETE'])
def image_files_delete(image_id):
    """Delete image object (DELETE) - Delete an image object from the
    database.
    """
    # Delete image file object with given identifier. Returns 204 if image
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not db.image_files_delete(image_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(image_id)


@app.route('/images/files/<string:image_id>/data')
def image_files_download(image_id):
    """Download image file (GET)"""
    # Get download information for given object.
    file_info = db.image_files_download(image_id)
    # The result is None if object does not exist
    if file_info is None:
        raise ResourceNotFound(image_id)
    # Send file in the object's upload folder
    return send_file(
        file_info.file,
        mimetype=file_info.mime_type,
        as_attachment=False,
        attachment_filename=file_info.name
    )


@app.route('/images/files/<string:image_id>/properties', methods=['POST'])
def image_files_upsert_property(image_id):
    """Upsert image object (POST) - Upsert a property of an image object in the
    database.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert image file properties. The response indicates if the image exists.
    # Will throw ValueError if property set results in illegal update.
    try:
        if db.image_files_upsert_property(image_id, properties) is None:
            raise ResourceNotFound(image_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Image Groups
# ------------------------------------------------------------------------------

@app.route('/images/groups')
def image_groups_list():
    """List image groups (GET) - List of all image group objects in the
    database."""
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate image group listing and return Json object
    return jsonify(
        serializer.image_groups_to_json(
            db.image_groups_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/images/groups/<string:image_group_id>', methods=['GET'])
def image_groups_get(image_group_id):
    """Get image group (GET) - Retrieve an image group from the database."""
    # Get image group object from database.
    img_grp = db.image_groups_get(image_group_id)
    if img_grp is None:
        # Raise exception if image group does not exist.
        raise ResourceNotFound(image_group_id)
    else:
        # Return Json serialization of object.
        return jsonify(serializer.image_group_to_json(img_grp))


@app.route('/images/groups/<string:image_group_id>', methods=['DELETE'])
def image_groups_delete(image_group_id):
    """Delete image group (DELETE) - Delete an image group object from the
    database.
    """
    # Delete image group object with given identifier. Returns 204 if image
    # group existed or 404 if result of delete is None (by raising
    # ResourceNotFound)
    if not db.image_groups_delete(image_group_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(image_group_id)


@app.route('/images/groups/<string:image_group_id>/data')
def image_groups_download(image_group_id):
    """Download image group file (GET)"""
    # Get download information for given object.
    file_info = db.image_groups_download(image_group_id)
    # The result is None if object does not exist
    if file_info is None:
        raise ResourceNotFound(image_group_id)
    # Send file in the object's upload folder
    return send_file(
        file_info.file,
        mimetype=file_info.mime_type,
        as_attachment=True,
        attachment_filename=file_info.name
    )


@app.route('/images/groups/<string:image_group_id>/images')
def image_groups_images_list(image_group_id):
    """List image group images (GET)"""
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type. Property set is ignored since group images have no
    # additional properties
    offset, limit, prop_set = get_listing_arguments(request)
    # Get group image listing. Return 404 if result is None, i.e., image group
    # is unknown
    listing = db.image_group_images_list(image_group_id, limit=limit, offset=offset)
    if limit is None:
        raise ResourceNotFound(image_group_id)
    # Decorate group image listing and return Json object
    return jsonify(serializer.image_group_images_to_json(listing, image_group_id))


@app.route('/images/groups/<string:image_group_id>/options', methods=['POST'])
def image_groups_update_options(image_group_id):
    """Upsert image group options (POST) - Upsert the options that are
    associated with an image group in the database. Given that these options
    cannot be included in the file upload, there has to be a separate API call.
    """
    # Make sure that the request contains a Json body with an 'options' element
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    if not 'options' in json_obj:
        raise InvalidRequest('missing element in Json body: options')
    # Convert the Json element associated with key 'options' into a list of
    # typed property instance. Throws an InvalidRequest exception if the format
    # of the element value is invalid.
    attributes = get_attributes(json_obj['options'])
    # Upsert object options. The result will be None if image group does not
    # exist.
    try:
        img_grp = db.image_groups_update_options(image_group_id, attributes)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if img_grp is None:
        raise ResourceNotFound(image_group_id)
    return '', 200


@app.route('/images/groups/<string:image_group_id>/properties', methods=['POST'])
def image_groups_upsert_property(image_group_id):
    """Upsert image object (POST) - Upsert a property of an image group in the
    database.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert image group properties. The response indicates if the image group
    # exists. Will throw ValueError if property set results in illegal update.
    try:
        if db.image_groups_upsert_property(image_group_id, properties) is None:
            raise ResourceNotFound(image_group_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Upload Images
# ------------------------------------------------------------------------------

@app.route('/images/upload', methods=['POST'])
def images_create():
    """Upload Images (POST) - Upload an image file or an archive of images."""
    # Upload the file to get handle. Type will depend on suffix of uploaded
    # file. A value error will be raised if file is invalid.
    tmp_dir, upload_file = get_upload_file(request)
    try:
        img_handle =  db.images_create(upload_file)
    except ValueError as err:
        # Make sure to delete temporary file before raising InvalidRequest
        shutil.rmtree(tmp_dir)
        raise InvalidRequest(str(err))
    # Clean up and return success.
    shutil.rmtree(tmp_dir)
    return jsonify(serializer.response_success(img_handle)), 201


# ------------------------------------------------------------------------------
# Subjects
# ------------------------------------------------------------------------------

@app.route('/subjects')
def subjects_list():
    """List subjects (GET) - List of brain anatomy MRI objects in the
    database.
    """
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate subject listing and return Json object
    return jsonify(
        serializer.subjects_to_json(
            db.subjects_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/subjects/<string:subject_id>', methods=['GET'])
def subjects_get(subject_id):
    """Get subject (GET) - Retrieve a brain anatomy MRI object from the
    database.
    """
    # Get subject from database.
    subject = db.subjects_get(subject_id)
    if subject is None:
        # Raise exception if subject does not exist.
        raise ResourceNotFound(subject_id)
    else:
        # Return Json serialization of object.
        return jsonify(serializer.subject_to_json(subject))


@app.route('/subjects', methods=['POST'])
def subjects_create():
    """Upload Subject (POST) - Upload an brain anatomy MRI archive file."""
    # Upload the given file to get an object handle for new subject. Method
    # throws InvalidRequest exception if necessary.
    tmp_dir, upload_file = get_upload_file(request)
    try:
        subject =  db.subjects_create(upload_file)
    except ValueError as ex:
        # Make sure to clean up and raise InvalidRequest exception
        shutil.rmtree(tmp_dir)
        raise InvalidRequest(str(ex))
    # Delete temp folder and return success.
    shutil.rmtree(tmp_dir)
    return jsonify(serializer.response_success(subject)), 201


@app.route('/subjects/<string:subject_id>', methods=['DELETE'])
def subjects_delete(subject_id):
    """Delete Subject (DELETE) - Delete a brain anatomy MRI object from the
    database.
    """
    # Delete subject data object with given identifier. Returns 204 if subject
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not db.subjects_delete(subject_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(subject_id)


@app.route('/subjects/<string:subject_id>/data')
def subject_download(subject_id):
    """Download subject (GET) - Download data of previously uploaded subject
    anatomy.
    """
    # Get download information for given object. Method raises ResourceNotFound
    # exception if object does not exists or does not have any downloadable
    # representation.
    file_info = db.subjects_download(subject_id)
    # The result is None if subject does not exists
    if file_info is None:
        raise ResourceNotFound(subject_id)
    # Send file in the object's upload folder
    return send_file(
        file_info.file,
        mimetype=file_info.mime_type,
        as_attachment=True,
        attachment_filename=file_info.name
    )


@app.route('/subjects/<string:subject_id>/properties', methods=['POST'])
def subjects_upsert_property(subject_id):
    """Upsert subject object (POST) - Upsert a property of a brain anatomy MRI
    object in the database.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert subject properties. The response indicates if the subject exists.
    # Will throw ValueError if property set results in illegal update.
    try:
        if db.subjects_upsert_property(subject_id, properties) is None:
            raise ResourceNotFound(subject_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
#
# API Request Exceptions
#
# ------------------------------------------------------------------------------

class APIRequestException(Exception):
    """Base class for API exceptions."""
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


class InvalidRequest(APIRequestException):
    """Exception for invalid requests that have status code 400."""
    def __init__(self, message):
        """Initialize the message and status code (400) of super class.

        Parameters
        ----------
        message : string
            Error message.
        """
        super(InvalidRequest, self).__init__(message, 400)


class ResourceNotFound(APIRequestException):
    """Exception for file not found situations that have status code 404."""
    def __init__(self, object_id):
        """Initialize the message and status code (404) of super class.

        Parameters
        ----------
        object_id : string
            Identifier of unknown resource
        """
        # Build the response message depending on whether object type is given
        message = 'unknown identifier: ' + object_id
        # Initialize the super class
        super(ResourceNotFound, self).__init__(message, 404)


# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Method Wrapper's that validate request format and arguments
# ------------------------------------------------------------------------------

def get_attributes(json_array):
    """Transform an Json array of typed attribute values into a list of
    datastore.Attribute objects.

    Expects a list of Json objects having 'name' and 'value' keys. The type of
    the element associated with the 'value' key is arbitrary. Raises a
    InvalidRequest exception if the given array violated expected format.

    Parameters
    ----------
    json_array : array
        List of Json objects ({'name':..., 'value':...})

    Returns
    -------
    List(Attribute)
        List of typed attribute instances
    """
    result = []
    # Make sure the given array is a list
    if not isinstance(json_array, list):
        raise InvalidRequest('argument is not a list')
    # Iterate over all elements in the list. Make sure they are Json objects
    # with 'name' and 'value' elements
    for element in json_array:
        if not isinstance(element, dict):
            raise InvalidRequest('element is not a dictionary')
        for key in ['name', 'value']:
            if not key in element:
                raise InvalidRequest('object has not key ' + key)
        name = str(element['name'])
        value = element['value']
        result.append(attribute.Attribute(name, value))
    return result


def get_listing_arguments(request, default_limit=DEFAULT_LISTING_SIZE):
    """Extract listing arguments from given request. Returns default values
    for parameters not present in the request.

    Parameters
    ----------
    request : flask.request
        Flask request object
    default_limit : int
        Default listing size if limit argument is not given in request

    Returns
    -------
    (int, int, [string])
        Tuple of offset, limit, properties
    """
    # Check if limit and offset parameters are given. Throws ValueError if
    # values are not integer.
    try:
        offset = int(request.args[hateoas.QPARA_OFFSET]) if hateoas.QPARA_OFFSET in request.args else 0
        limit = int(request.args[hateoas.QPARA_LIMIT]) if hateoas.QPARA_LIMIT in request.args else default_limit
    except ValueError as err:
        raise InvalidRequest(str(err))
    # Get the set of attributes that are included in the result listing. By
    # default, the object name is always included.
    prop_set = request.args[hateoas.QPARA_PROPERTIES].split(',') if hateoas.QPARA_PROPERTIES in request.args else None
    return offset, limit, prop_set


def get_properties_list(json_array, is_mandatory_value):
    """Convert an Json Array of key,value pairs into a dictionary.

    Parameters
    ----------
    List(Json object) : Array of Json objects
        List of Key,value-pairs
    is_mandatory_value : Boolean
        Flag indicating whether the 'value' element is mandatory for elements
        in the list

    Returns
    -------
    Dictionary
    """
    properties = {}
    for item in json_array:
        # Ensure that the Json object has all required elements
        if not 'key' in item:
            raise InvalidRequest('missing element: key')
        if is_mandatory_value and not 'value' in item:
            raise InvalidRequest('missing element: value')
        # Get key and new value (if given) of property to update
        key = item['key']
        value = item['value'] if 'value' in item else None
        properties[key] = value
    return properties


def get_upload_file(request):
    """Generalized method for file uploads. Ensures that request contains a
    file and returns the file. Raise InvalidRequest exception if request does
    not contain an uploded file.

    Creates a temporal directory and saves the uploaded file in that directory.

    Parameters
    ----------
    request : flask.request
        Flask request object

    Returns
    -------
    Temp Dir, File name
        File object in upload request and reference to created temporary
        directory.
    """
    # Make sure that the post request has the file part
    if 'file' not in request.files:
        raise InvalidRequest('no file argument in request')
    file = request.files['file']
    # A browser may submit a empty part without filename
    if file.filename == '':
        raise InvalidRequest('empty file name')
    # Save uploaded file to temp directory
    temp_dir = tempfile.mkdtemp()
    filename = secure_filename(file.filename)
    upload_file = os.path.join(temp_dir, filename)
    file.save(upload_file)
    return temp_dir, upload_file


def get_upsert_properties(request):
    """Extract dictionary of key,value-pairs defining the set of property
    upserts for an API resource.

    Parameters
    ----------
    request : flask.request
        Flask request object

    Returns
    -------
    Dictinary
        Dictionary of Key,value-pairs describing property updates
    """
    # Abort with 400 if the request is not a Json request or if the Json object
    # in the request does not have field key. Field value is optional.
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    if not 'properties' in request.json:
        raise InvalidRequest('missing element: properties')
    return get_properties_list(request.json['properties'], False)


# ------------------------------------------------------------------------------
# Error Handler
# ------------------------------------------------------------------------------

@app.errorhandler(APIRequestException)
def invalid_request_or_resource_not_found(error):
    """JSON response handler for invalid requests or requests that access
    unknown resources.

    Parameters
    ----------
    error :
        Exception thrown by request Handler

    Returns
    -------
    Http response
    """
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(500)
def internal_error(exception):
    """Exception handler that logs exceptions."""
    app.logger.error(exception)
    return make_response(jsonify({'error': str(exception)}), 500)


# ------------------------------------------------------------------------------
#
# Main
#
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    # Relevant documents:
    # http://werkzeug.pocoo.org/docs/middlewares/
    # http://flask.pocoo.org/docs/patterns/appdispatch/
    from werkzeug.serving import run_simple
    from werkzeug.wsgi import DispatcherMiddleware
    # Switch logging on if not in debug mode
    if app.debug is not True:
        import logging
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=1024 * 1024 * 100,
            backupCount=20
        )
        file_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        app.logger.addHandler(file_handler)
    # Load a dummy app at the root URL to give 404 errors.
    # Serve app at APPLICATION_ROOT for localhost development.
    application = DispatcherMiddleware(Flask('dummy_app'), {
        app.config['APPLICATION_ROOT']: app,
    })
    run_simple('0.0.0.0', SERVER_PORT, application, use_reloader=True)
