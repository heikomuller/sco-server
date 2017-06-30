#!venv/bin/python
import os
import shutil
import tempfile
import urllib2
import yaml

from flask import Flask, jsonify, make_response, request, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename

from api import SCOServerAPI
import hateoas

# ------------------------------------------------------------------------------
#
# Gobal Constants
#
# ------------------------------------------------------------------------------

"""Environment Variable containing path to config file. If not set will try
file config.yaml in working directory.
"""
ENV_CONFIG = 'SCOSERVER_CONFIG'

"""Url to default configuration file on GitHub."""
WEB_CONFIG_FILE_URI = 'https://raw.githubusercontent.com/heikomuller/sco-server/master/config/config.yaml'

"""Number of elements in object listings if limit is not specified in request"""
DEFAULT_LISTING_SIZE = 10


# -----------------------------------------------------------------------------
#
# App Configuration
#
# -----------------------------------------------------------------------------

# Expects a server config file in the local directory. The config file is
# expected to contain values for all server configuration parameter. These
# parameters are:
#
# server.apppath : Application path part of the Url to access the app
# server.url : Base Url of the server where the app is running
# server.port: Port the server is running on
# server.datadir : Path to base directory for data store
# server.logfile : Path to server log file
#
# app.name : Application name for the service description
# app.title : Application title for service description (used a page title in UI)
# app.debug : Flag to switch debugging on/off
#
# home.title : Title for main content on Web UI homepage
# home.content : Html snippet containing the Web UI homepage content
#
# The file is expected to contain a Json object with a single element
# 'properties' that is an array of key, value pair objects representing the
# configuration parameters.
LOCAL_CONFIG_FILE = os.getenv(ENV_CONFIG)
if not LOCAL_CONFIG_FILE is None and os.path.isfile(LOCAL_CONFIG_FILE):
    with open(LOCAL_CONFIG_FILE, 'r') as f:
        obj = yaml.load(f.read())
elif os.path.isfile('./config.yaml'):
    with open('./config.yaml', 'r') as f:
        obj = yaml.load(f.read())
elif os.path.isfile('/var/sco/config/config.yaml'):
    with open('/var/sco/config/config.yaml', 'r') as f:
        obj = yaml.load(f.read())
else:
    obj = yaml.load(urllib2.urlopen(WEB_CONFIG_FILE_URI).read())
config = {item['key']:item['value'] for item in obj['properties']}

# App Path and Url
APP_PATH = config['server.apppath']
SERVER_URL = config['server.url']
SERVER_PORT = config['server.port']
BASE_URL = SERVER_URL
if SERVER_PORT != 80:
    BASE_URL += ':' + str(SERVER_PORT)
BASE_URL += APP_PATH + '/'
# Flag to switch debugging on/off
DEBUG = config['app.debug']

# Log file
LOG_FILE = os.path.abspath(config['server.logfile'])

# ------------------------------------------------------------------------------
# Initialization
# ------------------------------------------------------------------------------

# Initialize the server API
api = SCOServerAPI(config, BASE_URL)

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = APP_PATH
app.config['DEBUG'] = DEBUG
CORS(app)


# ------------------------------------------------------------------------------
#
# API
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Service
# ------------------------------------------------------------------------------

@app.route('/')
def index():
    """Overview (GET) - Returns object containing web service name and a list
    of references to various resources.
    """
    return jsonify(api.service_description())


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
        api.experiments_list(limit=limit, offset=offset, properties=prop_set)
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
        result = api.experiments_create(
            json_obj['subject'],
            json_obj['images'],
            get_properties_list(json_obj['properties'], True)
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    # Return result including list of references for new experiment
    return jsonify(result), 201


@app.route('/experiments/<string:experiment_id>', methods=['GET'])
def experiments_get(experiment_id):
    """Get experiment (GET) - Retrieve an experiment object from the database.
    """
    # Get experiment object from database. Raise exception if experiment does
    # not exist.
    experiment = api.experiments_get(experiment_id)
    if experiment is None:
        raise ResourceNotFound(experiment_id)
    else:
        return jsonify(experiment)


@app.route('/experiments/<string:experiment_id>', methods=['DELETE'])
def experiments_delete(experiment_id):
    """Delete experiment (DELETE) - Delete an experiment object from the
    database.
    """
    # Delete experiment object with given identifier. Returns 204 if expeirment
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not api.experiments_delete(experiment_id) is None:
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
        if api.experiments_upsert_property(experiment_id, properties) is None:
            raise ResourceNotFound(experiment_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Functional Data
# ------------------------------------------------------------------------------

@app.route('/experiments/<string:experiment_id>/fmri', methods=['POST'])
def experiments_fmri_create(experiment_id):
    """Upload functional MRI data (POST) - Upload a functional MRI data archive
    file that is associated with a experiment.
    """
    # Get the uploaded file. Method raises InvalidRequest if no file was given
    tmp_dir, upload_file = get_upload_file(request)
    # Upload the fMRI data and associate it with the experiment.
    result = api.experiments_fmri_create(experiment_id, upload_file)
    # Delete temporary directory
    shutil.rmtree(tmp_dir)
    # If fMRI is None the given experiment does not exist
    if result is None:
        raise ResourceNotFound(experiment_id + ':fmri')
    # Return result including a list of references to updated experiment
    return jsonify(result), 201


@app.route('/experiments/<string:experiment_id>/fmri', methods=['GET'])
def experiments_fmri_get(experiment_id):
    """Get functional MRI data (GET) - Retrieve a functional MRI data object
    from the database.
    """
    # Get experiments fMRI object from database. Raise exception if not fMRI
    # object is associated with the given experiment.
    fmri = api.experiments_fmri_get(experiment_id)
    if fmri is None:
        raise ResourceNotFound(experiment_id + ':fmri')
    else:
        return jsonify(fmri)


@app.route('/experiments/<string:experiment_id>/fmri', methods=['DELETE'])
def experiments_fmri_delete(experiment_id):
    """Delete experiment fMRI data (DELETE) - Delete fMRI data associated with
    an experiment object from the database.
    """
    # Delete experiments fMRI object with given identifier. Returns 204 if
    # experiment had fMRI data associated with it or 404 if result of delete is
    # None (by raising ResourceNotFound)
    if not api.experiments_fmri_delete(experiment_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(experiment_id + ':fmri')


@app.route('/experiments/<string:experiment_id>/fmri/file')
def experiments_fmri_download(experiment_id):
    """Download functional MRI data (GET) - Download data of previously uploaded
    functional MRI data.
    """
    # Get download information for experiments fMRI data object and send the
    # data file. Raises 404 exception if no data file is associated with the
    # requested resource (or the experiment does not exist).
    return download_file(
        api.experiments_fmri_download(experiment_id),
        experiment_id + ':fmri'
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
        if api.experiments_fmri_upsert_property(experiment_id, properties) is None:
            raise ResourceNotFound(experiment_id + ':fmri')
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Prediction Data
# ------------------------------------------------------------------------------

@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/attachments/<string:resource_id>', methods=['POST'])
def experiments_predictions_attachments_create(experiment_id, run_id, resource_id):
    """Create Attachment (POST) - Attach data file to a given model run.
    """
    # Get attached file that is associated with request
    tmp_dir, upload_file = get_upload_file(request)
    # Update state. If result is None (i.e., experiment of model run does not
    # exists) return 404. Otherwise, return 200. If a ValueError is raised the
    # intended update violates a valid model run time line.
    try:
        result = api.experiments_predictions_attachments_create(
            experiment_id,
            run_id,
            resource_id,
            upload_file
        )
        if result is None:
            shutil.rmtree(tmp_dir)
            raise ResourceNotFound(':'.join([experiment_id, run_id, resource_id]))
    except ValueError as ex:
        shutil.rmtree(tmp_dir)
        raise InvalidRequest(str(ex))
    # Clean-up and return success
    shutil.rmtree(tmp_dir)
    return jsonify(result), 200


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/attachments/<string:resource_id>', methods=['DELETE'])
def experiments_predictions_attachments_delete( experiment_id, run_id, resource_id):
    """Delete attachment (DELETE) - Delete attached file with given resource
    identifier from a mode run.
    """
    # Delete attached resource with given identifier. Returns 204 if resource
    # existed or 404 if result of delete is False
    if api.experiments_predictions_attachments_delete(experiment_id, run_id, resource_id):
        return '', 204
    else:
        raise ResourceNotFound(':'.join([experiment_id, run_id, resource_id]))


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/attachments/<string:resource_id>/file', methods=['GET'])
def experiments_predictions_attachments_download(experiment_id, run_id, resource_id):
    """Download attachment (GET) - Download data file that has been attached to
    a given model run.
    """
    # Get download information for model run result and send the file. Raises
    # 404 exception if the resource does not exists.
    return download_file(
        api.experiments_predictions_attachments_download(
            experiment_id,
            run_id,
            resource_id
        ),
        experiment_id + ':' + run_id + ':' + resource_id
    )


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
        api.experiments_predictions_list(
            experiment_id,
            limit=limit,
            offset=offset,
            properties=prop_set
        )
    )


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
    for key in ['name', 'model', 'arguments']:
        if not key in json_obj:
            raise InvalidRequest('missing element in Json body: ' + key)
    # Get dictionary of properties ifpresent in request
    if 'properties' in json_obj:
        properties = get_properties_list(json_obj['properties'], False)
    else:
        properties = None
    # Call create method of API. This will raise an exception if the model
    # does not exist or if running the model failed. The result is None if the
    # experiment does not exist.
    try:
        result = api.experiments_predictions_create(
            experiment_id,
            json_obj['model'],
            json_obj['name'],
            json_obj['arguments'],
            properties=properties
        )
        # The result is None if experiment does not exists
        if result is None:
            raise ResourceNotFound(experiment_id)
        # Return result including list of references for new model run.
        return jsonify(result), 201
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>', methods=['GET'])
def experiments_predictions_get(experiment_id, run_id):
    """Get prediction (GET) - Retrieve a model run and its prediction result
    for a given experiment.
    """
    # Get prediction object from database. Raise exception if prediction does
    # not exist.
    prediction = api.experiments_predictions_get(experiment_id, run_id)
    if prediction is None:
        raise ResourceNotFound(experiment_id + ':' + run_id)
    else:
        return jsonify(prediction)


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>', methods=['DELETE'])
def experiments_predictions_delete(experiment_id, run_id):
    """Delete prediction (DELETE) - Delete model run and potential prediction
    results associated with a given experiment.
    """
    # Delete prediction object with given identifier. Returns 204 if prediction
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not api.experiments_predictions_delete(experiment_id, run_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(experiment_id + ':' + run_id)


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/file')
def experiments_predictions_download(experiment_id, run_id):
    """Download prediction (GET) - Download prediction result generated by a
    successfully finished model run that is associated with a given experiment.
    """
    # Get download information for model run result and send the file. Raises
    # 404 exception if the resource does not exists.
    return download_file(
        api.experiments_predictions_download(experiment_id, run_id),
        experiment_id + ':' + run_id
    )


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/properties', methods=['POST'])
def experiments_predictions_upsert_property(experiment_id, run_id):
    """Upsert prediction (POST) - Upsert a property of a model run object
    associated with a given experiment.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert model run properties. The response indicates if the model run
    # exists. Will throw ValueError if property set results in illegal update.
    try:
        result = api.experiments_predictions_upsert_property(
            experiment_id,
            run_id,
            properties)
        if result is None:
            raise ResourceNotFound(run_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/state/active', methods=['POST'])
def experiments_predictions_update_state_active(experiment_id, run_id):
    """Update run state (POST) - Update the state of an existing model run
    to active. Does not expect a request body"""
    # Update state. If result is None (i.e., experiment of model run does not
    # exists) return 404. Otherwise, return 200. If a ValueError is raised the
    # intended update violates a valid model run time line.
    try:
        result = api.experiments_predictions_update_state_active(
            experiment_id,
            run_id
        )
        if result is None:
            raise ResourceNotFound(run_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/state/error', methods=['POST'])
def experiments_predictions_update_state_error(experiment_id, run_id):
    """Update run state (POST) - Update the state of an existing model run to
    failed. Expects a list of error messages in the request body."""
    # Get state object from request
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    if not 'errors' in json_obj:
        raise InvalidRequest('missing element: errors')
    # Update state. If result is None (i.e., experiment of model run does not
    # exists) return 404. Otherwise, return 200. If a ValueError is raised the
    # intended update violates a valid model run time line.
    try:
        result = api.experiments_predictions_update_state_error(
            experiment_id,
            run_id,
            json_obj['errors']
        )
        if result is None:
            raise ResourceNotFound(run_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/experiments/<string:experiment_id>/predictions/<string:run_id>/state/success', methods=['POST'])
def experiments_predictions_update_state_success(experiment_id, run_id):
    """Update run state (POST) - Update the state of an existing model run to
    success. Expects a result file in the message body."""
    # Get model result file that is associated with request
    tmp_dir, upload_file = get_upload_file(request)
    # Update state. If result is None (i.e., experiment of model run does not
    # exists) return 404. Otherwise, return 200. If a ValueError is raised the
    # intended update violates a valid model run time line.
    try:
        result = api.experiments_predictions_update_state_success(
            experiment_id,
            run_id,
            upload_file
        )
        if result is None:
            shutil.rmtree(tmp_dir)
            raise ResourceNotFound(run_id)
    except ValueError as ex:
        shutil.rmtree(tmp_dir)
        raise InvalidRequest(str(ex))
    # Clean-up and return success
    shutil.rmtree(tmp_dir)
    return '', 200


# ------------------------------------------------------------------------------
# Image Files
# ------------------------------------------------------------------------------

@app.route('/images/files')
def image_files_list():
    """List images (GET) - List of all image objects in the database."""
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate image file listing and return Json object
    return jsonify(
        api.image_files_list(limit=limit, offset=offset, properties=prop_set)
    )


@app.route('/images/files/<string:image_id>', methods=['GET'])
def image_files_get(image_id):
    """Get image (GET) - Retrieve an image object from the database."""
    # Get image file object from database. Raise exception if image does not
    # exist.
    img = api.image_files_get(image_id)
    if img is None:
        raise ResourceNotFound(image_id)
    else:
        return jsonify(img)


@app.route('/images/files/<string:image_id>', methods=['DELETE'])
def image_files_delete(image_id):
    """Delete image object (DELETE) - Delete an image object from the
    database.
    """
    # Delete image file object with given identifier. Returns 204 if image
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not api.image_files_delete(image_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(image_id)


@app.route('/images/files/<string:image_id>/file')
def image_files_download(image_id):
    """Download image file (GET)"""
    # Get download information for image and send the file. Raises 404 exception
    # if the image does not exists.
    return download_file(
        api.image_files_download(image_id),
        image_id,
        as_attachment=False
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
        if api.image_files_upsert_property(image_id, properties) is None:
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
        api.image_groups_list(limit=limit, offset=offset, properties=prop_set)
    )


@app.route('/images/groups/options')
def image_groups_options():
    """List image group options (GET) - List of all supported image group
    options."""
    return jsonify(
        api.image_groups_options()
    )


@app.route('/images/groups/<string:image_group_id>', methods=['GET'])
def image_groups_get(image_group_id):
    """Get image group (GET) - Retrieve an image group from the database."""
    # Get image group object from database. Raise exception if image group does
    # not exist.
    img_grp = api.image_groups_get(image_group_id)
    if img_grp is None:
        raise ResourceNotFound(image_group_id)
    else:
        return jsonify(img_grp)


@app.route('/images/groups/<string:image_group_id>', methods=['DELETE'])
def image_groups_delete(image_group_id):
    """Delete image group (DELETE) - Delete an image group object from the
    database.
    """
    # Delete image group object with given identifier. Returns 204 if image
    # group existed or 404 if result of delete is None (by raising
    # ResourceNotFound)
    if not api.image_groups_delete(image_group_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(image_group_id)


@app.route('/images/groups/<string:image_group_id>/images')
def image_groups_images_list(image_group_id):
    """List image group images (GET)"""
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type. Property set is ignored since group images have no
    # additional properties
    offset, limit, prop_set = get_listing_arguments(request)
    # Get group image listing. Return 404 if result is None, i.e., image group
    # is unknown
    listing = api.image_groups_images_list(
        image_group_id,
        limit=limit,
        offset=offset
    )
    if listing is None:
        raise ResourceNotFound(image_group_id)
    return jsonify(listing)


@app.route('/images/groups/<string:image_group_id>/file')
def image_groups_download(image_group_id):
    """Download image group file (GET)"""
    # Get download information for image group and send the group archive file.
    # Raises 404 exception if the image group does not exists.
    return download_file(
        api.image_groups_download(image_group_id),
        image_group_id
    )


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
    # Upsert object options. The result will be None if image group does not
    # exist.
    try:
        result = api.image_groups_update_options(
            image_group_id,
            json_obj['options']
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if result is None:
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
        if api.image_groups_upsert_property(image_group_id, properties) is None:
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
        result =  api.images_create(upload_file)
    except ValueError as err:
        # Make sure to delete temporary file before raising InvalidRequest
        shutil.rmtree(tmp_dir)
        raise InvalidRequest(str(err))
    # Clean up and return success.
    shutil.rmtree(tmp_dir)
    return jsonify(result), 201


# ------------------------------------------------------------------------------
# Models
# ------------------------------------------------------------------------------

@app.route('/models')
def models_list():
    """List models (GET) - Get a list of all regostered predictive model.
    """
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate prediction listing and return Json object
    return jsonify(
        api.models_list(limit=limit, offset=offset, properties=prop_set)
    )


@app.route('/models', methods=['POST'])
def models_register():
    """Register model (POST) - Register a given predictive model with the
    worklow engine.
    """
    # Make sure that the post request has a json part
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # Make sure that all required keys are present in the given Json object
    for key in ['id', 'properties', 'parameters', 'outputs', 'connector']:
        if not key in json_obj:
            raise InvalidRequest('missing element in Json body: ' + key)
    # Call API method to create a new experiment object
    try:
        result = api.models_register(
            json_obj['id'],
            get_properties_list(json_obj['properties'], True),
            json_obj['parameters'],
            json_obj['outputs'],
            json_obj['connector']
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    # Return result including list of references for new experiment
    return jsonify(result), 201


@app.route('/models/<string:model_id>', methods=['GET'])
def models_get(model_id):
    """Get model (GET) - Retrieve a predictive model definition from the
    model repository.
    """
    # Get model from database. Raise exception if model does not exist.
    model = api.models_get(model_id)
    if model is None:
        raise ResourceNotFound(model_id)
    else:
        return jsonify(model)


@app.route('/models/<string:model_id>', methods=['DELETE'])
def models_delete(model_id):
    """Delete model (DELETE) - Delete an existing model from the registry.
    """
    # Get model from database. Raise exception if model does not exist.
    model = api.models_get(model_id)
    if not api.models_delete(model_id) is None:
        return '', 204
    raise ResourceNotFound(model_id)


@app.route('/models/<string:model_id>/properties', methods=['POST'])
def models_upsert_property(model_id):
    """Upsert model property (POST) - Upsert a property of a model
    object in the database.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert experiment properties. The response indicates if the experiment
    # exists. Will throw ValueError if property set results in illegal update.
    try:
        if api.models_upsert_property(model_id, properties) is None:
            raise ResourceNotFound(model_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


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
        api.subjects_list(limit=limit, offset=offset, properties=prop_set)
    )


@app.route('/subjects/<string:subject_id>', methods=['GET'])
def subjects_get(subject_id):
    """Get subject (GET) - Retrieve a brain anatomy MRI object from the
    database.
    """
    # Get subject from database. Raise exception if subject does not exist.
    subject = api.subjects_get(subject_id)
    if subject is None:
        raise ResourceNotFound(subject_id)
    else:
        return jsonify(subject)


@app.route('/subjects', methods=['POST'])
def subjects_create():
    """Upload Subject (POST) - Upload an brain anatomy MRI archive file."""
    # Upload the given file to get an object handle for new subject. Method
    # throws InvalidRequest exception if necessary.
    tmp_dir, upload_file = get_upload_file(request)
    try:
        result = api.subjects_create(upload_file)
    except ValueError as ex:
        # Make sure to clean up and raise InvalidRequest exception
        shutil.rmtree(tmp_dir)
        raise InvalidRequest(str(ex))
    # Delete temp folder and return success.
    shutil.rmtree(tmp_dir)
    return jsonify(result), 201


@app.route('/subjects/<string:subject_id>', methods=['DELETE'])
def subjects_delete(subject_id):
    """Delete Subject (DELETE) - Delete a brain anatomy MRI object from the
    database.
    """
    # Delete subject data object with given identifier. Returns 204 if subject
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not api.subjects_delete(subject_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(subject_id)


@app.route('/subjects/<string:subject_id>/file')
def subject_download(subject_id):
    """Download subject (GET) - Download data of previously uploaded subject
    anatomy.
    """
    # Get download information for subject and send the subject archive file.
    # Raises 404 exception if the subject does not exists.
    return download_file(
        api.subjects_download(subject_id),
        subject_id
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
        if api.subjects_upsert_property(subject_id, properties) is None:
            raise ResourceNotFound(subject_id)
        else:
            return '', 200
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Widgets
# ------------------------------------------------------------------------------

@app.route('/widgets')
def widgets_list():
    """List widgets (GET) - List of widgets that are in the database.
    """
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(request)
    # Decorate subject listing and return Json object
    return jsonify(
        api.widgets_list(limit=limit, offset=offset, properties=prop_set)
    )


@app.route('/widgets/<string:widget_id>', methods=['GET'])
def widgets_get(widget_id):
    """Get widget (GET) - Retrieve a visualization widget from the database.
    """
    # Get widget from database. Raise exception if widget does not exist.
    widget = api.widgets_get(widget_id)
    if widget is None:
        raise ResourceNotFound(widget_id)
    else:
        return jsonify(widget)


@app.route('/widgets', methods=['POST'])
def widgets_create():
    """Create widget (POST) - Create a new visualizaion widget."""
    # Make sure that the post request has a json part
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # Make sure that all required keys are present in the given Json object
    for key in ['engine', 'code', 'inputs', 'properties']:
        if not key in json_obj:
            raise InvalidRequest('missing element in Json body: ' + key)
    # Call API method to create a new widget object
    try:
        result = api.widgets_create(
            json_obj['engine'],
            json_obj['code'],
            json_obj['inputs'],
            get_properties_list(json_obj['properties'], True)
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    # Return result including list of references for new experiment
    return jsonify(result), 201


@app.route('/widgets/<string:widget_id>', methods=['DELETE'])
def widgets_delete(widget_id):
    """Delete widget (DELETE) - Delete a visualization widget from the
    database.
    """
    # Delete widget data object with given identifier. Returns 204 if widget
    # existed or 404 if result of delete is None (by raising ResourceNotFound)
    if not api.widgets_delete(widget_id) is None:
        return '', 204
    else:
        raise ResourceNotFound(widget_id)


@app.route('/widgets/<string:widget_id>', methods=['POST'])
def widgets_update(widget_id):
    """Update widget (POST) - Update code and/or input descriptors for a widget
    in the database."""
    # Make sure that the post request has a json part
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # code and inputs are optional elements in the request
    if 'code' in json_obj:
        code = json_obj['code']
    else:
        code = None
    if 'inputs' in json_obj:
        inputs = json_obj['inputs']
    else:
        inputs = None
    # Call API method to create a new widget object
    try:
        result = api.widgets_update(widget_id, code=code, inputs=inputs)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    # Return updated widget descriptor
    return jsonify(result), 200


@app.route('/widgets/<string:widget_id>/inputs', methods=['POST'])
def widgets_add_input_descriptor(widget_id):
    """Update widget inputs (POST) - Add an input descriptor for a visualization
    widget in the database."""
    # Make sure that the post request has a json part
    if not request.json:
        raise InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # Call API method to create a new widget object
    try:
        result = api.widgets_add_input_descriptor(
            widget_id,
            json_obj
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    # Return updated widget descriptor
    return jsonify(result), 200


@app.route('/widgets/<string:widget_id>/properties', methods=['POST'])
def widgets_upsert_property(widget_id):
    """Upsert widget properties (POST) - Upsert a property of a visualization
    widget in the database.
    """
    # Extract dictionary of key,value-pairs from request.
    properties = get_upsert_properties(request)
    # Upsert widget properties. The response indicates if the widget exists.
    # Will throw ValueError if property set results in illegal update.
    try:
        if api.widgets_upsert_property(widget_id, properties) is None:
            raise ResourceNotFound(widget_id)
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

def download_file(file_info, identifier, as_attachment=True):
    """Send content of a given file that is associated associated with a data
    store resource.

    Parameters
    ----------

    file_info : FileInfo
        Information about file on disk or None if requested resource does not
        exist
    identifier : string
        Identifier of requested resource
    as_attachment : bool
        Flag indicating whether to send the file as attachment or not
    """
    # Raise 404 exception if resource does not exists, i.e., file info object
    # is None
    if file_info is None:
        raise ResourceNotFound(identifier)
    # Send file in the object's upload folder
    return send_file(
        file_info.file,
        mimetype=file_info.mime_type,
        as_attachment=as_attachment,
        attachment_filename=file_info.name
    )


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
    run_simple('0.0.0.0', SERVER_PORT, application, use_reloader=app.config['DEBUG'])
