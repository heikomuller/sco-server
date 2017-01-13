#!venv/bin/python
import os

from flask import Flask, jsonify, make_response, request, send_file, send_from_directory
from flask_cors import CORS

import reqexcpt as exceptions
import hateoas
import db.api


# -----------------------------------------------------------------------------
#
# App Configuration
#
# -----------------------------------------------------------------------------

# App Path and Url
APP_PATH = '/sco-server/api/v1'
SERVER_URL = 'http://localhost'
SERVER_PORT = 5050
BASE_URL = SERVER_URL + ':' + str(SERVER_PORT) + APP_PATH + '/'
# Flag to switch debugging on/off
DEBUG = True
# Local folder for data files
DATA_DIR = '../resources/data'
# Log file
LOG_FILE = os.path.abspath(DATA_DIR + 'scoserv.log')
# MongoDB database
MONGO_DB = MongoClient().scoserv


# ------------------------------------------------------------------------------
# Initialization
# ------------------------------------------------------------------------------

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = APP_PATH
app.config['DEBUG'] = DEBUG
CORS(app)

# Instantiate the Standard Cortical Observer Data Store.
db = ad.api.SCODataStore(MONGO_DB, DATA_DIR)
# Factory for object references
refs = hateoas.HATEOASReferenceFactory(BASE_URL)


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
    return jsonify({
        'name': 'Standard Cortical Observer - Web Server API',
        'links' : refs.service_references()
    })


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
    offset, limit, prop_set = get_listing_arguments(reques)
    # Decorate experiment listing and return Json object
    return jsonify(
        refs.decorate_listing(
            db.experiments_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/experiments/<string:experiment_id>', methods=['GET'])
def experiments_get(experiment_id):
    """Get experiment (GET) - Retrieve an experiment object from the database.
    """
    # Return Json serialization of object. The decorate_object method throws
    # ResourceNotFound excpetion if the given object identifier does not
    # reference an existing experiment.
    return jsonify(
        refs.decorate_object(
            db.experiments_get(experiment_id)
        )
    )


@app.route('/experiments', methods=['POST'])
def experiments_create():
    """Create experiment (POST) - Create a new experiment object.
    """
    # Make sure that the post request has a json part
    if not request.json:
        raise exceptions.InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # Make sure that all required keys are present in the given Json object
    for key in ['name', 'subject', 'images']:
        if not key in json_obj:
            raise exceptions.InvalidRequest('missing element in Json body: ' + key)
    # Call API method to create a new experiment object
    experiment = api.experiments_create(
        json_obj['name'],
        datastore.ObjectId(json_obj['subject']),
        datastore.ObjectId(json_obj['images'])
    )
    # Return result including list of references for new experiment
    return jsonify({
        'result' : 'SUCCESS',
        'links': refs.object_references(experiment)
    }), 201


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
        raise excpt.ResourceNotFound(experiment_id)


@app.route('/experiments/<string:experiment_id>/properties', methods=['POST'])
def experiments_upsert_property(experiment_id):
    """Upsert experiment property (POST) - Upsert a property of an experiment
    object in the database.
    """
    # Extract key,value-pair for upsert property from request.
    key, value = get_upsert_property(request)
    # Upsert the object property. The response code indicates whether the
    # property was created, updated, or deleted. Method raises InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    return '', get_upsert_response(
        db.experiments_upsert_property(experiment_id, key, value=value),
        object_id,
        key,
        value
    )


# ------------------------------------------------------------------------------
# Functional Data
# ------------------------------------------------------------------------------

@app.route('/experiments/<string:experiment_id>/fmri', methods=['GET'])
def experiments_fmri_get(experiment_id):
    """Get functional MRI data (GET) - Retrieve a functional MRI data object
    from the database.
    """
    # Return Json serialization of object. The decorate_object method throws
    # ResourceNotFound excpetion if the given object identifier does not
    # reference an existing experiments fMRI data object.
    return jsonify(
        refs.decorate_object(
            db.experiments_fmri_get(experiment_id)
        )
    )


@app.route('/experiments/<string:experiment_id>/fmri', methods=['POST'])
def upload_experiment_fmri(experiment_id):
    """Upload functional MRI data (POST) - Upload a functional MRI data archive
    file that is associated with a experiment.
    """
    # Get the uploaded file. Method raises InvalidRequest if no file was given
    upload_file = get_upload_file(request)
    # Upload the fMRI data and associate it with the experiment. Method will
    # raise InvalidRequest or ResourceNotFound exceptions.
    experiment = sco.upload_experiment_fmri(
        datastore.ObjectId(experiment_id),
        upload_file
    )
    # Return result including a list of references to updated experiment
    return jsonify({
        'result' : 'SUCCESS',
        'links': sco.refs.object_references(experiment)
    })


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
        raise excpt.ResourceNotFound(experiment_id + ':fmri')


@app.route('/experiments/<string:experiment_id>/fmri/data')
def download_fmri(experiment_id):
    """Download functional MRI data (GET) - Download data of previously uploaded
    functional MRI data.
    """
    # Get download information for given object. Method raises ResourceNotFound
    # exception if object does not exists or does not have any downloadable
    # representation.
    directory, filename, mime_type = sco.get_download(
        datastore.ObjectId([experiment_id, fmri_id]),
        datastore.OBJ_FMRI_DATA
    )
    # Send file in the object's upload folder
    return send_from_directory(
        directory,
        filename,
        mimetype=mime_type,
        as_attachment=True,
        attachment_filename=filename
    )


@app.route('/experiments/<string:experiment_id>/fmri/properties', methods=['POST'])
def experiments_fmri_upsert_property(experiment_id):
    """Upsert functional MRI data object (POST) - Upsert a property of a
    functional MRI data object in the database.
    """
    # Extract key,value-pair for upsert property from request.
    key, value = get_upsert_property(request)
    # Upsert the object property. The response code indicates whether the
    # property was created, updated, or deleted. Method raises InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    return '', get_upsert_response(
        db.experiments_fmri_upsert_property(experiment_id, key, value=value),
        object_id,
        key,
        value
    )


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
    offset, limit, prop_set = get_listing_arguments(reques)
    # Decorate prediction listing and return Json object
    return jsonify(
        refs.decorate_listing(
            db.experiments_predictions_list(
                experiment_id,
                limit=limit,
                offset=offset),
            prop_set
        )
    )


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>', methods=['GET'])
def experiments_predictions_get(experiment_id, prediction_id):
    """Get prediction (GET) - Retrieve a model run and its prediction result
    for a given experiment.
    """
    # Return Json serialization of object. The decorate_object method throws
    # ResourceNotFound excpetion if the given object identifier does not
    # reference an existing prediction.
    return jsonify(
        refs.decorate_object(
            db.experiments_predictions_get(experiment_id, prediction_id)
        )
    )


@app.route('/experiments/<string:experiment_id>/predictions', methods=['POST'])
def create_experiment_prediction(experiment_id):
    """Create model run (POST) - Start a new model run for an experiment using a
    user provided set of arguments.
    """
    # Make sure that the post request has a json part
    if not request.json:
        raise exceptions.InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    # Make sure that all required keys are present in the given Json object
    for key in ['name', 'arguments']:
        if not key in json_obj:
            raise exceptions.InvalidRequest('missing element in Json body: ' + key)
    # Call create method of API to get a new model run object handle.
    prediction = sco.create_prediction(
        datastore.ObjectId(experiment_id),
        json_obj['name'],
        get_attributes(json_obj['arguments'])
    )
    # Return result including list of references for new model run.
    return jsonify({
        'result' : 'SUCCESS',
        'links': sco.refs.object_references(prediction)
    }), 201


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
        raise excpt.ResourceNotFound(experiment_id + ':' + prediction_id)


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>/data')
def download_experiment_prediction(experiment_id, prediction_id):
    """Download prediction (GET) - Download prediction result generated by a
    successfully finished model run that is associated with a given experiment.
    """
    # Get download information for given object. Method raises ResourceNotFound
    # exception if object does not exists or does not have any downloadable
    # representation.
    directory, filename, mime_type = sco.get_download(
        datastore.ObjectId([experiment_id, prediction_id]),
        datastore.OBJ_PREDICTION
    )
    # Send file in the object's upload folder
    return send_from_directory(
        directory,
        filename,
        mimetype=mime_type,
        as_attachment=False,
        attachment_filename=filename
    )


@app.route('/experiments/<string:experiment_id>/predictions/<string:prediction_id>/properties', methods=['POST'])
def experiments_predictions_upsert_property(experiment_id, prediction_id):
    """Upsert prediction (POST) - Upsert a property of a model run object
    associated with a given experiment.
    """
    # Extract key,value-pair for upsert property from request.
    key, value = get_upsert_property(request)
    # Upsert the object property. The response code indicates whether the
    # property was created, updated, or deleted. Method raises InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    return '', get_upsert_response(
        db.experiments_predictions_upsert_property(
            experiment_id,
            prediction_id,
            key,
            value=value),
        object_id,
        key,
        value
    )


# ------------------------------------------------------------------------------
# Images
# ------------------------------------------------------------------------------

@app.route('/images/files')
def image_files_list():
    """List images (GET) - List of all image objects in the database."""
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(reques)
    # Decorate image file listing and return Json object
    return jsonify(
        refs.decorate_listing(
            db.image_files_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/images/files/<string:image_id>', methods=['GET'])
def image_files_get(image_id):
    """Get image (GET) - Retrieve an image object from the database."""
    # Return Json serialization of object. The decorate_object method throws
    # ResourceNotFound excpetion if the given object identifier does not
    # reference an existing image file object.
    return jsonify(
        refs.decorate_object(
            db.image_files_get(image_id)
        )
    )


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
        raise excpt.ResourceNotFound(image_id)


@app.route('/images/files/<string:image_id>/data')
def download_image(image_id):
    """Download image file (GET)"""
    # Get download information for given object. Method raises ResourceNotFound
    # exception if object does not exists or does not have any downloadable
    # representation.
    directory, filename, mime_type = sco.get_download(
        datastore.ObjectId(image_id),
        datastore.OBJ_IMAGE
    )
    # Send file in the object's upload folder
    return send_from_directory(
        directory,
        filename,
        mimetype=mime_type,
        as_attachment=False,
        attachment_filename=filename
    )


@app.route('/images/files/<string:image_id>/properties', methods=['POST'])
def image_files_upsert_property(image_id):
    """Upsert image object (POST) - Upsert a property of an image object in the
    database.
    """
    # Extract key,value-pair for upsert property from request.
    key, value = get_upsert_property(request)
    # Upsert the object property. The response code indicates whether the
    # property was created, updated, or deleted. Method raises InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    return '', get_upsert_response(
        db.image_files_upsert_property(image_id, key, value=value),
        object_id,
        key,
        value
    )


# ------------------------------------------------------------------------------
# Image Groups
# ------------------------------------------------------------------------------

@app.route('/images/groups')
def image_groups_list():
    """List image groups (GET) - List of all image group objects in the database."""
    # Get listing arguments. Method raises exception if argument values are
    # of invalid type
    offset, limit, prop_set = get_listing_arguments(reques)
    # Decorate image group listing and return Json object
    return jsonify(
        refs.decorate_listing(
            db.image_groups_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/images/groups/<string:image_group_id>', methods=['GET'])
def image_groups_get(image_group_id):
    """Get image group (GET) - Retrieve an image group from the database."""
    # Return Json serialization of object. The decorate_object method throws
    # ResourceNotFound excpetion if the given object identifier does not
    # reference an existing image group.
    return jsonify(
        refs.decorate_object(
            db.image_groups_get(image_group_id)
        )
    )


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
        raise excpt.ResourceNotFound(image_group_id)


@app.route('/images/groups/<string:image_group_id>/data')
def download_image_group(image_group_id):
    """Download image group file (GET)"""
    # Get download information for given object. Method raises ResourceNotFound
    # exception if object does not exists or does not have any downloadable
    # representation.
    directory, filename, mime_type = sco.get_download(
        datastore.ObjectId(image_group_id),
        datastore.OBJ_IMAGEGROUP
    )
    # Send file in the object's upload folder
    return send_from_directory(
        directory,
        filename,
        mimetype=mime_type,
        as_attachment=True,
        attachment_filename=filename
    )


@app.route('/images/groups/<string:image_group_id>/options', methods=['POST'])
def update_image_group_options(image_group_id):
    """Upsert image group options (POST) - Upsert the options that are
    associated with an image group in the database. Given that these options
    cannot be included in the file upload, there has to be a separate API call.
    """
    # Make sure that the request contains a Json body with an 'options' element
    if not request.json:
        raise exceptions.InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    if not 'options' in json_obj:
        raise exceptions.InvalidRequest('missing element in Json body: options')
    # Convert the Json element associated with key 'options' into a list of
    # typed property instance. Throws an InvalidRequest exception if the format
    # of the element value is invalid.
    attributes = get_attributes(json_obj['options'])
    # Upsert object options. Method will raise InvalidRequest or
    # ResourceNotFound exceptions if necessary.
    sco.update_object_attributes(
        datastore.ObjectId(image_group_id),
        datastore.OBJ_IMAGEGROUP,
        attributes
    )
    return '', 200


@app.route('/images/groups/<string:image_group_id>/properties', methods=['POST'])
def image_groups_upsert_property(image_group_id):
    """Upsert image object (POST) - Upsert a property of an image group in the
    database.
    """
    # Extract key,value-pair for upsert property from request.
    key, value = get_upsert_property(request)
    # Upsert the object property. The response code indicates whether the
    # property was created, updated, or deleted. Method raises InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    return '', get_upsert_response(
        db.image_groups_upsert_property(image_group_id, key, value=value),
        object_id,
        key,
        value
    )


# ------------------------------------------------------------------------------
# Upload Images
# ------------------------------------------------------------------------------

@app.route('/images/upload', methods=['POST'])
def upload_images():
    """Upload Images (POST) - Upload an image file or an archive of images."""
    # Upload the file and an image file or image group. Method will throw
    # InvalidRequest exception if necessary.
    img_handle =  sco.upload_file(datastore.OBJ_IMAGE, get_upload_file(request))
    # Return result including list of references for the new database object.
    return jsonify({
        'result' : 'SUCCESS',
        'links': sco.refs.object_references(img_handle)
    })


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
    offset, limit, prop_set = get_listing_arguments(reques)
    # Decorate subject listing and return Json object
    return jsonify(
        refs.decorate_listing(
            db.subjects_list(limit=limit, offset=offset),
            prop_set
        )
    )


@app.route('/subjects/<string:subject_id>', methods=['GET'])
def subjects_get(subject_id):
    """Get subject (GET) - Retrieve a brain anatomy MRI object from the
    database.
    """
    # Return Json serialization of object. The decorate_object method throws
    # ResourceNotFound excpetion if the given object identifier does not
    # reference an existing subject.
    return jsonify(
        refs.decorate_object(
            db.subjects_get(subject_id)
        )
    )


@app.route('/subjects', methods=['POST'])
def upload_subject():
    """Upload Subject (POST) - Upload an brain anatomy MRI archive file."""
    # Upload the given file to get an object handle for new subject. Method
    # throws InvalidRequest exception if necessary.
    subject =  sco.upload_file(datastore.OBJ_SUBJECT, get_upload_file(request))
    # Return result including a list of references to new subject in database.
    return jsonify({
        'result' : 'SUCCESS',
        'links': sco.refs.object_references(subject)
    })


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
        raise excpt.ResourceNotFound(subject_id)


@app.route('/subjects/<string:subject_id>/data')
def download_subject(subject_id):
    """Download subject (GET) - Download data of previously uploaded subject
    anatomy.
    """
    # Get download information for given object. Method raises ResourceNotFound
    # exception if object does not exists or does not have any downloadable
    # representation.
    directory, filename, mime_type = sco.get_download(
        datastore.ObjectId(subject_id),
        datastore.OBJ_SUBJECT
    )
    print directory
    print filename
    # Send file in the object's upload folder
    return send_from_directory(
        directory,
        filename,
        mimetype=mime_type,
        as_attachment=True,
        attachment_filename=filename
    )


@app.route('/subjects/<string:subject_id>/properties', methods=['POST'])
def subjects_upsert_property(subject_id):
    """Upsert subject object (POST) - Upsert a property of a brain anatomy MRI
    object in the database.
    """
    # Extract key,value-pair for upsert property from request.
    key, value = get_upsert_property(request)
    # Upsert the object property. The response code indicates whether the
    # property was created, updated, or deleted. Method raises InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    return '', get_upsert_response(
        db.subjects_upsert_property(subject_id, key, value=value),
        object_id,
        key,
        value
    )


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
        raise exceptions.InvalidRequest('argument is not a list')
    # Iterate over all elements in the list. Make sure they are Json objects
    # with 'name' and 'value' elements
    for element in json_array:
        if not isinstance(element, dict):
            raise exceptions.InvalidRequest('element is not a dictionary')
        for key in ['name', 'value']:
            if not key in element:
                raise exceptions.InvalidRequest('object has not key ' + key)
        name = str(element['name'])
        value = element['value']
        result.append(datastore.Attribute(name, value))
    return result


def get_listing_arguments(request):
    """Extract listing arguments from given request. Returns default values
    for parameters not present in the request.

    Parameters
    ----------
    request : flask.request
        Flask request object

    Returns
    -------
    (int, int, [string])
        Tuple of offset, limit, properties
    """
    # Check if limit and offset parameters are given. Throws ValueError if
    # values are not integer.
    try:
        offset = int(request.args[hateoas.QPARA_OFFSET]) if hateoas.QPARA_OFFSET in request.args else 0
        limit = int(request.args[hateoas.QPARA_LIMIT]) if hateoas.QPARA_LIMIT in request.args else -1
    except ValueError as err:
        raise exceptions.InvalidRequest(str(err))
    # Get the set of attributes that are included in the result listing. By
    # default, the object name is always included.
    prop_set = request.args[hateoas.QPARA_PROPERTIES].split(',') if hateoas.QPARA_PROPERTIES in request.args else None
    return offset, limit, prop_set


def get_upsert_property(request):
    """Extract key,value-pair defining the property and it's new value that are
    to be updated.

    Parameters
    ----------
    request : flask.request
        Flask request object

    Returns
    -------
    (string, string) : (key, value)
        Key,value-pair describing property update
    """
    # Abort with 400 if the request is not a Json request or if the Json object
    # in the request does not have field key. Field value is optional.
    if not request.json:
        raise exceptions.InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    if not 'key' in json_obj:
        raise exceptions.InvalidRequest('missing element in Json body: key')
    # Get key and new value (if given) of property to update
    key = json_obj['key']
    value = json_obj['value'] if 'value' in json_obj else None
    return key, value


def get_upsert_response(state, object_id, key, value):
    """Get request response code for property upsert operation status code.
    Method raises an exception if the given state singals that the upsert has
    not been successful.

    Parameters
    ----------
    state : int
        Property upsert result
    object_id : string
        Unique object identifier
    key : string
        Updated property
    value : string
        New property value

    Returns
    -------
        Http response code
    """
    if state == datastore.OP_ILLEGAL:
        raise excpt.InvalidRequest(
            'illegal upsert: ' + object_id + '.' + str(key) + '=' + str(value)
        )
    elif state == datastore.OP_CREATED:
        return 201
    elif state == datastore.OP_DELETED:
        return 204
    elif state == datastore.OP_UPDATED:
        return 200
    else: # -1
        # This point should never be reached, unless object has been deleted
        # concurrently.
        raise excpt.ResourceNotFound(object_id)


def get_upload_file(request):
    """Generalized method for file uploads. Ensures that request contains a
    file and returns the file. Raise InvalidRequest exception if request does
    not contain an uploded file.

    Parameters
    ----------
    request : flask.request
        Flask request object

    Returns
    -------
    File Object
        File object in upload request.
    """
    # Make sure that the post request has the file part
    if 'file' not in request.files:
        raise exceptions.InvalidRequest('no file argument in request')
    file = request.files['file']
    # A browser may submit a empty part without filename
    if file.filename == '':
        raise exceptions.InvalidRequest('empty file name')
    # Call upload method of API
    return file


# ------------------------------------------------------------------------------
# Error Handler
# ------------------------------------------------------------------------------

@app.errorhandler(exceptions.APIRequestException)
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
