#!venv/bin/python
import getopt
import os
import sys

from flask import Flask, jsonify, make_response, request, send_file, send_from_directory
from flask_cors import CORS

import api
import reqexcpt as exceptions
import hateoas
import db.datastore as datastore


# -----------------------------------------------------------------------------
#
# App Configuration
#
# -----------------------------------------------------------------------------

APP_PATH = '/sco-server/api/v1'
SERVER_URL = 'http://localhost'
SERVER_PORT = 5050

# Flag to swith debugging on/off
DEBUG = True
# Local folders
DATA_DIR = '../resources/data'
# Log file
LOG_FILE = os.path.abspath(DATA_DIR + 'scoserv.log')

# ------------------------------------------------------------------------------
# Parse command line arguments
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    command_line = """
    Usage:
    [-a | --path <app-path>]
    [-d | --data <directory>]
    [-p | --port <port-number>]
    [-s | --server <server-url>]
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'a:d:p:s:', 'data=,port=0,server=')
    except getopt.GetoptError:
        print command_line
        sys.exit()

    if len(args) != 0:
        print command_line
        sys.exit()

    for opt, param in opts:
        if opt in ('-a', '--path'):
            APP_PATH = param
            if not APP_PATH.startswith('/'):
                print 'Invalid application path: ' + APP_PATH
                sys.exit()
            if APP_PATH.endswith('/'):
                APP_PATH = APP_PATH[:-1]
        elif opt in ('-d', '--data'):
            DATA_DIR = param
        elif opt in ('-p', '--port'):
            try:
                PORT = int(param)
            except ValueError:
                print 'Invalid port number: ' + param
                sys.exit()
        elif opt in ('-s', '--server'):
            SERVER_URL = param


# Base URL used as prefix for all HATEOAS URL's
if SERVER_PORT != 80:
    BASE_URL = SERVER_URL + ':' + str(SERVER_PORT) + APP_PATH + '/'
else:
    BASE_URL = SERVER_URL + APP_PATH + '/'

# ------------------------------------------------------------------------------
# Initialization
# ------------------------------------------------------------------------------

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = APP_PATH
app.config['DEBUG'] = DEBUG
CORS(app)

# Instantiate the URL factory
urls = hateoas.UrlFactory(BASE_URL)
# Instantiate the Standard Cortical Observer Data Server. Currently, we uses
# the default MongoDB client connection.
sco = api.DataServer(DATA_DIR, urls)
# Factory for object references
refs = hateoas.HATEOASReferenceFactory(urls)


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
def list_experiments():
    """List experiments data (GET) - List of all experiment objects in the
    database.
    """
    # Method raises exception if request argument values are not integers
    return jsonify(list_objects(request, datastore.OBJ_EXPERIMENT))


@app.route('/experiments', methods=['POST'])
def create_experiment():
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
    # Call upload method of API to get a new experiment object handle.
    experiment = sco.create_experiment(
        json_obj['name'],
        datastore.ObjectId(json_obj['subject']),
        datastore.ObjectId(json_obj['images'])
    )
    # Return result including list of references for new experiment.
    return jsonify({
        'result' : 'SUCCESS',
        'links': sco.refs.object_references(experiment)
    }), 201


@app.route('/experiments/<string:experiment_id>', methods=['GET'])
def get_experiment(experiment_id):
    """Get experiment (GET) - Retrieve an experiment object from the database.
    """
    # Return Json serialization of object. The API throws ResourceNotFound
    # excpetion if the given object identifier does not reference an existing
    # experiment.
    return jsonify(sco.get_object(
        datastore.ObjectId(experiment_id),
        datastore.OBJ_EXPERIMENT)
    )


@app.route('/experiments/<string:experiment_id>', methods=['DELETE'])
def delete_experiment(experiment_id):
    """Delete experiment (DELETE) - Delete an experiment object from the
    database.
    """
    # Delete experiment object with given identifier and return 204. The API
    # will throw ResourceNotFound exception if the given identifier does not
    # reference an existing experiment.
    sco.delete_object(
        datastore.ObjectId(experiment_id),
        datastore.OBJ_EXPERIMENT
    )
    return '', 204


@app.route('/experiments/<string:experiment_id>/properties', methods=['POST'])
def upsert_experiment_property(experiment_id):
    """Upsert experiment property (POST) - Upsert a property of an experiment
    object in the database.
    """
    # Upsert the object property. If response code indicates whether the
    # property was created, updated, or deleted. Method throws InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    code = upsert_object_property(
        request,
        datastore.ObjectId(experiment_id),
        datastore.OBJ_EXPERIMENT
    )
    return '', code


# ------------------------------------------------------------------------------
# Functional Data
# ------------------------------------------------------------------------------

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


@app.route('/experiments/<string:experiment_id>/fmri/<string:fmri_id>', methods=['GET'])
def get_fmri(experiment_id, fmri_id):
    """Get functional MRI data (GET) - Retrieve a functional MRI data object
    from the database.
    """
    # Return Json serialization of object. The API throws ResourceNotFound
    # excpetion if the given object identifier does not reference an existing
    # functional MRI data object.
    return jsonify(sco.get_object(
        datastore.ObjectId([experiment_id, fmri_id]),
        datastore.OBJ_FMRI_DATA
    ))


@app.route('/experiments/<string:experiment_id>/fmri/<string:fmri_id>/properties', methods=['POST'])
def upsert_fmri_property(experiment_id, fmri_id):
    """Upsert functional MRI data object (POST) - Upsert a property of a
    functional MRI data object in the database.
    """
    # The object identifier is a tuple of experiment and fmri identifier
    object_id = (experiment_id, fmri_id)
    # Upsert the object property. If response code indicates whether the
    # property was created, updated, or deleted. Method throws InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    code = upsert_object_property(
        request,
        datastore.ObjectId([experiment_id, fmri_id]),
        datastore.OBJ_FMRI_DATA
    )
    return '', code


@app.route('/experiments/<string:experiment_id>/fmri/<string:fmri_id>/data')
def download_fmri(experiment_id, fmri_id):
    """Download functional MRI data (GET) - Download data of previously uploaded
    functional MRI data.
    """
    # Get download information for given object. Method raises ResourceNotFound
    # exception if object does not exists or does not have any downloadable
    # representation.
    print 'DOWNLOAD'
    directory, filename, mime_type = sco.get_download(
        datastore.ObjectId([experiment_id, fmri_id]),
        datastore.OBJ_FMRI_DATA
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


# ------------------------------------------------------------------------------
# Prediction Data
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# Images
# ------------------------------------------------------------------------------

@app.route('/images/files')
def list_images():
    """List images (GET) - List of all image objects in the database."""
    # Method raises exception if request argument values are not integers
    return jsonify(list_objects(request, datastore.OBJ_IMAGE))


@app.route('/images/files/<string:image_id>', methods=['GET'])
def get_image(image_id):
    """Get image (GET) - Retrieve an image object from the database."""
    # Return Json serialization of object. The API throws ResourceNotFound
    # excpetion if the given object identifier does not reference an existing
    # image object.
    return jsonify(sco.get_object(
        datastore.ObjectId(image_id),
        datastore.OBJ_IMAGE)
    )


@app.route('/images/files/<string:image_id>/properties', methods=['POST'])
def upsert_image_property(image_id):
    """Upsert image object (POST) - Upsert a property of an image object in the
    database.
    """
    # Upsert the object property. If response code indicates whether the
    # property was created, updated, or deleted. Method throws InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    code = upsert_object_property(
        request,
        datastore.ObjectId(image_id),
        datastore.OBJ_IMAGE
    )
    return '', code


@app.route('/images/files/<string:image_id>', methods=['DELETE'])
def delete_image(image_id):
    """Delete image object (DELETE) - Delete an image object from the
    database.
    """
    # Delete image object with given identifier and return 204. The API
    # will throw ResourceNotFound exception if the given identifier does not
    # reference an existing image.
    sco.delete_object(datastore.ObjectId(image_id), datastore.OBJ_IMAGE)
    return '', 204


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


# ------------------------------------------------------------------------------
# Image Groups
# ------------------------------------------------------------------------------

@app.route('/images/groups')
def list_image_groups():
    """List image groups (GET) - List of all image group objects in the database."""
    # Method raises exception if request argument values are not integers
    return jsonify(list_objects(request, datastore.OBJ_IMAGEGROUP))


@app.route('/images/groups/<string:image_group_id>', methods=['GET'])
def get_image_group(image_group_id):
    """Get image group (GET) - Retrieve an image group from the database."""
    # Return Json serialization of object. The API throws ResourceNotFound
    # excpetion if the given object identifier does not reference an existing
    # image group object.
    return jsonify(sco.get_object(
        datastore.ObjectId(image_group_id),
        datastore.OBJ_IMAGEGROUP)
    )


@app.route('/images/groups/<string:image_group_id>', methods=['DELETE'])
def delete_image_group(image_group_id):
    """Delete image group (DELETE) - Delete an image group object from the
    database.
    """
    # Delete image group object with given identifier and return 204. The API
    # will throw ResourceNotFound exception if the given identifier does not
    # reference an existing image group.
    sco.delete_object(
        datastore.ObjectId(image_group_id),
        datastore.OBJ_IMAGEGROUP
    )
    return '', 204


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
def upsert_image_group_property(image_group_id):
    """Upsert image object (POST) - Upsert a property of an image group in the
    database.
    """
    # Upsert the object property. If response code indicates whether the
    # property was created, updated, or deleted. Method throws InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    code = upsert_object_property(
        request,
        datastore.ObjectId(image_group_id),
        datastore.OBJ_IMAGEGROUP
    )
    return '', code


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
def list_subjects():
    """List subjects (GET) - List of brain anatomy MRI objects in the
    database.
    """
    # Method raises exception if request argument values are not integers
    return jsonify(list_objects(request, datastore.OBJ_SUBJECT))


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


@app.route('/subjects/<string:subject_id>', methods=['GET'])
def get_subject(subject_id):
    """Get subject (GET) - Retrieve a brain anatomy MRI object from the
    database.
    """
    # Return Json serialization of object. The API throws ResourceNotFound
    # excpetion if the given object identifier does not reference an existing
    # subject data object.
    return jsonify(sco.get_object(
        datastore.ObjectId(subject_id),
        datastore.OBJ_SUBJECT)
    )


@app.route('/subjects/<string:subject_id>', methods=['DELETE'])
def delete_subject(subject_id):
    """Delete Subject (DELETE) - Delete a brain anatomy MRI object from the
    database.
    """
    # Delete subject with given identifier and return 204. The API will throw
    # ResourceNotFound exception if the given identifier does not reference an
    # existing subject in the database.
    sco.delete_object(datastore.ObjectId(subject_id), datastore.OBJ_SUBJECT)
    return '', 204


@app.route('/subjects/<string:subject_id>/properties', methods=['POST'])
def upsert_subject_property(subject_id):
    """Upsert subject object (POST) - Upsert a property of a brain anatomy MRI
    object in the database.
    """
    # Upsert the object property. If response code indicates whether the
    # property was created, updated, or deleted. Method throws InvalidRequest
    # or ResourceNotFound exceptions if necessary.
    code = upsert_object_property(
        request,
        datastore.ObjectId(subject_id),
        datastore.OBJ_SUBJECT
    )
    return '', code


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
    List(TypedPropertyInstance)
        List of typed property instances
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


def list_objects(request, object_type):
    """Generalized method to return a list of database objects. Ensures that all
    arguments are in expected format and the calls the respective API method.

    Parameters
    ----------
    request : flask.request
        Flask request object
    object_type : string
        String representation of object type

    Returns
    -------
    Json-like object
        Dictionary representing list of objects. Raises UnknownObjectType
        exception if object type is unknown or InvalidRequest if arguments
        for pagination are not integers.
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
    # Call list_object method of API with request arguments
    return sco.list_objects(object_type, offset=offset, limit=limit, prop_set=prop_set)


def upsert_object_property(request, object_id, object_type):
    """Wrapper to upsert property of given object.Ensures that the request
    contains a valid Json document and calls the respective API method.

    Parameters
    ----------
    request : flask.request
        Flask request object
    object_id : datastore.ObjectId
        Unique object identifier
    object_type : string
        String representation of object type

    Returns
    -------
    int
        Http response code
    """
    # Abort with 400 if the request is not a Json request or if the Json object
    # in the request does not have field key. Field value is optional.
    if not request.json:
        raise exceptions.InvalidRequest('not a valid Json object in request body')
    json_obj = request.json
    if not 'key' in json_obj:
        raise exceptions.InvalidRequest('missing element in Json body: key')
    # Call API's upsert property method
    return sco.upsert_object_property(object_id, object_type, json_obj)


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
