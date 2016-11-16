#!venv/bin/python
import getopt
import sys

from flask import Flask, abort, jsonify, make_response, request, send_file, send_from_directory
from flask_cors import CORS

import api
import exceptions
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

# Local folders
DATA_DIR = '../resources/data'

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
    try:
        # Method raises exception if request argument values are not integers
        return jsonify(list_objects(request, datastore.OBJ_EXPERIMENT))
    except ValueError:
        abort(400)


@app.route('/experiments', methods=['POST'])
def create_experiment():
    """Create experiment (POST) - Create a new experiment object.
    """
    # Make sure that the post request has a json part
    if not request.json:
        abort(400)
    json_obj = request.json
    # Make sure that all required keys are present in the given Json object
    for key in ['name', 'subject', 'images']:
        if not key in json_obj:
            abort(400)
    # Set functional data if key is present.
    functional_data = json_obj['fmri'] if 'fmri' in json_obj else None
    # Call upload method of API
    refs = sco.create_experiment(
        json_obj['name'],
        json_obj['subject'],
        json_obj['images'],
        fmri_data_id=functional_data
    )
    if not refs is None:
        return jsonify({
            'result' : 'SUCCESS',
            'links': refs
        })
    else:
        abort(400)


@app.route('/experiments/<string:object_id>', methods=['GET'])
def get_experiment(object_id):
    """Get experiment (GET) - Retrieve an experiment object from the database.
    """
    result = sco.get_object(object_id, datastore.OBJ_EXPERIMENT)
    if not result is None:
        return jsonify(result)
    else:
        abort(404)


@app.route('/experiments/<string:object_id>', methods=['POST'])
def update_experiment(object_id):
    """Update experiment (POST) - Update functional data information associated
    with an experiment.
    """
    # Upsert the object property. If response code is below 400 return with
    # success, otherwise abort.
    # Make sure that the post request has a json part
    if not request.json:
        abort(400)
    json_obj = request.json
    # Make sure that 'fmri' key is present in the given Json object
    if not 'fmri' in json_obj:
        abort(400)
    # Update experiment. Code idicates success (< 400) or error (>= 400)
    code = sco.update_experiment(object_id,  json_obj['fmri'])
    if code < 400:
        return '', code
    else:
        abort(code)


@app.route('/experiments/<string:object_id>', methods=['DELETE'])
def delete_experiment(object_id):
    """Delete experiment (DELETE) - Delete an experiment object from the
    database.
    """
    # Delete experiment object with given identifier. Return 204 if
    # successful and 404 if the result of the delete operation is False, i.e.,
    # object not found.
    if sco.delete_object(object_id, datastore.OBJ_EXPERIMENT):
        return '', 204
    else:
        abort(404)


@app.route('/experiments/<string:object_id>/properties', methods=['POST'])
def upsert_experiment_property(object_id):
    """Upsert experiment property (POST) - Upsert a property of an experiment
    object in the database.
    """
    # Upsert the object property. If response code is below 400 return with
    # success, otherwise abort.
    code = upsert_object_property(request, object_id, datastore.OBJ_EXPERIMENT)
    if code < 400:
        return '', code
    else:
        abort(code)


# ------------------------------------------------------------------------------
# Functional Data
# ------------------------------------------------------------------------------

@app.route('/fmris')
def list_fmris():
    """List functional MRI data (GET) - List of all functional MRI data objects
    in the database.
    """
    try:
        # Method raises exception if request argument values are not integers
        return jsonify(list_objects(request, datastore.OBJ_FMRI_DATA))
    except ValueError:
        abort(400)


@app.route('/fmris', methods=['POST'])
def upload_fmri():
    """Upload functional MRI data (POST) - Upload a functional MRI data archive
    file.
    """
    refs =  upload_file(request, datastore.OBJ_FMRI_DATA)
    if not refs is None:
        return jsonify({
            'result' : 'SUCCESS',
            'links': refs
        })
    else:
        abort(400)


@app.route('/fmris/<string:object_id>', methods=['GET'])
def get_fmri(object_id):
    """Get functional MRI data (GET) - Retrieve a functional MRI data object
    from the database.
    """
    result = sco.get_object(object_id, datastore.OBJ_FMRI_DATA)
    if not result is None:
        return jsonify(result)
    else:
        abort(404)


@app.route('/fmris/<string:object_id>/properties', methods=['POST'])
def upsert_fmri_property(object_id):
    """Upsert functional MRI data object (POST) - Upsert a property of a
    functional MRI data object in the database.
    """
    # Upsert the object property. If response code is below 400 return with
    # success, otherwise abort.
    code = upsert_object_property(request, object_id, datastore.OBJ_FMRI_DATA)
    if code < 400:
        return '', code
    else:
        abort(code)


@app.route('/fmris/<string:object_id>', methods=['DELETE'])
def delete_fmri(object_id):
    """Delete functional MRI data (DELETE) - Delete a functional MRI data object
    from the database.
    """
    # Delete functional MRI data object with given identifier. Return 204 if
    # successful and 404 if the result of the delete operation is False, i.e.,
    #object not found.
    if sco.delete_object(object_id, datastore.OBJ_FMRI_DATA):
        return '', 204
    else:
        abort(404)


@app.route('/fmris/<string:object_id>/data')
def download_fmri(object_id):
    """Download functional MRI data (GET) - Download data of previously uploaded
    functional MRI data.
    """
    # Get download information for given object. Result is None if object does not
    # exists or does not have any downloadable representation.
    directory, filename, mime_type = sco.get_download(object_id, datastore.OBJ_FMRI_DATA)
    if directory is None:
        abort(404)
    # Send file in the object's upload folder
    return send_from_directory(
        directory,
        filename,
        mimetype=mime_type,
        as_attachment=True,
        attachment_filename=filename
    )


# ------------------------------------------------------------------------------
# Images
# ------------------------------------------------------------------------------

@app.route('/images/files')
def list_images():
    """List images (GET) - List of all image objects in the database."""
    try:
        # Method raises exception if request argument values are not integers
        return jsonify(list_objects(request, datastore.OBJ_IMAGE))
    except ValueError:
        abort(400)


@app.route('/images/files/<string:object_id>', methods=['GET'])
def get_image(object_id):
    """Get image (GET) - Retrieve an image object from the database."""
    result = sco.get_object(object_id, datastore.OBJ_IMAGE)
    if not result is None:
        return jsonify(result)
    else:
        abort(404)


@app.route('/images/files/<string:object_id>/properties', methods=['POST'])
def upsert_image_property(object_id):
    """Upsert image object (POST) - Upsert a property of an image object in the
    database.
    """
    # Upsert the object property. If response code is below 400 return with
    # success, otherwise abort.
    code = upsert_object_property(request, object_id, datastore.OBJ_IMAGE)
    if code < 400:
        return '', code
    else:
        abort(code)


@app.route('/images/files/<string:object_id>', methods=['DELETE'])
def delete_image(object_id):
    """Delete image object (DELETE) - Delete an image object from the
    database.
    """
    # Delete image object with given identifier. Return 204 if successful
    # and 404 if the result of the delete operation is False, i.e., object
    # not found.
    if sco.delete_object(object_id, datastore.OBJ_IMAGE):
        return '', 204
    else:
        abort(404)


@app.route('/images/files/<string:object_id>/data')
def download_image(object_id):
    """Download image file (GET)"""
    # Get download information for given object. Result is None if object does not
    # exists or does not have any downloadable representation.
    directory, filename, mime_type = sco.get_download(object_id, datastore.OBJ_IMAGE)
    if directory is None:
        abort(404)
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
    try:
        # Method raises exception if request argument values are not integers
        return jsonify(list_objects(request, datastore.OBJ_IMAGEGROUP))
    except ValueError:
        abort(400)


@app.route('/images/groups/<string:object_id>', methods=['GET'])
def get_image_group(object_id):
    """Get image group (GET) - Retrieve an image group from the database."""
    result = sco.get_object(object_id, datastore.OBJ_IMAGEGROUP)
    if not result is None:
        return jsonify(result)
    else:
        abort(404)


@app.route('/images/groups/<string:object_id>', methods=['DELETE'])
def delete_image_group(object_id):
    """Delete image group (DELETE) - Delete an image group object from the
    database.
    """
    # Delete image object with given identifier. Return 204 if successful
    # and 404 if the result of the delete operation is False, i.e., object
    # not found.
    if sco.delete_object(object_id, datastore.OBJ_IMAGEGROUP):
        return '', 204
    else:
        abort(404)


@app.route('/images/groups/<string:object_id>/data')
def download_image_group(object_id):
    """Download image group file (GET)"""
    # Get download information for given object. Result is None if object does not
    # exists or does not have any downloadable representation.
    directory, filename, mime_type = sco.get_download(object_id, datastore.OBJ_IMAGEGROUP)
    if directory is None:
        abort(404)
    # Send file in the object's upload folder
    return send_from_directory(
        directory,
        filename,
        mimetype=mime_type,
        as_attachment=True,
        attachment_filename=filename
    )


@app.route('/images/groups/<string:object_id>/options', methods=['POST'])
def update_image_group_options(object_id):
    """Upsert image group options (POST) - Upsert the options that are
    associated with an image group in the database. Given that these options
    cannot be included in the file upload, there has to be a separate API call.
    """
    # Make sure that the request contains a Json body with an 'options' element
    if not request.json:
        abort(400)
    json_obj = request.json
    if not 'options' in json_obj:
        abort(400)
    # Convert the Json element associated with key 'options' into a list of
    # typed property instance. Throws a ValueError if the format of the element
    # value is invalid.
    try:
        attributes = get_attributes(json_obj['options'])
    except ValueError:
        abort(400)
    # Upsert object options. Returned code corresponds to HTTP response code.
    code = sco.update_object_attributes(object_id, datastore.OBJ_IMAGEGROUP, attributes)
    if code < 400:
        return '', code
    else:
        abort(code)


@app.route('/images/groups/<string:object_id>/properties', methods=['POST'])
def upsert_image_group_property(object_id):
    """Upsert image object (POST) - Upsert a property of an image group in the
    database.
    """
    # Upsert the object property. If response code is below 400 return with
    # success, otherwise abort.
    code = upsert_object_property(request, object_id, datastore.OBJ_IMAGEGROUP)
    if code < 400:
        return '', code
    else:
        abort(code)


# ------------------------------------------------------------------------------
# Upload Images
# ------------------------------------------------------------------------------

@app.route('/images/upload', methods=['POST'])
def upload_images():
    """Upload Images (POST) - Upload an image file or an archive of images."""
    refs =  upload_file(request, datastore.OBJ_IMAGE)
    if not refs is None:
        return jsonify({
            'result' : 'SUCCESS',
            'links': refs
        })
    else:
        abort(400)


# ------------------------------------------------------------------------------
# Subjects
# ------------------------------------------------------------------------------

@app.route('/subjects')
def list_subjects():
    """List subjects (GET) - List of brain anatomy MRI objects in the
    database.
    """
    try:
        # Method raises exception if request argument values are not integers
        return jsonify(list_objects(request, datastore.OBJ_SUBJECT))
    except ValueError:
        abort(400)


@app.route('/subjects', methods=['POST'])
def upload_subject():
    """Upload Subject (POST) - Upload an brain anatomy MRI archive file."""
    refs =  upload_file(request, datastore.OBJ_SUBJECT)
    if not refs is None:
        return jsonify({
            'result' : 'SUCCESS',
            'links': refs
        })
    else:
        abort(400)


@app.route('/subjects/<string:object_id>', methods=['GET'])
def get_subject(object_id):
    """Get subject (GET) - Retrieve a brain anatomy MRI object from the
    database.
    """
    result = sco.get_object(object_id, datastore.OBJ_SUBJECT)
    if not result is None:
        return jsonify(result)
    else:
        abort(404)


@app.route('/subjects/<string:object_id>/properties', methods=['POST'])
def upsert_subject_property(object_id):
    """Upsert subject object (POST) - Upsert a property of a brain anatomy MRI
    object in the database.
    """
    # Upsert the object property. If response code is below 400 return with
    # success, otherwise abort.
    code = upsert_object_property(request, object_id, datastore.OBJ_SUBJECT)
    if code < 400:
        return '', code
    else:
        abort(code)


@app.route('/subjects/<string:object_id>', methods=['DELETE'])
def delete_subject(object_id):
    """Delete Subject (DELETE) - Delete a brain anatomy MRI object from the
    database.
    """
    # Delete anatomy object with given identifier. Return 204 if successful
    # and 404 if the result of the delete operation is False, i.e., object
    # not found.
    if sco.delete_object(object_id, datastore.OBJ_SUBJECT):
        return '', 204
    else:
        abort(404)


@app.route('/subjects/<string:object_id>/data')
def download_subject(object_id):
    """Download subject (GET) - Download data of previously uploaded subject
    anatomy.
    """
    # Get download information for given object. Result is None if object does not
    # exists or does not have any downloadable representation.
    directory, filename, mime_type = sco.get_download(object_id, datastore.OBJ_SUBJECT)
    if directory is None:
        abort(404)
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
    ValueError if the given array violated expected format.

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
        raise ValueError('Argument is not a list')
    # Iterate over all elements in the list. Make sure they are Json objects
    # with 'name' and 'value' elements
    for element in json_array:
        if not isinstance(element, dict):
            raise ValueError('Element is not a dictionary')
        for key in ['name', 'value']:
            if not key in element:
                raise ValueError('Object has not key ' + key)
        name = str(element['name'])
        value = element['value']
        result.append(datastore.Attribute(name, value))
    return result

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
        exception if object type is unknown.
    """
    # Check if limit and offset parameters are given. Throws ValueError if
    # values are not integer.
    try:
        offset = int(request.args[hateoas.QPARA_OFFSET]) if hateoas.QPARA_OFFSET in request.args else 0
        limit = int(request.args[hateoas.QPARA_LIMIT]) if hateoas.QPARA_LIMIT in request.args else -1
    except ValueError:
        abort(400)
    # Get the set of attributes that are included in the result listing. By
    # default, the object name is always included.
    prop_set = request.args[hateoas.QPARA_PROPERTIES].split(',') if hateoas.QPARA_PROPERTIES in request.args else None
    # Call list_object method of API with request arguments
    return sco.list_objects(object_type, offset=offset, limit=limit, prop_set=prop_set)


def upload_file(request, object_type):
    """Generalized method for file uploads. Ensures that request contains a
    file and calls the upload method of the API.

    Parameters
    ----------
    request : flask.request
        Flask request object
    object_type : string
        String representation of object type

    Returns
    -------
    List
        List of object references for created object. The reference list is
        None in case of errors.
    """
    # Make sure that the post request has the file part
    if 'file' not in request.files:
        abort(400)
    file = request.files['file']
    # A browser may submit a empty part without filename
    if file.filename == '':
        abort(400)
    # Call upload method of API
    return sco.upload_file(object_type, file)


def upsert_object_property(request, object_id, object_type):
    """Wrapper to upsert property of given object.Ensures that the request
    contains a valid Json document and calls the respective API method.

    Parameters
    ----------
    request : flask.request
        Flask request object
    object_id : string
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
        abort(400)
    json_obj = request.json
    if not 'key' in json_obj:
        abort(400)
    # Call API's upsert property method
    return sco.upsert_object_property(object_id, object_type, json_obj)


# ------------------------------------------------------------------------------
# Error Handler
# ------------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(error):
    """404 JSON response generator."""
    print str(error)
    return make_response(jsonify({'error': 'Not found'}), 404)


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
    app.config['DEBUG'] = True
    # Load a dummy app at the root URL to give 404 errors.
    # Serve app at APPLICATION_ROOT for localhost development.
    application = DispatcherMiddleware(Flask('dummy_app'), {
        app.config['APPLICATION_ROOT']: app,
    })
    run_simple('0.0.0.0', SERVER_PORT, application, use_reloader=True)
