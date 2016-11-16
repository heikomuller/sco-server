"""Standard Cortical Observer - API

Collection of classes and methods used by the Standard Cortical Observer API
Web service.
"""

import os
import shutil
import tarfile
import tempfile

from pymongo import MongoClient
from werkzeug.utils import secure_filename

import exceptions
import hateoas
import utils
import db.datastore as datastore
import db.experiment as experiement
import db.funcdata as funcdata
import db.images as images
import db.subject as subject


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

# Sets of valid suffixes for file upload
ARCHIVE_SUFFIXES = set(['.tar', '.tar.gz', '.tgz'])


# ------------------------------------------------------------------------------
#
# Web API Server Classes
#
# ------------------------------------------------------------------------------

class DataServer:
    """Data Server - The SCO data server captures references to all
    components needed to manage server resources for subjects and images. In
    the current implementation we use MongoDB as the storage backend.

    Attributes
    ----------
    object_urls : Dictionary
        Dictionary of listing URL's for different object types
    refs : hateoas.HATEOASReferenceFactory
        Reference factory for HATEOAS links
    stores : Dictionary
        Dictionary of datastore.ObjectStores for different object types
    data_dir : string
        Base directory for data files
    """
    def __init__(self, data_dir, urls, mongo_client=MongoClient()):
        """Initialize the subject manager.

        Parameters
        ----------
        data_dir : string
            The directory for storing data files. Directory will be created
            if it does not exist. For different types of objects various
            sub-directories will also be created if they don't exist.
        urls : hateoas.UrlFactory
            Factory for server resources URL's
        mongo_client : MongoClient, optional
            Client for MongoDB. Will use default client connected to local
            database server if ommited.
        """
        # Create HATEOAS reference factory
        self.refs = hateoas.HATEOASReferenceFactory(urls)
        # The current implementation uses MongoDB database 'scoserv'
        db = mongo_client.scoserv
        # Ensure that varios data sub-folders exist
        self.data_dir = utils.create_dir(data_dir)
        subjects_dir = utils.create_dir(os.path.join(self.data_dir, 'subjects'))
        images_dir = utils.create_dir(os.path.join(self.data_dir, 'images'))
        image_files_dir = utils.create_dir(os.path.join(images_dir, 'files'))
        image_groups_dir = utils.create_dir(os.path.join(images_dir, 'groups'))
        fmridata_dir = utils.create_dir(os.path.join(self.data_dir, 'fmris'))
        # Create object stores and maintain a dictionary for access.
        self.stores = {
            datastore.OBJ_EXPERIMENT : experiement.DefaultExperimentManager(db.experiments),
            datastore.OBJ_FMRI_DATA : funcdata.DefaultFMRIDataManager(db.fmris, fmridata_dir),
            datastore.OBJ_IMAGE : images.DefaultImageManager(db.images, image_files_dir),
            datastore.OBJ_IMAGEGROUP : images.DefaultImageGroupManager(db.imagegroups, image_groups_dir),
            datastore.OBJ_SUBJECT : subject.DefaultSubjectManager(db.subjects, subjects_dir)
        }
        # Maintain dictionary of listing URL's for different object types
        self.object_urls = {
            datastore.OBJ_EXPERIMENT : urls.experiments.list(),
            datastore.OBJ_FMRI_DATA : urls.fmris.list(),
            datastore.OBJ_IMAGE : urls.images.list(),
            datastore.OBJ_IMAGEGROUP : urls.image_groups.list(),
            datastore.OBJ_SUBJECT : urls.subjects.list()
        }

    def create_experiment(self, name, subject_id, image_group_id, fmri_data_id=None):
        """Create a new experiment object. Ensures that all referenced objects
        are valid.

        Parameters
        ----------
        name : string
            User-provided name
        subject_id : string
            Unique subject identifier
        image_group_id : string
            Unique image group identifier
        fmri_data_id : string, optional
            Unique functional data object identifier

        Returns
        -------
        List
            List of object references for created object. The result is None in
            case of errors.
        """
        # Ensure that the subject identifier refers to an existing object.
        if not self.object_store(datastore.OBJ_SUBJECT).exists_object(subject_id):
            return None
        # Ensure that the image group identifier refers to an existing object.
        if not self.object_store(datastore.OBJ_IMAGEGROUP).exists_object(image_group_id):
            return None
        # Ensure that the functional data identifier refers to an existing
        # object (if given).
        if not fmri_data_id is None:
            if not self.object_store(datastore.OBJ_FMRI_DATA).exists_object(fmri_data_id):
                return None
        # Get object store for experiments.
        store = self.object_store(datastore.OBJ_EXPERIMENT)
        # Create new object in experiment store
        db_obj = store.create_object(
            name,
            subject_id,
            image_group_id,
            fmri_data=fmri_data_id
        )
        # Return object references
        return self.refs.object_references(db_obj)

    def delete_object(self, object_id, object_type):
        """Delete object with given identifier of given type.

        Parameters
        ----------
        object_id : string
            Unique object identifier
        object_type : string
            String representation of object type

        Returns
        -------
        Boolean
            Returns True if success or False if object not found.
        """
        # Return result of object store's get_download method.
        db_obj = self.object_store(object_type).delete_object(object_id)
        return not db_obj is None

    def get_download(self, object_id, object_type):
        """Get download information for object with given identifier of given
        type.

        Parameters
        ----------
        object_id : string
            Unique object identifier
        object_type : string
            String representation of object type

        Returns
        -------
        Tuple (string, string, string)
            Returns directory, file name, and mime type of downloadable file.
            Result contains all None if object does not exist.
        """
        # Return result of object store's get_download method.
        return self.object_store(object_type).get_download(object_id)

    def get_object(self, object_id, object_type):
        """Retrieve database object. Type specifies the object store
        while identifier specifies the object.

        Parameters
        ----------
        object_id : string
            Unique object identifier
        object_type : string
            String representation of object type

        Returns
        -------
        Json-like object
            Dictionary representing the identified object. the result is None
            if the object does not exists.
        """
        # Get ObjectStore for given object type.
        store = self.object_store(object_type)
        # Get the object identifier by object_id. Return None if object does
        # not exists.
        db_obj = store.get_object(object_id)
        if db_obj is None:
            return None
        # Construct the result item. The base serialization includes identifier,
        # object type, timestamp, name, and properties listing.
        # and add HATEOAS references.
        item = {
            'id' : db_obj.identifier,
            'type' : db_obj.type,
            'timestamp' : str(db_obj.timestamp.isoformat()),
            'name' : db_obj.name,
            'properties' : utils.to_list(db_obj.properties),
            'links' : self.refs.object_references(db_obj)
        }
        # Add object type specific elements to the result item
        if db_obj.is_image_group:
            # For image groups add list of group images and list of options
            item['images'] = [
                {
                    'identifier' : image.identifier,
                    'folder' : image.folder,
                    'name' : image.name,
                    'links' : self.refs.to_references(
                        {'self' : self.refs.urls.images.get(image.identifier)}
                    )

                } for image in db_obj.images
            ]
            item['options'] = [
                {
                    'name' : attr,
                    'value' : db_obj.options[attr]
                } for attr in db_obj.options
            ]
        elif db_obj.is_experiment:
            # Add descriptors for associated objects
            subject = self.object_store(datastore.OBJ_SUBJECT).get_object(db_obj.subject, include_inactive=True)
            if subject is None:
                return None
            else:
                item['subject'] = {
                    'identifier' : subject.identifier,
                    'name' : subject.name,
                    'links' : self.refs.object_references(subject)
                }
            images = self.object_store(datastore.OBJ_IMAGEGROUP).get_object(db_obj.images, include_inactive=True)
            if images is None:
                return None
            else:
                item['images'] = {
                    'identifier' : images.identifier,
                    'name' : images.name,
                    'links' : self.refs.object_references(images)
                }
            if not db_obj.fmri_data is None:
                fmri = self.object_store(datastore.OBJ_FMRI_DATA).get_object(db_obj.fmri_data, include_inactive=True)
                if fmri is None:
                    return None
                else:
                    item['fmri'] = {
                        'identifier' : fmri.identifier,
                        'name' : fmri.name,
                        'links' : self.refs.object_references(fmri)
                    }
        return item

    def list_objects(self, object_type, offset=-1, limit=-1, prop_set=None):
        """Generalized method to return a list of database objects.

        Parameters
        ----------
        object_type : string
            String representation of object type
        offset : int, optional
            Offset for list items to be included in result
        limit : int, optional
            Limit number of items in result
        prop_set : List(string)
            List of object properties to be included for items in result

        Returns
        -------
        Json-like object
            Dictionary representing list of objects. Raises UnknownObjectType
            exception if object type is unknown.
        """
        # Get ObjectStore for given object type.
        store = self.object_store(object_type)
        # Get the object listing
        list_result = store.list_objects(offset=offset, limit=limit)
        # Build the result. This includes the item list, offset, limit, and
        # attributes as well as links for navigation
        result = {
            'count' : len(list_result.items),
            'totalCount' : list_result.total_count
        }
        if not prop_set is None:
            result['properties'] = prop_set
        if limit >= 0:
            result[hateoas.QPARA_LIMIT] = limit
        if offset >= 0:
            result[hateoas.QPARA_OFFSET] = offset
        items = []
        for db_obj in list_result.items:
            item = {
                'id' : db_obj.identifier,
                'timestamp' : str(db_obj.timestamp.isoformat()),
                'name' : db_obj.name,
                'links' : self.refs.object_references(db_obj)
            }
            if prop_set is None:
                item['properties'] = utils.to_list(db_obj.properties)
            else:
                dict_attr = {}
                for attr in prop_set:
                    if attr in db_obj.properties:
                        dict_attr[attr] = db_obj.properties[attr]
                item['properties'] = utils.to_list(dict_attr)
            items.append(item)
        result['items'] = items
        # Attribute parameter
        attributes = ','.join(prop_set) if not prop_set is None else None
        # Get base Url for given object type and pagination decorator
        url = self.object_urls[object_type]
        pages = hateoas.PaginationDecorator(url, offset, limit, list_result.total_count, attributes)
        # Add navigational links first, last, prov, and next
        links = {
            'self' : url,
            'first' : pages.first(),
            'last' : pages.last()
        }
        prev_page = pages.prev()
        if not prev_page is None:
            links['prev'] = prev_page
        next_page = pages.next()
        if not next_page is None:
            links['next'] = next_page
        result['links'] = self.refs.to_references(links)
        # Return the object listing
        return result

    def object_store(self, object_type):
        """Get ObjectStore for given object type. Raises UnknownObjectType
        exception if result is None.

        Parameters
        ----------
        object_type : string
            String representation of object type

        Returns
        -------
        ObjectStore
            Object store for given object type. Raises exception if object type
            is unknown.
        """
        store = self.stores[object_type]
        if store is None:
            raise exceptions.UnknownObjectType(object_type)
        return store

    def update_experiment(self, object_id, fmri_data_id):
        """Update the functional data object that is associated with the
        experiment identified by the given object identifier.

        Parameters
        ----------
        object_id : string
            Unique experiment identifier
        fmri_data_id : string
            Unique functional data object identifier

        Returns
        -------
        int
            Http response code. 200 if uopdate is successful. Returns 404 if
            the experiment does not exist and 400 if the functional data object
            does not exist.
        """
        # Ensure that the functional data identifier refers to an existing
        # object. If not, return with 400 (INVALID REQUEST).
        if not self.object_store(datastore.OBJ_FMRI_DATA).exists_object(fmri_data_id):
            return 400
        # Get object store for experiments
        store = self.object_store(datastore.OBJ_EXPERIMENT)
        # Retrieve the experiment from the database. Return 404 if the object
        # does not exist
        db_obj = store.get_object(object_id)
        if db_obj is None:
            return 404
        # Update object in database and return success
        db_obj.fmri_data = fmri_data_id
        store.replace_object(db_obj)
        return 200

    def upload_file(self, object_type, file):
        """Generalized method for file uploads. Type specifies the object store.

        Parameters
        ----------
        object_type : string
            String representation of object type
        file : FileObject
            File that is being uploaded

        Returns
        -------
        List
            List of object references for created object. The result is None in
            case of errors.
        """
        # Depending on the object type different upload methods are invoked.
        if object_type == datastore.OBJ_SUBJECT:
            return self.upload_subject(file)
        elif object_type in [datastore.OBJ_IMAGE, datastore.OBJ_IMAGEGROUP]:
            return self.upload_images(file)
        elif object_type == datastore.OBJ_FMRI_DATA:
            return self.upload_fmri_data(file)
        else:
            # Cannot upload object of given type.
            return None

    def upload_fmri_data(self, file):
        """Upload a functional MRI data archive file. Expects a tar-file.

        Parameters
        ----------
        file : File
            File-type object referencing the uploaded file

        Returns
        -------
        List
            List of object references for created the uploaded file. If there
            was an error the list is None.
        """
        # Get object store for functional data objects.
        store = self.object_store(datastore.OBJ_FMRI_DATA)
        # Get the suffix of the uploaded file. Result is None if not a valid
        # suffix
        suffix = utils.get_filename_suffix(file.filename, ARCHIVE_SUFFIXES)
        if not suffix is None:
            # Copy the file to a temporary folder and then invoke the database
            # upload method.
            temp_dir = tempfile.mkdtemp()
            filename = secure_filename(file.filename)
            upload_file = os.path.join(temp_dir, filename)
            file.save(upload_file)
            try:
                db_obj = store.create_object(upload_file)
            except ValueError as ex:
                return None
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return self.refs.object_references(db_obj)
        else:
            # Not a valid file suffix
            return None

    def upload_images(self, file):
        """Upload image file. Expects either a single image or a tar-file of
        images.

        Parameters
        ----------
        file : File
            File-type object referencing the uploaded file

        Returns
        -------
        List
            List of object references for created object (image or image group).
            The result is None in case of an error.
        """
        # Check if file is a single image
        suffix = utils.get_filename_suffix(file.filename, images.VALID_IMGFILE_SUFFIXES)
        if not suffix is None:
            # Copy the file to a temporary folder and then invoke the image
            # store upload method.
            temp_dir = tempfile.mkdtemp()
            filename = secure_filename(file.filename)
            upload_file = os.path.join(temp_dir, filename)
            file.save(upload_file)
            try:
                db_obj = self.object_store(datastore.OBJ_IMAGE).create_object(upload_file)
            except ValueError as ex:
                return None
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return self.refs.object_references(db_obj)
        # Check if the file is a valid tar archive (based on siffix)
        suffix = utils.get_filename_suffix(file.filename, ARCHIVE_SUFFIXES)
        if not suffix is None:
            # Unpack the file to a temporary folder .
            temp_dir = tempfile.mkdtemp()
            filename = secure_filename(file.filename)
            upload_file = os.path.join(temp_dir, filename)
            file.save(upload_file)
            try:
                tf = tarfile.open(name=upload_file, mode='r')
                tf.extractall(path=temp_dir)
            except (tarfile.ReadError, IOError) as err:
                # Clean up in case there is an error during extraction
                shutil.rmtree(temp_dir)
                raise err
            # Get names of all files with valid image suffixes and create an
            # object for each image object
            img_store = self.object_store(datastore.OBJ_IMAGE)
            group_images = []
            for img_file in images.get_image_files(temp_dir, []):
                img_obj = img_store.create_object(img_file)
                folder = img_file[len(temp_dir):-len(img_obj.name)]
                group_images.append(images.GroupImage(
                    img_obj.identifier,
                    folder,
                    img_obj.name
                ))
            # Create image group
            name = os.path.basename(os.path.normpath(filename))[:-len(suffix)]
            db_obj = self.object_store(datastore.OBJ_IMAGEGROUP).create_object(name, group_images, upload_file)
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return self.refs.object_references(db_obj)
        else:
            # Not a valid file suffix
            return None

    def upload_subject(self, file):
        """Upload a subject file. Expects a Freesurfer tar-file.

        Parameters
        ----------
        file : File
            File-type object referencing the uploaded file

        Returns
        -------
        List
            List of object references for created subject. If there was an error
            the list is None.
        """
        # Get object store for subjects.
        store = self.object_store(datastore.OBJ_SUBJECT)
        # Get the suffix of the uploaded file. Result is None if not a valid siffix
        suffix = utils.get_filename_suffix(file.filename, ARCHIVE_SUFFIXES)
        if not suffix is None:
            # Copy the file to a temporary folder and then invoke the database
            # upload method.
            temp_dir = tempfile.mkdtemp()
            filename = secure_filename(file.filename)
            upload_file = os.path.join(temp_dir, filename)
            file.save(upload_file)
            try:
                db_obj = store.upload_file(upload_file)
            except ValueError as ex:
                return None
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return self.refs.object_references(db_obj)
        else:
            # Not a valid file suffix
            return None

    def update_object_attributes(self, object_id, object_type, attributes):
        """Update set of typed attributes (options) that are associated with
        a given object.

        Parameters
        ----------
        object_id : string
            Unique object identifier
        object_type : string
            String representation of object type
        attributes : List(datastore.Attribute)
            List of attribute instances

        Returns
        -------
        int
            Http response code (204 for success, 400 for invalid set of,
            attributes or 404 if object not found)
        """
        # Get ObjectStore for given object type.
        store = self.object_store(object_type)
        # Test if an object with given identifier exists in the object store.
        # Abort with 404 if result is False.
        if not store.exists_object(object_id):
            return 404
        # Call the object store specific method to update object options
        try:
            store.update_object_attributes(object_id, attributes)
        except ValueError as err:
            print err
            return 400
        # Return 204 to signal successful update of object attributes
        return 204

    def upsert_object_property(self, object_id, object_type, json_obj):
        """Upsert property of given object. Type specifies the object store
        while identifier specifies the object. The response depends on whether
        the object property was created, updated, or deleted.

        Parameters
        ----------
        object_id : string
            Unique object identifier
        object_type : string
            String representation of object type
        json_obj : Json object
            Json object containing key and (optinal) value of property that is
            being updated.

        Returns
        -------
        int
            Http response code
        """
        # Get ObjectStore for given object type.
        store = self.object_store(object_type)
        # Get the object identifier by object_id. Abort with 404 if result is
        # None
        db_obj = store.get_object(object_id)
        if db_obj is None:
            return 404
        # Call the update_object_property method of the ObjectStore with the key
        # and optional value fields
        key = json_obj['key']
        value = json_obj['value'] if 'value' in json_obj else None
        state = store.upsert_object_property(db_obj.identifier, key, value=value)
        # Return a response code based on the outcome of the update operation.
        if state == datastore.OP_ILLEGAL:
            return 400
        elif state == datastore.OP_CREATED:
            return 201
        elif state == datastore.OP_DELETED:
            return 204
        elif state == datastore.OP_UPDATED:
            return 200
        else: # -1
            return 404
