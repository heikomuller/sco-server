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

import reqexcpt as exceptions
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

    def create_experiment(self, name, subject_id, image_group_id):
        """Create a new experiment object.

        Ensures that all referenced objects exist. Throws InvalidRequest
        exception if one of the object does not exist.

        Parameters
        ----------
        name : string
            User-provided name
        subject_id : string
            Unique subject identifier
        image_group_id : string
            Unique image group identifier

        Returns
        -------
        experiment.ExperimentHandle
            Handle for created experiment object in database.
        """
        # Ensure that the subject identifier refers to an existing object.
        if not self.object_store(datastore.OBJ_SUBJECT).exists_object(subject_id):
            raise exceptions.InvalidRequest('unknown subject reference: ' + subject_id)
        # Ensure that the image group identifier refers to an existing object.
        if not self.object_store(datastore.OBJ_IMAGEGROUP).exists_object(image_group_id):
            raise exceptions.InvalidRequest('unknown image group reference: ' + image_group_id)
        # Get object store for experiments.
        store = self.object_store(datastore.OBJ_EXPERIMENT)
        # Create new object in experiment store and return it
        return store.create_object(
            name,
            subject_id,
            image_group_id
        )
        # Return object references

    def delete_object(self, object_id, object_type):
        """Delete object with given identifier of given type.

        If the given object is an experiment that has an functional MRI data
        object associated with it we delete the fMRI object as well since these
        objects have no 'existence' without an experiment.

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
        # Delete object in object store that is associated with the given object
        # type. Returns a reference to the object or None, if object did not
        # exist.
        db_obj = self.object_store(object_type).delete_object(object_id)
        # Return object reference or raise ResourceNotFound exception if object
        # did not exist.
        if not db_obj is None:
            # Delete associated FMRI data if the deleted object is an experiment
            if db_obj.is_experiment:
                if not db_obj.fmri_data is None:
                    self.delete_object(
                        db_obj.fmri_data,
                        datastore.OBJ_FMRI_DATA
                    )
            return db_obj
        else:
            raise exceptions.ResourceNotFound(object_id, object_type)

    def get_download(self, object_id, object_type):
        """Get download information for object with given identifier of given
        type.

        Raises ResourceNotFound exception if resource does not exist.

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
        """
        # Return result of object store's get_download method.
        download = self.object_store(object_type).get_download(object_id)
        if not download is None:
            return download
        else:
            raise exceptions.ResourceNotFound(object_id, object_type)

    def get_object(self, object_id, object_type):
        """Retrieve database object. Type specifies the object store
        while identifier specifies the object.

        Raises ResourceNotFound exception if the requested object does not
        exist.

        Parameters
        ----------
        object_id : string
            Unique object identifier
        object_type : string
            String representation of object type

        Returns
        -------
        Json-like object
            Dictionary representing the identified object.
        """
        # Get ObjectStore for given object type.
        store = self.object_store(object_type)
        # Get the object identifier by object_id. Return None if object does
        # not exists.
        db_obj = store.get_object(object_id)
        if db_obj is None:
            raise exceptions.ResourceNotFound(object_id, object_type)
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
            subject = self.object_store(datastore.OBJ_SUBJECT).get_object(
                db_obj.subject,
                include_inactive=True
            )
            item['subject'] = {
                'identifier' : subject.identifier,
                'name' : subject.name,
                'links' : self.refs.object_references(subject)
            }
            images = self.object_store(datastore.OBJ_IMAGEGROUP).get_object(
                db_obj.images,
                include_inactive=True
            )
            item['images'] = {
                'identifier' : images.identifier,
                'name' : images.name,
                'links' : self.refs.object_references(images)
            }
            if not db_obj.fmri_data is None:
                fmri = self.object_store(datastore.OBJ_FMRI_DATA).get_object(
                    db_obj.fmri_data,
                    include_inactive=True
                )
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

    def upload_experiment_fmri(self, object_id, file):
        """Update the functional data object that is associated with the
        experiment identified by the given object identifier.

        Raises ResourceNotFound exception if experiment is not found.
        Parameters
        ----------
        object_id : string
            Unique experiment identifier
        file : FileObject
            Functional MRI data file that is being uploaded

        Returns
        -------
        ExperimentHandle
            Handle for updated experiment object in database.
        """
        # Get object store for experiments
        store = self.object_store(datastore.OBJ_EXPERIMENT)
        # Retrieve the experiment from the database. Raise exception if the
        # object does not exist
        experiment = store.get_object(object_id)
        if experiment is None:
            raise exceptions.ResourceNotFound(
                object_id,
                datastore.OBJ_EXPERIMENT
            )
        # If the experiment has a functional MRI data object associated with
        # it, delete that object since existence of these objects is determined
        # by their reference from a subject.
        if not experiment.fmri_data is None:
            self.delete_object(experiment.fmri_data, datastore.OBJ_FMRI_DATA)
        # Create a database object for the uploaded fMRI file.
        fmri_obj = self.upload_fmri_data(file)
        # Update object in database and return the updated experiment object
        experiment.fmri_data = fmri_obj.identifier
        store.replace_object(experiment)
        return experiment

    def upload_file(self, object_type, file):
        """Generalized method for file uploads. Type specifies the object store.

        Raises a InvalidRequest exceptio if the given object type does not
        support file uploads.

        Parameters
        ----------
        object_type : string
            String representation of object type
        file : FileObject
            File that is being uploaded

        Returns
        -------
        DBObject
            Handle for the modified database object.
        """
        # Depending on the object type different upload methods are invoked.
        if object_type == datastore.OBJ_SUBJECT:
            return self.upload_subject(file)
        elif object_type in [datastore.OBJ_IMAGE, datastore.OBJ_IMAGEGROUP]:
            return self.upload_images(file)
        else:
            # Cannot upload object of given type.
            raise exceptions.InvalidRequest(
                'object type does not support file upload: ' + object_type
            )

    def upload_fmri_data(self, file):
        """Upload a functional MRI data archive file. Expects a tar-file.

        Throws a InvalidRequest exception if the given file has an unexpected
        suffix.

        Parameters
        ----------
        file : File
            File-type object referencing the uploaded file

        Returns
        -------
        FunctionalDataHandle
            Handle for the newly created functional MRI data object.
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
            db_obj = store.create_object(upload_file)
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return db_obj
        else:
            # Not a valid file suffix
            raise exceptions.InvalidRequest(
                'invalid file suffix: ' + file.filename
            )

    def upload_images(self, file):
        """Upload image file. Expects either a single image or a tar-file of
        images.

        Raises a InvalidRequest exception if the given file does not have a
        recognized suffix.

        Parameters
        ----------
        file : File
            File-type object referencing the uploaded file

        Returns
        -------
        DBObject
            Handle for the modified image object.
        """
        # Check if file is a single image
        suffix = utils.get_filename_suffix(
            file.filename,
            images.VALID_IMGFILE_SUFFIXES
        )
        if not suffix is None:
            # Copy the file to a temporary folder and then invoke the image
            # store upload method.
            temp_dir = tempfile.mkdtemp()
            filename = secure_filename(file.filename)
            upload_file = os.path.join(temp_dir, filename)
            file.save(upload_file)
            db_obj = self.object_store(datastore.OBJ_IMAGE).create_object(
                upload_file
            )
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return db_obj
        # The file has not been recognized as a valid image. Check if the file
        # is a valid tar archive (based on suffix).
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
            db_obj = self.object_store(datastore.OBJ_IMAGEGROUP).create_object(
                name,
                group_images,
                upload_file
            )
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return db_obj
        else:
            # Not a valid file suffix
            raise exceptions.InvalidRequest(
                'invalid file suffix: ' + file.filename
                )

    def upload_subject(self, file):
        """Upload a subject file. Expects a Freesurfer tar-file.

        Raises a InvalidRequest exception if the given file does not have a
        recognized suffix.

        Parameters
        ----------
        file : File
            File-type object referencing the uploaded file

        Returns
        -------
        SubjectHandle
            Handle for the modified subject in database.
        """
        # Get object store for subjects.
        store = self.object_store(datastore.OBJ_SUBJECT)
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
            db_obj = store.upload_file(upload_file)
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return db_obj
        else:
            # Not a valid file suffix
            raise exceptions.InvalidRequest(
                'Invalid file suffix: ' + file.filename
            )

    def update_object_attributes(self, object_id, object_type, attributes):
        """Update set of typed attributes (options) that are associated with
        a given object.

        Raises ResourceNotFound exception if the requested object does not
        exist. Raises InvalidRequest exception if the given attributes violate
        the attributes definition of the updated object type.

        Parameters
        ----------
        object_id : string
            Unique object identifier
        object_type : string
            String representation of object type
        attributes : List(datastore.Attribute)
            List of attribute instances
        """
        # Get ObjectStore for given object type.
        store = self.object_store(object_type)
        # Test if an object with given identifier exists in the object store.
        # Raise ResourceNotFound exception if result is False.
        if not store.exists_object(object_id):
            raise exceptions.ResourceNotFound(object_id, object_type)
        # Call the object store specific method to update object options
        try:
            store.update_object_attributes(object_id, attributes)
        except ValueError as err:
            raise exceptions.InvalidRequest(str(err))

    def upsert_object_property(self, object_id, object_type, json_obj):
        """Upsert property of given object. Type specifies the object store
        while identifier specifies the object. The response depends on whether
        the object property was created, updated, or deleted.

        Raises ResourceNotFound exception if the requested object does not
        exist. Raises InvalidRequest exception if the upsert resulted in an
        illegal operation.

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
        # Get the object identifier by object_id. Raise ResourceNotFound
        # exception if result is None
        db_obj = store.get_object(object_id)
        if db_obj is None:
            raise exceptions.ResourceNotFound(object_id, object_type)
        # Call the update_object_property method of the ObjectStore with the key
        # and optional value fields
        key = json_obj['key']
        value = json_obj['value'] if 'value' in json_obj else None
        state = store.upsert_object_property(db_obj.identifier, key, value=value)
        # Return a response code based on the outcome of the update operation.
        if state == datastore.OP_ILLEGAL:
            message = 'illegal upsert for type ' + object_type
            message += ': (' + key + ', ' + value + ')'
            raise exceptions.InvalidRequest(message)
        elif state == datastore.OP_CREATED:
            return 201
        elif state == datastore.OP_DELETED:
            return 204
        elif state == datastore.OP_UPDATED:
            return 200
        else: # -1
            # This point should never be reached, unless object has been deleted
            # concurrently.
            raise exceptions.ResourceNotFound(object_id, object_type)
