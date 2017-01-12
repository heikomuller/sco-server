"""Functional Data - Collection of methods to manage brain response MRI
scans of subjects in experiments. Contains objects to represent and manipulate
functional data files on local disk.
"""

import datetime
import os
import shutil
import uuid

import datastore


# ------------------------------------------------------------------------------
#
# Database Objects
#
# ------------------------------------------------------------------------------
class FunctionalDataHandle(datastore.DataObjectHandle):
    """Handle to access and manipulate brain responses MRI data object. Each
    object has an unique identifier, the timestamp of it's creation, a list of
    properties, and a reference to the archive file on disk containing the
    functional data files.

    Functional data may either be generated by fMRI's or as a result of
    predictive model runs.

    Attributes
    ----------
    directory : string
        (Absolute) Path to directory conatining functional data archive file
    """
    def __init__(self, identifier, properties, directory, timestamp=None, is_active=True):
        """Initialize the object handle. The directory references a directory
        on the local disk that contains the functional data archive file.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        properties : Dictionary
            Dictionary of subject specific properties
        directory : string
            Directory conatining functional data archive file
        timestamp : datetime, optional
            Time stamp of object creation (UTC).
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Initialize super class
        super(FunctionalDataHandle, self).__init__(
            identifier,
            timestamp,
            properties,
            directory,
            is_active=is_active
        )

    @property
    def is_functional_data(self):
        """Override the is_functional_data property of the base class."""
        return True

    @property
    def data_file(self):
        """Original uploaded data file the subject was created from.

        Returns
        -------
        File-type object
            Reference to file on local disk
        """
        return os.path.join(self.directory, self.properties[datastore.PROPERTY_FILENAME])


# ------------------------------------------------------------------------------
#
# Object Store
#
# ------------------------------------------------------------------------------
class DefaultFunctionalDataManager(datastore.DefaultObjectStore):
    """Manager for functional data objects. Implements create_object method that
    creates function adata objects in database from a given archive file.

    This is a default implentation that uses MongoDB as storage backend.

    Attributes
    ----------
    directory : string
        Base directory on local disk for functional data files.
    """
    def __init__(self, mongo_collection, base_directory):
        """Initialize the MongoDB collection and base directory where to store
        functional data MRI files.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB storing functional data information
        base_directory : string
            Base directory on local disk for anatomy files. Files are stored
            in sub-directories named by the object identifier.
        """
        # The original name of uploaded files is a mandatory and immutable
        # property. This name is used as file name when downloading subject
        # data. The file type and mime type do not change either.
        properties = [
            datastore.PROPERTY_FILENAME,
            datastore.PROPERTY_MIMETYPE
        ]
        # Initialize the super class
        super(DefaultFunctionalDataManager, self).__init__(
            mongo_collection,
            base_directory,
            properties
        )

    def create_object(self, filename):
        """Create a functional data object for the given file. Currently, no
        tests are performed that the file contains valid data. Expects the file
        to be a valid tar archive. The file will be copied into the data
        object's folder on the local disk.

        Parameters
        ----------
        filename : string
            Name of the (uploaded) file

        Returns
        -------
        FunctionalDataHandle
            Handle for created functional data object in database
        """

        # Get the file name, i.e., last component of the given absolute path
        prop_name = os.path.basename(os.path.normpath(filename))
        # Ensure that the uploaded file has a valid suffix. Currently no tests
        # are performed to ensure that the file actually conatains any data.
        if prop_name.endswith('.tar'):
            prop_mime = 'application/x-tar'
        elif prop_name.endswith('.tar.gz') or prop_name.endswith('.tgz'):
            prop_mime =  'application/gzip'
        else:
            raise ValueError('unsupported file type: ' + prop_name)
        # Create a new object identifier.
        identifier = str(uuid.uuid4())
        # The object directory is given by the object identifier.
        object_dir = os.path.join(self.directory, identifier)
        # Create the directory if it doesn't exists
        if not os.access(object_dir, os.F_OK):
            os.makedirs(object_dir)
        # Create the initial set of properties for the new image object.
        properties = {
            datastore.PROPERTY_NAME: prop_name,
            datastore.PROPERTY_FILENAME : prop_name,
            datastore.PROPERTY_MIMETYPE : prop_mime
        }
        # Move original file to object directory
        shutil.copyfile(filename, os.path.join(object_dir, prop_name))
        # Create object handle and store it in database before returning it
        obj = FunctionalDataHandle(
            identifier,
            properties,
            object_dir
        )
        self.insert_object(obj)
        return obj

    def from_json(self, document):
        """Create functional data object from JSON document retrieved from
        database.

        Parameters
        ----------
        document : JSON
            Json document in database

        Returns
        -------
        FunctionalDataHandle
            Handle for functional data object
        """
        identifier = str(document['_id'])
        active = document['active']
        # The directory is not materilaized in database to allow moving the
        # base directory without having to update the database.
        directory = os.path.join(self.directory, identifier)
        timestamp = datetime.datetime.strptime(document['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
        properties = document['properties']
        return FunctionalDataHandle(identifier, properties, directory, timestamp=timestamp, is_active=active)
