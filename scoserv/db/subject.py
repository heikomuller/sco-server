"""Brain Anatomy Subject - Collection of methods to manage brain anatomy MRI
scans of subjects in experiments. Contains objects to represent and manipulate
anatomy files on local disk.
"""

import datetime
import neuropythy as neuro
import os
import shutil
import tarfile
import tempfile
import uuid

import datastore


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

"""Names of sub-folders in subject directories."""

# Unpacked Freesurfer directory
DATA_DIRECTORY = 'data'
# Folder for original upload file
UPLOAD_DIRECTORY = 'upload'

"""File types: Define supported file types when creating a new anatomy object
on local disk from a given file. Currently, only Freesurfer anatomy archives
are being supported.
"""

# Freesurfer Directory
FILE_TYPE_FREESURFER_DIRECTORY = 'FREESURFER-DIRECTORY'

# List of supported file types
FILE_TYPES = [FILE_TYPE_FREESURFER_DIRECTORY]


# ------------------------------------------------------------------------------
#
# Database Objects
#
# ------------------------------------------------------------------------------
class SubjectHandle(datastore.DataObject):
    """Subject Handle - Handle to access and manipulate a brain anatomy object.
    Each object has an unique identifier, the timestamp of it's creation, a list
    of properties, a file type, and a reference to the directory on disk where
    all anatomy files are being stored.

    Attributes
    ----------
    data_directory : string
        (Absolute) Path to directory containing unpacked data files
    upload_directory : string
        (Absolute) Path to directory containing original upload file
    """
    def __init__(self, identifier, properties, directory, timestamp=None, is_active=True):
        """Initialize the subject handle. The directory references a directory
        on the local disk that contains the anatomy data files necessary as
        input when running a model locally.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        properties : Dictionary
            Dictionary of subject specific properties
        directory : string
            Directory on local disk that contains anatomy files
        timestamp : datetime, optional
            Time stamp of object creation (UTC).
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Throw an exception of file type is not supported
        file_type = properties[datastore.PROPERTY_FILETYPE]
        if file_type is None:
            raise ValueError('Missing property: ' + datastore.PROPERTY_FILETYPE)
        if not file_type in FILE_TYPES:
            raise ValueError('Unknown file type: ' + str(file_type))
        # Initialize super class
        super(SubjectHandle, self).__init__(
            identifier,
            datastore.OBJ_SUBJECT,
            timestamp,
            properties,
            directory,
            is_active=is_active
        )
        # Set data and upload directory
        self.data_directory = os.path.join(directory, DATA_DIRECTORY)
        self.upload_directory = os.path.join(directory, UPLOAD_DIRECTORY)


# ------------------------------------------------------------------------------
#
# Object Store
#
# ------------------------------------------------------------------------------
class DefaultSubjectManager(datastore.DefaultObjectStore):
    """Default Subject Manager - Manager for anatomy objects. In addition to the
    inherited methods from the data store, each Subject Manager is expected to
    implement the following interface methods:

    upload_freesurfer_archive(filename::string, type::string) -> SubjectHandle

    This is a default implentation that uses MongoDB as storage backend.

    Attributes
    ----------
    directory : string
        Base directory on local disk for subject files.
    """
    def __init__(self, mongo_collection, base_directory):
        """Initialize the MongoDB collection and base directory where to store
        subjects and anatomy MRI files.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB storing subject information
        base_directory : string
            Base directory on local disk for anatomy files. Files are stored
            in sub-directories named by the subject identifier.
        """
        # The original name of uploaded files is a mandatory and immutable
        # property. This name is used as file name when downloading subject
        # data. The file type and mime type do not change either.
        properties = [
            datastore.PROPERTY_FILENAME,
            datastore.PROPERTY_FILETYPE,
            datastore.PROPERTY_MIMETYPE
        ]
        # Initialize the super class
        super(DefaultSubjectManager, self).__init__(
            mongo_collection,
            base_directory,
            properties
        )

    def from_json(self, document):
        """Create subject object from JSON document retrieved from database.
        Overwrites super class method since downloadable subject data is
        stored in a sub-folder of the object's directory.

        Parameters
        ----------
        document : JSON
            Json document in database

        Returns
        -------
        SubjectHandle
            Handle for subject
        """
        identifier = str(document['_id'])
        active = document['active']
        # The directory is not materilaized in database to allow moving the
        # base directory without having to update the database.
        directory = os.path.join(self.directory, identifier)
        timestamp = datetime.datetime.strptime(document['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
        properties = document['properties']
        return SubjectHandle(identifier, properties, directory, timestamp=timestamp, is_active=active)

    def get_download(self, identifier):
        """Get download information for object with given identifier.

        Parameters
        ----------
        identifier : string
            Unique object identifier

        Returns
        -------
        Tuple (string, string, string)
            Returns directory, file name, and mime type of downloadable file.
            Result contains all None if object does not exist.
        """
        # Retrieve subject from database. Abort if it does not exist.
        subject = self.get_object(identifier)
        if subject is None:
            return None, None, None
        # Return subjects upload directory, original file name, and mime type
        directory = subject.upload_directory
        filename = subject.properties[datastore.PROPERTY_FILENAME]
        mime_type = subject.properties[datastore.PROPERTY_MIMETYPE]
        return directory, filename, mime_type

    def upload_file(self, filename, file_type=FILE_TYPE_FREESURFER_DIRECTORY):
        """Create an anatomy object on local disk from the given file.
        Currently, only Freesurfer anatomy directories are supported. Expects a
        tar file.

        Parameters
        ----------
        filename : string
            Name of the (uploaded) file
        file_type : string
            File type (currently expects FILE_TYPE_FREESURFER_DIRECTORY)

        Returns
        -------
        SubjectHandle
            Handle for created subject in database
        """
        # We currently only support one file type (i.e., FREESURFER_DIRECTORY).
        if file_type != FILE_TYPE_FREESURFER_DIRECTORY:
            raise ValueError('Unsupported file type: ' + file_type)
        return self.upload_freesurfer_archive(filename)

    def upload_freesurfer_archive(self, filename):
        """Create an anatomy object on local disk from a Freesurfer anatomy
        tar file.

        Parameters
        ----------
        filename : string
            Name of the (uploaded) file

        Returns
        -------
        SubjectHandle
            Handle for created subject in database
        """
        # At this point we expect the file to be a (compressed) tar archive.
        # Extract the archive contents into a new temporary directory
        temp_dir = tempfile.mkdtemp()
        try:
            tf = tarfile.open(name=filename, mode='r')
            tf.extractall(path=temp_dir)
        except (tarfile.ReadError, IOError) as err:
            # Clean up in case there is an error during extraction
            shutil.rmtree(temp_dir)
            raise err
        # Find a folder that contains sub-folders 'surf' and 'mri'. These
        # are the only folders we keep in the new anatomy folder. Raise an
        # error if no such folder esists
        freesurf_dir = get_freesurfer_dir(temp_dir)
        if not freesurf_dir:
            # Remove anatomy directory and extracted files
            shutil.rmtree(temp_dir)
            raise ValueError('Not a valid subject directory')
        # Create a new identifier. This identifier will be used as the
        # directory name. Because of the latter we (rarely) have to try
        # different identifier until we get one that does not reference an
        # existing directory.
        identifier = None
        while not identifier:
            identifier = str(uuid.uuid4())
            subject_dir = os.path.join(self.directory, identifier)
            # Test if the identifier references an existing directory. If so,
            # create a new identifier and test again.
            if os.access(subject_dir, os.F_OK):
                identifier = None
        # Create the initial set of properties for the new anatomy object. The
        # name is derived from the filename minus any known extensions
        prop_filename = os.path.basename(os.path.normpath(filename))
        prop_name = prop_filename
        # Based on the valid list of suffixes the file is either a tar-file
        # or a zipped tar-file.
        prop_mime = 'application/x-tar' if filename.endswith('.tar') else 'application/gzip'
        for suffix in ['.tar', '.tgz', '.tar.gz']:
            if prop_name.endswith(suffix):
                prop_name = prop_name[:-len(suffix)]
                break
        properties = {
            datastore.PROPERTY_FILENAME: prop_filename,
            datastore.PROPERTY_FILETYPE : FILE_TYPE_FREESURFER_DIRECTORY,
            datastore.PROPERTY_MIMETYPE : prop_mime,
            datastore.PROPERTY_NAME: prop_name
        }
        # Create the directory for the anatomy object, the unpacked data files
        # and the original uploaded file (for download).
        os.mkdir(subject_dir)
        data_dir = os.path.join(subject_dir, DATA_DIRECTORY)
        os.mkdir(data_dir)
        upload_dir = os.path.join(subject_dir, UPLOAD_DIRECTORY)
        os.mkdir(upload_dir)
        # Move all sub-folders from the Freesurfer directory to the new anatomy
        # data directory
        for f in os.listdir(freesurf_dir):
            sub_folder = os.path.join(freesurf_dir, f)
            if os.path.isdir(sub_folder):
                shutil.move(sub_folder, data_dir)
        # Move original upload file to upload directory
        os.rename(filename, os.path.join(upload_dir, prop_filename))
        # Remove the temp directory
        shutil.rmtree(temp_dir)
        # Use current time in UTC as the object's timestamp
        obj = SubjectHandle(identifier, properties, subject_dir)
        self.insert_object(obj)
        return obj


# ------------------------------------------------------------------------------
#
# Helper methods
#
# ------------------------------------------------------------------------------

def get_freesurfer_dir(directory):
    """Test if a directory is a Freesurfer anatomy directory. Currently, the
    only test is whether there are sub-folders with name 'surf' and 'mri'.
    Processes all sub-folders recursively until a freesurfer directory is found.
    If no matching folder is found the result is None

    Parameters
    ----------
    directory : string
        Directory on local disk containing unpacked files

    Returns
    -------
    string
        Sub-directory containing folders 'surf' and 'mri' or None if no such
        directory is found.
    """
    dir_files = [f for f in os.listdir(directory)]
    # Look for sub-folders 'surf' and 'mri'
    if 'surf' in dir_files and 'mri' in dir_files:
        # Use neuropythy's freesurfer_subject method to test whether the
        # directory is actually a freesurfer subject directoy
        if not neuro.freesurfer_subject(directory) is None:
            return directory
    # Directory is not a valid freesurfer directory. Continue to search
    # recursively until a matching directory is found.
    for f in os.listdir(directory):
        sub_dir = os.path.join(directory, f)
        if os.path.isdir(sub_dir):
            if get_freesurfer_dir(sub_dir):
                return sub_dir
    # The given directory does not contain a freesurfer anatomy directory
    return None
