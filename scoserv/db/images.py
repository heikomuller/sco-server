"""Images and Image Store - Collection of methods to manage image files,
collections of images, and their properties.
"""

import datetime
import os
import shutil
import uuid

import attribute
import datastore


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Dictionary of valid suffixes for image files and their respective Mime types.
# ------------------------------------------------------------------------------
VALID_IMGFILE_SUFFIXES = {
    '.jpg' : 'image/jpeg',
    '.jpeg' : 'image/jpeg',
    '.png' : 'image/png',
    '.gif' : 'image/gif'}


# ------------------------------------------------------------------------------
#
# Images and Image Collection Classes
#
# ------------------------------------------------------------------------------

class ImageHandle(datastore.DataObject):
    """Image Handle - Handle to access and manipulate an image file. Each object
    has an unique identifier, the timestamp of it's creation, a list of
    properties, and a reference to the (unique) directory where the image file
    is stored on disk. File name and Mime type of the image are part of the
    mandatory, immutable set of image properties.
    """
    def __init__(self, identifier, properties, directory, timestamp=None, is_active=True):
        """Initialize the image handle. The directory references a directory
        on the local disk that contains the image files. The original image
        file name is part of the property set.

        Parameters
        ----------
        identifier : datastore.ObjectId
            Unique object identifier
        properties : Dictionary
            Dictionary of subject specific properties
        directory : string
            Directory on local disk that contains image file
        timestamp : datetime, optional
            Time stamp of object creation (UTC).
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Initialize super class
        super(ImageHandle, self).__init__(
            identifier,
            datastore.OBJ_IMAGE,
            timestamp,
            properties,
            directory,
            is_active=is_active
        )


class ImageGroupHandle(datastore.DataObject):
    """Image Group Handle - Handle to access and manipulate a collection of
    image files. Each collection has an unique identifier, the timestamp of it's
    creation, a list of properties, a reference to the directory where a zipped
    tar-file conatining images in the group is stored.

    In addition to the general data object properties, image groups contain a
    list of object identifier for image objects in the collection.

    Attributes
    ----------
    images : List(string)
        List of object identifier for images in the collection
    """
    def __init__(self, identifier, properties, directory, images, options, timestamp=None, is_active=True):
        """Initialize the image group handle. The directory references a
        directory on the local disk that contains the tar-file with all images.
        The name of that tar-file is part of the property set.

        Parameters
        ----------
        identifier : datastore.ObjectId
            Unique object identifier
        properties : Dictionary
            Dictionary of subject specific properties
        directory : string
            Directory on local disk that contains images tar-file file
        images : List(GroupImage)
            List of images in the collection
        options: Dictionary(attribute.Attribute)
            Dictionary of typed attributes defining the image group options
        timestamp : datetime, optional
            Time stamp of object creation (UTC).
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Initialize super class
        super(ImageGroupHandle, self).__init__(
            identifier,
            datastore.OBJ_IMAGEGROUP,
            timestamp,
            properties,
            directory,
            is_active=is_active
        )
        # Initialize local object variables
        self.images = images
        self.options = options


class GroupImage(object):
    """Descriptor for images in an image group - Each image in an image group
    has a name and folder. Together they for a unique identifier of the image,
    just like the image identifier that is also part of the group image object.

    Attributes
    ----------
    identifier : string
        Unique identifier of the image
    folder : string
        (Sub-)folder in the grouop (default: /)
    name : string
        Image name (unique within the folder)
    """
    def __init__(self, identifier, folder, name):
        """Initialize attributes of the group image.

        Parameters
        ----------
        identifier : datastore.ObjectId
            Unique identifier of the image
        folder : string
            (Sub-)folder in the grouop (default: /)
        name : string
            Image name (unique within the folder)
        """
        self.identifier = identifier
        self.folder = folder
        self.name = name


# ------------------------------------------------------------------------------
#
# Object Stores
#
# ------------------------------------------------------------------------------

class DefaultImageManager(datastore.DefaultObjectStore):
    """Default Image Manager - Manager for images objects. Image objects are
    stored in folders that are named by a prefix of the object identifier.

    This is a default implentation that uses MongoDB as storage backend.

    Attributes
    ----------
    directory : string
        Base directory on local disk for image files.
    """
    def __init__(self, mongo_collection, base_directory):
        """Initialize the MongoDB collection and base directory where to store
        images files. Set immutable and mandatory properties.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB storing image information
        base_directory : string
            Base directory on local disk for image files. Files are stored
            in sub-directories named by the subject identifier. To avoid having
            too many files in a single directory we group these sub-directories
            in directories that start with the first two characters of the
            object identifier.
        """
        # Initialize the super class
        super(DefaultImageManager, self).__init__(
            mongo_collection,
            base_directory,
            [datastore.PROPERTY_FILENAME, datastore.PROPERTY_MIMETYPE]
        )

    def create_object(self, filename):
        """Create an image object on local disk from the given file.

        Parameters
        ----------
        filename : string
            Path to file on disk
        Returns
        -------
        ImageHandle
            Handle for created image object
        """
        # Get the file name, i.e., last component of the given absolute path
        prop_name = os.path.basename(os.path.normpath(filename))
        # Ensure that the image file has a valid suffix. Currently we do not
        # check whether the file actually is an image. If the suffix is valid
        # get the associated Mime type from the dictionary.
        prop_mime = None
        pos = prop_name.rfind('.')
        if pos >= 0:
            suffix = prop_name[pos:].lower()
            if suffix in VALID_IMGFILE_SUFFIXES:
                prop_mime = VALID_IMGFILE_SUFFIXES[suffix]
        if not prop_mime:
            raise ValueError('Unsupported image type: ' + prop_name)
        # Create a new object identifier.
        identifier = str(uuid.uuid4())
        # The sub-folder to store the image is given by the first two
        # characters of the identifier.
        image_dir = self.get_directory(identifier)
        # Create the directory if it doesn't exists
        if not os.access(image_dir, os.F_OK):
            os.makedirs(image_dir)
        # Create the initial set of properties for the new image object.
        properties = {
            datastore.PROPERTY_NAME: prop_name,
            datastore.PROPERTY_FILENAME : prop_name,
            datastore.PROPERTY_MIMETYPE : prop_mime
        }
        # Move original file to object directory
        os.rename(filename, os.path.join(image_dir, prop_name))
        # Create object handle and store it in database before returning it
        obj = ImageHandle(datastore.ObjectId(identifier), properties, image_dir)
        self.insert_object(obj)
        return obj

    def from_json(self, document):
        """Create image object from JSON document retrieved from database.

        Parameters
        ----------
        document : JSON
            Json document in database

        Returns
        -------
        ImageHandle
            Handle for image object
        """
        # Get object properties from Json document
        identifier = datastore.ObjectId(document['identifier'])
        active = document['active']
        timestamp = datetime.datetime.strptime(document['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
        properties = document['properties']
        # The directory is not materilaized in database to allow moving the
        # base directory without having to update the database.
        directory = self.get_directory(repr(identifier))
        # Cretae image handle
        return ImageHandle(identifier, properties, directory, timestamp=timestamp, is_active=active)

    def get_directory(self, identifier):
        """Implements the policy for naming directories for image objects. Image
        object directories are name by their identifier. In addition, these
        directories are grouped in parent directories named by the first two
        characters of the identifier. The aim is to avoid having too many
        sub-folders in a single directory.

        Parameters
        ----------
        identifier : datastore.ObjectId
            Unique object identifier

        Returns
        -------
        string
            Path to image objects data directory
        """
        return os.path.join(
            os.path.join(self.directory, repr(identifier)[:2]),
            repr(identifier)
        )


class DefaultImageGroupManager(datastore.DefaultObjectStore):
    """Default Image Group Manager - Manager for image collections objects.

    This is a default implentation that uses MongoDB as storage backend.

    Attributes
    ----------
    directory : string
        Base directory on local disk for image group files.
    """
    def __init__(self, mongo_collection, base_directory):
        """Initialize the MongoDB collection and base directory where to store
        images group files. Set immutable and mandatory properties.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB storing image group information
        base_directory : string
            Base directory on local disk for image group files. Files are stored
            in sub-directories named by the subject identifier.
        """
        # Initialize the super class
        super(DefaultImageGroupManager, self).__init__(
            mongo_collection,
            base_directory,
            [datastore.PROPERTY_FILENAME, datastore.PROPERTY_MIMETYPE]
        )
        # Initialize the definition of image group options attributes
        self.options_def = {
            'stimulus_pixels_per_degree' : attribute.AttributeDefinition(
                'stimulus_pixels_per_degree',
                attribute.FloatType()
            ),
            'stimulus_edge_value' : attribute.AttributeDefinition(
                'stimulus_edge_value',
                attribute.FloatType(),
                default_value=0.5
            ),
            'stimulus_aperture_edge_value' : attribute.AttributeDefinition(
                'stimulus_aperture_edge_value',
                attribute.FloatType()
            ),
            'normalized_stimulus_aperture' : attribute.AttributeDefinition(
                'normalized_stimulus_aperture',
                attribute.FloatType()
            ),
            'stimulus_gamma' : attribute.AttributeDefinition(
                'stimulus_gamma',
                attribute.ArrayType(attribute.FloatType())
            )
        }

    def create_object(self, name, images, filename):
        """Create an image group object with the given list of images. The
        file name specifies the location on local disk where the tar-file
        containing the image group files is located.

        Parameters
        ----------
        name : string
            User-provided name for the image group
        images : List(GroupImage)
            List of objects describing images in the group
        filename : string
            Location of local file containing all images in the group

        Returns
        -------
        ImageGroupHandle
            Object handle for created image group
        """
        # Raise an exception if given image group is not valied.
        self.validate_group(images)
        # Create a new object identifier.
        identifier = str(uuid.uuid4())
        # Create the initial set of properties.
        prop_filename = os.path.basename(os.path.normpath(filename))
        prop_mime = 'application/x-tar' if filename.endswith('.tar') else 'application/gzip'
        properties = {
            datastore.PROPERTY_NAME: name,
            datastore.PROPERTY_FILENAME : prop_filename,
            datastore.PROPERTY_MIMETYPE : prop_mime
        }
        # Directories are simply named by object identifier
        directory = os.path.join(self.directory, identifier)
        # Create the directory if it doesn't exists
        if not os.access(directory, os.F_OK):
            os.makedirs(directory)
        # Move original file to object directory
        os.rename(filename, os.path.join(directory, prop_filename))
        # The initial set of oprions is given by those options for which default
        # values are defined.
        options = attribute.get_default_attributes(self.options_def)
        # Create the image group object and store it in the database before
        # returning it.
        obj = ImageGroupHandle(
            datastore.ObjectId(identifier),
            properties,
            directory,
            images,
            options
        )
        self.insert_object(obj)
        return obj

    def from_json(self, document):
        """Create image group object from JSON document retrieved from database.

        Parameters
        ----------
        document : JSON
            Json document in database

        Returns
        -------
        ImageGroupHandle
            Handle for image group object
        """
        # Get object attributes from Json document
        identifier = datastore.ObjectId(document['identifier'])
        # Create list of group images from Json
        images = list()
        for grp_image in document['images']:
            images.append(GroupImage(
                datastore.ObjectId(grp_image['identifier']),
                grp_image['folder'],
                grp_image['name']
            ))
        # Directories are simply named by object identifier
        directory = os.path.join(self.directory, repr(identifier))
        # Create image group handle.
        return ImageGroupHandle(
            identifier,
            document['properties'],
            directory,
            images,
            attribute.attributes_from_json(document['options']),
            timestamp=datetime.datetime.strptime(
                document['timestamp'],
                '%Y-%m-%dT%H:%M:%S.%f'
            ),
            is_active=document['active']
        )

    def get_collections_for_image(self, image_id):
        """Get identifier of all collections that contain a given image.

        Parameters
        ----------
        image_id : datastore.ObjectId
            Unique identifierof image object

        Returns
        -------
        List(datastore.ObjectId)
            List of image collection identifier
        """
        result = []
        # Get all active collections that contain the image identifier
        for document in self.collection.find({'active' : True, 'images.identifier' : image_id.keys}):
            result.append(datastore.ObjectId(document['images.identifier']))
        return result

    def to_json(self, img_coll):
        """Create a Json-like dictionary for image group. Extends the basic
        object with an array of image identifiers.

        Parameters
        ----------
        img_coll : ImageGroupHandle

        Returns
        -------
        (JSON)
            Json-like object, i.e., dictionary.
        """
        # Get the basic Json object from the super class
        json_obj = super(DefaultImageGroupManager, self).to_json(img_coll)
        # Add list of images as Json array
        images = []
        for img_group in img_coll.images:
            images.append({
                'identifier' : img_group.identifier.keys,
                'folder' : img_group.folder,
                'name' : img_group.name
            })
        json_obj['images'] = images
        # Transform dictionary of options into list of elements, one per typed
        # attribute in the options set.
        json_obj['options'] = attribute.attributes_to_json(img_coll.options)
        return json_obj

    def update_object_attributes(self, identifier, attributes):
        """Update set of typed attributes (options) that are associated with
        a given image group. Raises a ValueError if any of the given
        attributes violates the attribute definitions associated with image
        groups.

        Parameters
        ----------
        identifier : datastore.ObjectId
            Unique object identifier
        attributes : List(attribute.Attribute)
            List of attribute instances
        """
        # Create a dictionary of options from the attribute list. The dictionary
        # allows to detect duplicate definitions of the same attribute
        options= {}
        for attr in attributes:
            if attr.name in options:
                raise ValueError('duplicate attribute: ' + attr.name)
            if not attr.name in self.options_def:
                raise ValueError('unknown attribute: ' + attr.name)
            attr_def = self.options_def[attr.name]
            if not attr_def.validate(attr.value):
                raise ValueError('invalid value for attribute: ' + attr.name)
            options[attr.name] = attr
        # Retrieve object from database and replace existing options
        img_group = self.get_object(identifier)
        img_group.options = options
        # Replace existing object in database
        self.replace_object(img_group)

    @staticmethod
    def validate_group(images):
        """Validates that the combination of folder and name for all images in
        a group is unique. Raises a ValueError exception if uniqueness
        constraint is violated.

        Parameters
        ----------
        images : List(GroupImage)
            List of images in group
        """
        image_ids = set()
        for image in images:
            key = image.folder + image.name
            if key in image_ids:
                raise ValueError('Duplicate images in group: ' + key)
            else:
                image_ids.add(key)


# ------------------------------------------------------------------------------
#
# Helper methods
#
# ------------------------------------------------------------------------------
def get_image_files(directory, files):
    """Recursively iterate through directory tree and list all files that have a
    valid image file suffix

    Parameters
    ----------
    directory : directory
        Path to directory on disk
    files : List(string)
        List of file names

    Returns
    -------
    List(string)
        List of files that have a valid image suffix
    """
    # For each file in the directory test if it is a valid image file or a
    # sub-directory.
    for f in os.listdir(directory):
        abs_file = os.path.join(directory, f)
        if os.path.isdir(abs_file):
            # Recursively iterate through sub-directories
            get_image_files(abs_file, files)
        else:
            # Add to file collection if has valid suffix
            if '.' in f and '.' + f.rsplit('.', 1)[1] in VALID_IMGFILE_SUFFIXES:
                files.append(abs_file)
    return files
