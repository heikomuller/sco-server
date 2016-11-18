""" Data Store - Basic definitons and helper methods for objects in the Standard
Cortical Observer. Primarily deals with Anatomical MRI's, Images, and the
results of model runs.

 Objects may be stored using different database backends.
"""

from abc import abstractmethod
import datetime
import os
import pymongo
import re


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

"""Object types: Every class that extends DBObject should register a unique
object type here."""

# Experimental setup object
OBJ_EXPERIMENT = 'EXPERIMENT'
# Functional Data object
OBJ_FMRI_DATA = 'FMRI'
# Single Image Object
OBJ_IMAGE = 'IMAGE'
# Collection of images
OBJ_IMAGEGROUP = 'IMG_GROUP'
# Model run prediction output
OBJ_PREDICTION = 'PREDICTION'
# Subject anatomy object
OBJ_SUBJECT = 'SUBJECT'

# Set of valid object types.
OBJECT_TYPES = set([
    OBJ_EXPERIMENT,
    OBJ_FMRI_DATA,
    OBJ_IMAGE,
    OBJ_IMAGEGROUP,
    OBJ_PREDICTION,
    OBJ_SUBJECT
])

""" Object properties: Definition of general properties that can be associated
with a database object."""

# Name of the original archive file
PROPERTY_FILENAME = 'filename'
# String representation of file type.
PROPERTY_FILETYPE = 'filetype'
# Default Mime-type of the image (based on the file name suffix)
PROPERTY_MIMETYPE = 'mimetype'
# Descriptive name (mandatory for all database objects)
PROPERTY_NAME = 'name'

"""Return codes for operations that manipulate a set of properties."""

# Requested operation illegal due to constraints on the property set
OP_ILLEGAL = 0
# Operation deleted a property from the property set
OP_DELETED = 1
# Operation created a new property in the property set
OP_CREATED = 2
# Operation updated an existing property in the property set
OP_UPDATED = 3


# ------------------------------------------------------------------------------
#
# Database Objects
#
# ------------------------------------------------------------------------------

class ObjectId(object):
    """Identifier for database objects. Identifiers are given by a list (or
    tuple) of values. For weak entities the list contains more than one element
    while strong entities have only a single element.

    The concatenation of all key values in the identifier list is assumed to
    be a single unique identifier for the object.

    Attributes
    ----------
    keys : List(string)
        List of identifier.
    """
    def __init__(self, keys):
        # At the moment the argument can be a single string, a tuple of strings,
        # or a list of string. Convert the given value into a list of strings
        if isinstance(keys, basestring):
            self.keys = [keys]
        elif isinstance(keys, tuple):
            self.keys = list(keys)
        elif isinstance(keys, list):
            self.keys = keys
        else:
            raise ValueError('invalid type for object keys')
        # Ensure that none of the key values contains the delimiter that is used
        # to build the identifier representation
        for key in self.keys:
            if self.delimiter in key:
                raise ValueError('invaid key value: ' + key)

    def __repr__(self):
        """String representation for list of keys. This is a concatenation of
        elements in the list of keys.

        Returns
        -------
        string
            Global unique string identifier
        """
        return self.delimiter.join(self.keys)

    @property
    def delimiter(self):
        """Deliminator used when concatenating identifier components to build
        unique identifier representation. Currently, the concatenation delimiter
        is '#'

        Returns
        -------
        string
            Concatenation delimiter used for identifier representation
        """
        return '#'

class DBObject(object):
    """Database Object - Base implementation of database objects. Contains the
    object identifier, type, and set of object-specific properties.

    Attributes
    ----------
    identifier : ObjectId
        Unique object identifier
    type : string
        String representation of object type (has to be in OBJECT_TYPES)
    timestamp : datetime
        Time stamp of object creation (UTC time). If None, the current
        date and time is used.
    properties : Dictionary
        Dictionary of object specific properties. Definition of mandatory
        and immutable properties are part of the object store that manages
        objects of sub-classes that extend DBObject.
    is_active : Boolean
        Flag indicating whether the object is active or has been deleted.

    """
    def __init__(self, identifier, type, timestamp, properties, is_active=True):
        """Initialize identifier, type, timestamp, and properties. Raises an
        exception if the given type is not a valid object type or if the
        manadatory property NAME is missing.

        For each object type a is_type() method should be added to this base
        class implementation.

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier
        type : string
            String representation of object type (has to be in OBJECT_TYPES)
        timestamp : datetime
            Time stamp of object creation (UTC time). If None, the current
            date and time is used.
        properties : Dictionary
            Dictionary of object specific properties. Definition of mandatory
            and immutable properties are part of the object store that manages
            objects of sub-classes that extend DBObject.
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Ensure that type is a valid object type
        if not type in OBJECT_TYPES:
            raise UnknownObjectType(type)
        # Ensure that the properties contains the mandatory property NAME
        if not PROPERTY_NAME in properties:
            raise ValueError('missing property: ' + PROPERTY_NAME)
        # Initialize object variables
        self.identifier = identifier
        self.type = type
        self.timestamp = timestamp or datetime.datetime.utcnow()
        self.properties = properties
        self.is_active = is_active

    @property
    def name(self):
        """Value for the NAME property for the object. This is a mandatory
        property for each object and should therefore never be None.

        Returns
        -------
        string
            Object name
        """
        return self.properties[PROPERTY_NAME]

    @property
    def is_experiment(self):
        """Flag indicating whether this object represents an experiment object.

        Returns
        -------
        Boolean
            True, if object is of type OBJ_EXPERIMENT
        """
        return self.type == OBJ_EXPERIMENT

    @property
    def is_fmri_data(self):
        """Flag indicating whether this object represents a functional MRI data
        object.

        Returns
        -------
        Boolean
            True, if object is of type OBJ_FMRI_DATA
        """
        return self.type == OBJ_FMRI_DATA

    @property
    def is_image(self):
        """Flag indicating whether this object represents a single image file
        object.

        Returns
        -------
        Boolean
            True, if object is of type OBJ_IMAGE
        """
        return self.type == OBJ_IMAGE

    @property
    def is_image_group(self):
        """Flag indicating whether this object represents an image group object.

        Returns
        -------
        Boolean
            True, if object is of type OBJ_IMAGEGROUP
        """
        return self.type == OBJ_IMAGEGROUP

    @property
    def is_prediction(self):
        """Flag indicating whether this object represents an prediction object.

        Returns
        -------
        Boolean
            True, if object is of type OBJ_PREDICTION
        """
        return self.type == OBJ_PREDICTION

    @property
    def is_subject(self):
        """Flag indicating whether this object represents an anatomy subject.

        Returns
        -------
        Boolean
            True, if object is of type OBJ_SUBJECT
        """
        return self.type == OBJ_SUBJECT


class DataObject(DBObject):
    """Data objects are database objects that have a directory on the local
    file system associated with them. The contents of these directories are
    dependent on the implementing object type. The directory should contain
    at least all the files that are downloadable for a particular object.

    The directory is not maintained as an object property (unlike the file name
    for example). This is done to avoid exposing the directory name to the
    outside world via the Web API that gives access to object properties. It
    also makes it easier to change directories (i.e., move data locally) without
    the need to update the underlying data store."""
    def __init__(self, identifier, type, timestamp, properties, directory, is_active=True):
        # Initialize the super class
        super(DataObject, self).__init__(identifier, type, timestamp, properties, is_active=is_active)
        # Set the objects data directory. The directory should be an absolute
        # path. However, this is not enforeced at this point.
        self.directory = directory


class ObjectListing(object):
    """Result of a list_objects operation. Contains two field: The list of
    objects in the result an the total number of objects in the database.
    """
    def __init__(self, items, total_count):
        """Initialize the object list and total count.

        Parameters
        ----------
        items : List(DBObject)
            List of objects that are subclass of DBObject.
        total_count : int
            Total number of object's of this type in the database. This number
            may be different from the size of the items list, which may only
            contain a subset of items. The total_number of objects is necessary
            for pagination of object listings in the web interface.
        """
        self.items = items
        self.total_count = total_count


# ------------------------------------------------------------------------------
#
# Object Stores
#
# ------------------------------------------------------------------------------

class ObjectStore(object):
    """Object Store - Base implementation of a storage manager for database
    objects. Each object store should implement the following interface methods:

    delete_object(identifier::string) -> True|False
    get_object(identifier::string) -> (Subclass of)DBObject
    list_objects(limit=-1, offset=-1) -> ObjectListing
    replace_object(object::(Subclass of)DBObject)
    update_object_property(identifier::string, key::string, value::string)

    Attributes
    ----------
    immutable_properties : List(string)
        List the names if immutage properties
    mandatory_properties : List(string)
        List the names of mandatory properties
    """
    def __init__(self):
        """Initialize the set of immutable and manadatory properties.
        Sub-classes may add properties to this set. Note that immutable
        properties not necessarily have to exist (not mandatory).
        """
        # List of properties that caannot be updated
        self.immutable_properties = set()
        # List of properties that are mandatory for all object in the object
        # store.
        self.mandatory_properties = set([PROPERTY_NAME])

    @abstractmethod
    def delete_object(self, identifier):
        """Delete object with given identifier. Returns the handle for the
        deleted object or None if object identifier is unknown.

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier

        Returns
        -------
        (Sub-class of)DBObject
        """
        pass

    @abstractmethod
    def exists_object(self, identifier):
        """Test if object with given identifier exists in object store and is
        active.

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier

        Returns
        -------
        Boolean
            True, if active object with given identifier exists.
        """
        pass

    @abstractmethod
    def get_download(self, identifier):
        """Get download information for object with given identifier.

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier

        Returns
        -------
        Tuple (string, string, string)
            Returns directory, file name, and mime type of downloadable file.
            Result contains all None if object does not exist.
        """
        pass

    @abstractmethod
    def get_object(self, identifier, include_inactive=False):
        """Retrieve object with given identifier from data store.

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier
        include_inactive : Boolean
            Flag indicating whether inactive (i.e., deleted) object should be
            included in the search (i.e., return an object with given
            identifier if it has been deleted or return None)

        Returns
        -------
        (Subclass of)DBObject
        """
        pass

    @abstractmethod
    def list_objects(self, limit=-1, offset=-1, parent_id=None):
        """Retrieve list of all objects from data store.

        Parameters
        ----------
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)
        parent_id : datastore.ObjectId, optional
            Parent object identifier for weak entities.

        Returns
        -------
        ObjectListing
        """
        pass

    @abstractmethod
    def replace_object(self, object):
        """Store modified object in data store. Assumes that the object exists.

        Parameters
        ----------
        object : (Subclass of)DBObject
            Replacement object. The original is identified by the unique
            object identifier.
        """
        pass

    def upsert_object_property(self, identifier, key, value=None, ignore_constraints=False):
        """Manipulate an object's property set. If the identified property does
        not exist it is created. If the value is None an existing property is
        deleted. Deleting mandatory properties results in an illegal operation.
        Updating immutable properties also results in an illegal operation.
        These constraints can be disabled using the ignore_constraints
        parameter.

        The following return values are possible:
        -1: Object does not exists
        OP_ILLEGAL: Operation violates a constraint
        OP_DELETED: Deleted
        OP_CREATED: Created
        OP_UPDATED: Updated

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier
        key : string
            Property name
        value : string
            New property value
        ignore_constraints : Boolean
            Flag indicating whether to ignore immutable and mandatory property
            constraints (True) or nore (False, Default).

        Returns
        -------
        int
        """
        # Retrieve the object with the gievn identifier. This is a (sub-)class
        # of DBObject
        obj = self.get_object(identifier)
        if not obj is None:
            # If the update affects an immutable property return OP_ILLEGAL
            if not ignore_constraints and key in self.immutable_properties:
                return OP_ILLEGAL
            # Check whether the operation is an UPSERT or DELETE. If value is
            # None we are deleting the property.
            if not value is None:
                # UPSERT
                if key in obj.properties:
                    op = OP_UPDATED
                else:
                    op = OP_CREATED
                obj.properties[key] = value
            else:
                # DELETE. Make sure the property is not mandatory
                if not ignore_constraints and key in self.mandatory_properties:
                    return OP_ILLEGAL
                elif key in obj.properties:
                    del obj.properties[key]
                op = OP_DELETED
            self.replace_object(obj)
            return op
        else:
            # No object with given identifier exists
            return -1


class MongoDBStore(ObjectStore):
    """MongoDB Object Store - Abstract implementation of a data store that uses
    MongoDB to store database objects. Implements all abstract methods of the
    super class. Object-specific implementations of this store need only to
    implement the abstract method from_json() that creates an object instance
    from a Json representation in the database.

    Attributes
    ----------
    collection : Collection
        Collection in MongoDB where object information is stored
    """
    def __init__(self, mongo_collection):
        """Initialize the MongoDB collection where objects are being stored.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB
        """
        super(MongoDBStore, self).__init__()
        self.collection = mongo_collection

    def delete_object(self, identifier):
        """Delete the entry with given identifier in the database. Returns the
        handle for the deleted object or None if object identifier is unknown.

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier

        Returns
        -------
        (Sub-class of)DBObject
        """
        # Get object to ensure that it exists.
        db_object = self.get_object(identifier)
        # Set active flag to False if object exists.
        if not db_object is None:
            # Delete object with given identifier. Result contains object count
            # to determine if the object existed or not
            self.collection.update_one({"_id": repr(identifier)}, {'$set' : {'active' : False}})
        # Return retrieved object or None if it didn't exist.
        return db_object

    def exists_object(self, identifier):
        """Override ObjectStore.exists_object.

        Parameters
        ----------
        identifier : string
            Unique object identifier

        Returns
        -------
        Boolean
            True, if active object with given identifier exists.
        """
        # Return True if query for object identifier with active flag on returns
        # a result.
        return self.collection.find({'_id': repr(identifier), 'active' : True}).count() > 0

    @abstractmethod
    def from_json(self, document):
        """Create a database object from a given Json document. Implementation
        depends on the type of object that is being stored.

        Parameters
        ----------
        document : JSON
            Json representation of the object

        Returns
        (Sub-class of)DBObject
        """
        pass

    def get_object(self, identifier, include_inactive=False):
        """Retrieve object with given identifier from the database.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        include_inactive : Boolean
            Flag indicating whether inactive (i.e., deleted) object should be
            included in the search (i.e., return an object with given
            identifier if it has been deleted or return None)

        Returns
        -------
        (Sub-class of)DBObject
            The database object with given identifier or None if no object
            with identifier exists.
        """
        # Find all objects with given identifier. The result size is expected
        # to be zero or one
        query = {'_id': repr(identifier)}
        if not include_inactive:
            query['active'] = True
        cursor = self.collection.find(query)
        if cursor.count() > 0:
            return self.from_json(cursor.next())
        else:
            return None

    def insert_object(self, db_object):
        """Create new entry in the database.

        Parameters
        ----------
        db_object : (Sub-class of)DBObject
        """
        # Create object using the  to_json() method. Use the object
        # identifier as ObjectId.
        obj = self.to_json(db_object)
        obj['active'] = True
        self.collection.insert_one(obj)

    def list_objects(self, query=None, limit=-1, offset=-1, parent_id=None):
        """List of all objects in the database. Optinal parameter limit and
        offset for pagination. A dictionary of key,value-pairs can be given as
        query for object properties.

        Parameters
        ----------
        query : Dictionary
            Filter objects by property-value pairs defined by dictionary.
        limit : int
            Limit number of items in the result set
        offset : int
            Set offset in list (order as defined by object store)
        parent_id : datastore.ObjectId, optional
            Parent object identifier for weak entities.

        Returns
        -------
        ObjectListing
        """
        result = []
        # Build the document query
        doc = {'active' : True}
        if not query is None:
            for key in query:
                doc['properties.' + key] = query[key]
        # Add a regular expression to the query that filters based on _id prefix
        # defined by given parent identifier (if given).
        if not parent_id is None:
            doc['_id'] = re.compile(repr(parent_id) + parent_id.delimiter)
        # Iterate over all objects in the MongoDB collection and add them to
        # the result
        coll = self.collection.find(doc).sort([('timestamp', pymongo.DESCENDING)])
        count = 0
        for document in coll:
            # We are done if the limit is reached. Test first in case limit is
            # zero.
            if limit >= 0 and len(result) == limit:
                break
            if offset < 0 or count >= offset:
                result.append(self.from_json(document))
            count += 1
        return ObjectListing(result, coll.count())

    def replace_object(self, db_object):
        """Update an existing object (identified by the object identifier) with
        the given modified object.

        Parameters
        ----------
        db_object : (Sub-class of)DBObject
            Replacement object
        """
        # To enable provenance traces objects are not actually deleted from the
        # database. Instead, their active flag is set to False.
        obj = self.to_json(db_object)
        obj['active'] = True
        self.collection.replace_one({'_id' : repr(db_object.identifier), 'active' : True}, obj)

    def to_json(self, db_obj):
        """Create a Json-like dictionary for objects managed by this object
        store.

        Parameters
        ----------
        db_obj : (Sub-class of)DBObject

        Returns
        -------
        (JSON)
            Json-like object, i.e., dictionary.
        """
        # Make sure to use the string representation of the object identifier
        # as document identifier. Include the identifier's keys in the document
        # that is stored in the database (only if there is more than one key,
        # i.e., the identifier cannot be generated from the document _id.
        return {
            '_id' : repr(db_obj.identifier),
            'identifier' : db_obj.identifier.keys,
            'timestamp' : str(db_obj.timestamp.isoformat()),
            'properties' : db_obj.properties}


class DefaultObjectStore(MongoDBStore):
    """Extension of MongoDB store with an directory to store external files.
    Many objects in the Standard Cortical Observer have additional files
    attached to them. Thus, in most cases we currently use a combination of
    MongoDB and disk storage, i.e., this default object store.


    Attributes
    ----------
    directory : string
        Base directory on local disk for object files.
    """
    def __init__(self, mongo_collection, base_directory, properties=[]):
        """Initialize the MongoDB collection and base directory where to store
        object files. Set immutable and mandatory properties.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB storing object information
        base_directory : string
            Base directory on local disk for object files.
        properties : List(String)
            List of manadatory and immutable properties
        """
        # Set the MongoDB collection of the super class
        super(DefaultObjectStore, self).__init__(mongo_collection)
        # Raise an exception if the base directory does not exist or is not
        # a directory
        if not os.access(base_directory, os.F_OK):
            raise ValueError('Directory does not exist: ' + base_directory)
        if not os.path.isdir(base_directory):
            raise ValueError('Not a directory: ' + base_directory)
        self.directory = base_directory
        # Set immutable and mandatory properties
        for prop in properties:
            self.immutable_properties.add(prop)
            self.mandatory_properties.add(prop)


    def get_download(self, identifier):
        """Get download information for object with given identifier. Assumes
        that the object has an attribute directory as well as file name and mime
        type properties.

        Parameters
        ----------
        identifier : ObjectId
            Unique object identifier

        Returns
        -------
        Tuple (string, string, string)
            Returns directory, file name, and mime type of downloadable file.
            Result contains all None if object does not exist.
        """
        # Retrieve image from database. Abort if it does not exist.
        db_obj = self.get_object(identifier)
        if db_obj is None:
            return None
        # Return image's data directory, original file name, and mime type
        directory = db_obj.directory
        filename = db_obj.properties[PROPERTY_FILENAME]
        mime_type = db_obj.properties[PROPERTY_MIMETYPE]
        return directory, filename, mime_type
