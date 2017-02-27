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

""" Object properties: Definition of general properties that can be associated
with a database object."""

# Name of the original archive file
PROPERTY_FILENAME = 'filename'
# Size of file in bytes.
PROPERTY_FILESIZE = 'filesize'
# String representation of file type.
PROPERTY_FILETYPE = 'filetype'
# Default Mime-type of the image (based on the file name suffix)
PROPERTY_MIMETYPE = 'mimetype'
# Descriptive name (mandatory for all database objects)
PROPERTY_NAME = 'name'
# Object state
PROPERTY_STATE = 'state'


# ------------------------------------------------------------------------------
#
# Database Objects
#
# ------------------------------------------------------------------------------

class ObjectHandle(object):
    """Base implementation of object handles to access and manipulate database
    objects. Each handle contains the object identifier, type, and a set of
    object-specific properties represented as a dictionary of key,value-pairs.

    For each class of objects accessible in the SCO Data Store and sub-class
    of object handle is generated. To allow type checking of objects include
    a is_type property in this base class and set the return value to True in
    the aub-class.

    Attributes
    ----------
    identifier : string
        Unique object identifier
    timestamp : datetime
        Time stamp of object creation (UTC time). If None, the current
        date and time is used.
    properties : Dictionary
        Dictionary of object specific properties. Definition of mandatory
        and immutable properties are part of the object store that manages
        objects of sub-classes that extend the object handle.
    is_active : Boolean
        Flag indicating whether the object is active or has been deleted.

    Properties
    ----------
    is_experiment : Booloean
        True, if object is ExperimentHandle.
    is_functional_data : Boolean
        True, if object is FunctionalDataHandle.
    is_image : Boolean
        True, if object is ImageHandle.
    is_image_group : Boolean
        True, if object is ImageGroupHandle.
    is_model_run : Boolean
        True, if object is PredictionHandle.
    is_subject : Boolean
        True, if object is SubjectHandle.
    """
    def __init__(self, identifier, timestamp, properties, is_active=True):
        """Initialize identifier, type, timestamp, and properties. Raises an
        exception if the given type is not a valid object type or if the
        manadatory property NAME is missing.

        For each object type a is_type() method should be added to this base
        class implementation.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        timestamp : datetime
            Time stamp of object creation (UTC time). If None, the current
            date and time is used.
        properties : Dictionary
            Dictionary of object specific properties. Definition of mandatory
            and immutable properties are part of the object store that manages
            objects of sub-classes that extend the object handle.
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Ensure that the properties contains the mandatory property NAME
        if not PROPERTY_NAME in properties:
            raise ValueError('missing property: ' + PROPERTY_NAME)
        # Initialize object variables
        self.identifier = identifier
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
            True, if object is ExperimentHandle. By default the result is False
            in the base class.
        """
        return False

    @property
    def is_functional_data(self):
        """Flag indicating whether this object represents a functional MRI data
        object.

        Returns
        -------
        Boolean
            True, if object is FunctionalDataHadnle. By default the result is
            False in the base class.
        """
        return False

    @property
    def is_image(self):
        """Flag indicating whether this object represents a single image file
        object.

        Returns
        -------
        Boolean
            True, if object is ImageHandle.  By default the result is False
            in the base class.
        """
        return False

    @property
    def is_image_group(self):
        """Flag indicating whether this object represents an image group object.

        Returns
        -------
        Boolean
            True, if object isImageGroupHandle. By default the result is False
            in the base class.
        """
        return False

    @property
    def is_model_run(self):
        """Flag indicating whether this object represents an prediction object.

        Returns
        -------
        Boolean
            True, if object is Predictionhandle. By default the result is False
            in the base class.
        """
        return False

    @property
    def is_subject(self):
        """Flag indicating whether this object represents an anatomy subject.

        Returns
        -------
        Boolean
            True, if object is Subjecthandle. By default the result is False
            in the base class.
        """
        return False


class DataObjectHandle(ObjectHandle):
    """Data objects are database objects that have a directory on the local
    file system associated with them. The contents of these directories are
    dependent on the implementing object type. The directory should contain
    at least all the files that are downloadable for a particular object.

    The directory is not maintained as an object property (unlike the file name
    for example). This is done to avoid exposing the directory name to the
    outside world via the Web API that gives access to object properties. It
    also makes it easier to change directories (i.e., move data locally) without
    the need to update the underlying data store.

    Attributes
    ----------
    directory : string
        (Absolute) path to local directory containing object's data files.
    """
    def __init__(self, identifier, timestamp, properties, directory, is_active=True):
        """Initialize basic handle properties and data directory.

        Parameters
        ----------
        directory : string
            (Absolute) path to local directory containing object's data files.
        """
        # Initialize the super class
        super(DataObjectHandle, self).__init__(identifier, timestamp, properties, is_active=is_active)
        # Set the objects data directory. The directory should be an absolute
        # path. However, this is not enforeced at this point.
        self.directory = directory


class ObjectListing(object):
    """Result of a list_objects operation. Contains two field: The list of
    objects in the result an the total number of objects in the database.

    Attributes
    ----------
    items : List(ObjectHandle | image.GroupImage)
        List of objects that are subclass of ObjectHandle.
    limit : int
        Result has been limited to not include all items (or -1 for all)
    offset : int
        Offset in list (order as defined by object store)
    total_count : int
        Total number of object's of this type in the database. This number
        may be different from the size of the items list, which may only
        contain a subset of items. The total_number of objects is necessary
        for pagination of object listings in the web interface.
    """
    def __init__(self, items, offset, limit, total_count):
        """Initialize the object list and total count.

        Parameters
        ----------
        items : List(ObjectHandle)
            List of objects that are subclass of ObjectHandle.
        limit : int
            Result has been limited to not include all items (or -1 for all)
        offset : int
            Offset in list (order as defined by object store)
        total_count : int
            Total number of object's of this type in the database. This number
            may be different from the size of the items list, which may only
            contain a subset of items. The total_number of objects is necessary
            for pagination of object listings in the web interface.
        """
        self.items = items
        self.offset = offset
        self.limit = limit
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
    get_object(identifier::string) -> (Subclass of)ObjectHandle
    list_objects(limit=-1, offset=-1) -> ObjectListing
    replace_object(object::(Subclass of)ObjectHandle)
    update_object_property(identifier::string, key::string, value::string)

    Attributes
    ----------
    immutable_properties : List(string)
        List the names if immutage properties
    mandatory_properties : List(string)
        List the names of mandatory properties
    """
    def __init__(self, properties=[]):
        """Initialize the set of immutable and manadatory properties.
        Sub-classes may add properties to this set. Note that immutable
        properties not necessarily have to exist (not mandatory).

        Parameters
        ----------
        properties : List(String)
            List of manadatory and immutable properties
        """
        # List of properties that caannot be updated
        self.immutable_properties = set()
        # List of properties that are mandatory for all object in the object
        # store.
        self.mandatory_properties = set([PROPERTY_NAME])
        # Set immutable and mandatory properties
        for prop in properties:
            self.immutable_properties.add(prop)
            self.mandatory_properties.add(prop)

    @abstractmethod
    def delete_object(self, identifier, erase=False):
        """Delete object with given identifier. Returns the handle for the
        deleted object or None if object identifier is unknown.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        erase : Boolean, optional
            If true, the record will be deleted from the database. Otherwise,
            the active flag will be set to False to support provenance tracking.

        Returns
        -------
        (Sub-class of)ObjectHandle
        """
        pass

    @abstractmethod
    def exists_object(self, identifier):
        """Test if object with given identifier exists in object store and is
        active.

        Parameters
        ----------
        identifier : string
            Unique object identifier

        Returns
        -------
        Boolean
            True, if active object with given identifier exists.
        """
        pass

    @abstractmethod
    def get_object(self, identifier, include_inactive=False):
        """Retrieve object with given identifier from data store.

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
        (Subclass of)ObjectHandle
        """
        pass

    @abstractmethod
    def list_objects(self, limit=-1, offset=-1):
        """Retrieve list of all objects from data store.

        Parameters
        ----------
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)

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
        object : (Subclass of)ObjectHandle
            Replacement object. The original is identified by the unique
            object identifier.
        """
        pass

    def upsert_object_property(self, identifier, properties, ignore_constraints=False):
        """Manipulate an object's property set. Inserts or updates properties in
        given dictionary. If a property key does not exist in the object's
        property set it is created. If the value is None an existing property is
        deleted.

        Existing object properties that are not present in the given property
        set remain unaffacted.

        Deleting mandatory properties or updating immutable properties results
        in a ValueError. These constraints can be disabled using the
        ignore_constraints parameter.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        properties : Dictionary()
            Dictionary of property names and their new values.
        ignore_constraints : Boolean
            Flag indicating whether to ignore immutable and mandatory property
            constraints (True) or nore (False, Default).

        Returns
        -------
        ObjectHandle
            Handle to updated object or None if object does not exist
        """
        # Retrieve the object with the gievn identifier. This is a (sub-)class
        # of ObjectHandle
        obj = self.get_object(identifier)
        if not obj is None:
            # Modify property set of retrieved object handle. Raise exception if
            # and of the upserts is not valid.
            for key in properties:
                value = properties[key]
                # If the update affects an immutable property raise exception
                if not ignore_constraints and key in self.immutable_properties:
                    raise ValueError('update to immutable property: ' + key)
                # Check whether the operation is an UPSERT (value != None) or
                # DELETE (value == None)
                if not value is None:
                    obj.properties[key] = value
                else:
                    # DELETE. Make sure the property is not mandatory
                    if not ignore_constraints and key in self.mandatory_properties:
                        raise ValueError('delete mandatory property: ' + key)
                    elif key in obj.properties:
                        del obj.properties[key]
            # Update object in database
            self.replace_object(obj)
        # Return object handle
        return obj

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
    def __init__(self, mongo_collection, properties=[]):
        """Initialize the MongoDB collection where objects are being stored.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB
        properties : List(String)
            List of manadatory and immutable properties
        """
        super(MongoDBStore, self).__init__(properties)
        self.collection = mongo_collection

    def delete_object(self, identifier, erase=False):
        """Delete the entry with given identifier in the database. Returns the
        handle for the deleted object or None if object identifier is unknown.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        erase : Boolean, optinal
            If true, the record will be deleted from the database. Otherwise,
            the active flag will be set to False to support provenance tracking.

        Returns
        -------
        (Sub-class of)ObjectHandle
        """
        # Get object to ensure that it exists.
        db_object = self.get_object(identifier)
        # Set active flag to False if object exists.
        if not db_object is None:
            if erase:
                # Erase object from database
                self.collection.delete_many({"_id": identifier})
            else:
                # Delete object with given identifier by setting active flag
                # to False
                self.collection.update_one({"_id": identifier}, {'$set' : {'active' : False}})
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
        return self.collection.find({'_id': identifier, 'active' : True}).count() > 0

    @abstractmethod
    def from_json(self, document):
        """Create a database object from a given Json document. Implementation
        depends on the type of object that is being stored.

        Parameters
        ----------
        document : JSON
            Json representation of the object

        Returns
        (Sub-class of)ObjectHandle
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
        (Sub-class of)ObjectHandle
            The database object with given identifier or None if no object
            with identifier exists.
        """
        # Find all objects with given identifier. The result size is expected
        # to be zero or one
        query = {'_id': identifier}
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
        db_object : (Sub-class of)ObjectHandle
        """
        # Create object using the  to_json() method.
        obj = self.to_json(db_object)
        obj['active'] = True
        self.collection.insert_one(obj)

    def list_objects(self, query=None, limit=-1, offset=-1):
        """List of all objects in the database. Optinal parameter limit and
        offset for pagination. A dictionary of key,value-pairs can be given as
        addictional query condition for document properties.

        Parameters
        ----------
        query : Dictionary
            Filter objects by property-value pairs defined by dictionary.
        limit : int
            Limit number of items in the result set
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
        """
        result = []
        # Build the document query
        doc = {'active' : True}
        if not query is None:
            for key in query:
                doc[key] = query[key]
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
        return ObjectListing(result, offset, limit, coll.count())

    def replace_object(self, db_object):
        """Update an existing object (identified by the object identifier) with
        the given modified object.

        Parameters
        ----------
        db_object : (Sub-class of)ObjectHandle
            Replacement object
        """
        # To enable provenance traces objects are not actually deleted from the
        # database. Instead, their active flag is set to False.
        obj = self.to_json(db_object)
        obj['active'] = True
        self.collection.replace_one({'_id' : db_object.identifier, 'active' : True}, obj)

    def to_json(self, db_obj):
        """Create a Json-like dictionary for objects managed by this object
        store.

        Parameters
        ----------
        db_obj : (Sub-class of)ObjectHandle

        Returns
        -------
        (JSON)
            Json-like object, i.e., dictionary.
        """
        # Base Json serialization for database objects
        return {
            '_id' : db_obj.identifier,
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
        super(DefaultObjectStore, self).__init__(mongo_collection, properties)
        # Raise an exception if the base directory does not exist or is not
        # a directory
        if not os.access(base_directory, os.F_OK):
            raise ValueError('directory does not exist: ' + base_directory)
        if not os.path.isdir(base_directory):
            raise ValueError('not a directory: ' + base_directory)
        self.directory = base_directory
