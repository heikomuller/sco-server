"""Experiments - Collection of methods to experiment configurations for SCO
model predictions. Contains objects to represent and manipulate experiment
objects.
"""

import datetime
import uuid

import datastore


# ------------------------------------------------------------------------------
#
# Database Objects
#
# ------------------------------------------------------------------------------
class ExperimentHandle(datastore.DBObject):
    """Experiment Handle - Handle to access and manipulate an object
    representing an experiment configuration. Each experiment encapsules a
    subject, an image group, and an optional functional data object. For each
    referenced object the unique object identifier is maintained.

    Attributes
    ----------
    subject : string
        Unique identifier of experiment subject
    images: string
        Unique identifier of used image group
    fmri_data : string, optional
        Unique identifier of functional MRI data for experiment subject
    """
    def __init__(self, identifier, properties, subject, images, fmri_data=None, timestamp=None, is_active=True):
        """Initialize the subject handle. The directory references a directory
        on the local disk that contains the anatomy data files necessary as
        input when running a model locally.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        properties : Dictionary
            Dictionary of experiment specific properties
        subject : string
            Unique identifier of experiment subject
        images: string
            Unique identifier of used image group
        fmri_data : string, optional
            Unique identifier of functional MRI data for experiment subject
        timestamp : datetime, optional
            Time stamp of object creation (UTC).
        is_active : Boolean, optional
            Flag indicating whether the object is active or has been deleted.
        """
        # Initialize super class
        super(ExperimentHandle, self).__init__(
            identifier,
            datastore.OBJ_EXPERIMENT,
            timestamp,
            properties,
            is_active=is_active
        )
        # Initialize class specific Attributes
        self.subject = subject
        self.images = images
        self.fmri_data = fmri_data


# ------------------------------------------------------------------------------
#
# Object Store
#
# ------------------------------------------------------------------------------
class DefaultExperimentManager(datastore.MongoDBStore):
    """Default Experiement Data Manager - Manager for experiment objects.

    This is a default implentation that uses MongoDB as storage backend.
    """
    def __init__(self, mongo_collection):
        """Initialize the MongoDB collection and base directory where to store
        functional data MRI files.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB storing functional data information
        """
        # Initialize the super class
        super(DefaultExperimentManager, self).__init__(mongo_collection)

    def create_object(self, name, subject, images, fmri_data=None):
        """Create an experiment object for the subject and image group. Objects
        are referenced by their identifier. The reference to a functional data
        object is optional.

        Parameters
        ----------
        name : string
            User-provided name for the experiment
        subject : string
            Unique identifier of subject
        images : string
            Unique identifier of image group
        fmri_data : string, optional
            Unique identifier of functional MRI data object

        Returns
        -------
        ExperimentHandle
            Handle for created experiment object in database
        """
        # Create a new object identifier.
        identifier = str(uuid.uuid4())
        # Create the initial set of properties for the new experiement object.
        properties = {datastore.PROPERTY_NAME: name}
        # Create object handle and store it in database before returning it
        obj = ExperimentHandle(identifier, properties, subject, images, fmri_data=fmri_data)
        self.insert_object(obj)
        return obj

    def from_json(self, document):
        """Create experiment object from JSON document retrieved from database.

        Parameters
        ----------
        document : JSON
            Json document in database

        Returns
        -------
        ExperimentHandle
            Handle for experiment object
        """
        identifier = str(document['_id'])
        active = document['active']
        timestamp = datetime.datetime.strptime(document['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
        properties = document['properties']
        subject = document['subject']
        images = document['images']
        fmri_data = document['fmri'] if 'fmri' in document else None
        return ExperimentHandle(
            identifier,
            properties,
            subject,
            images,
            fmri_data=fmri_data,
            timestamp=timestamp,
            is_active=active
        )

    def to_json(self, experiment):
        """Create a Json-like object for an experiment. Extends the basic
        object with subject, image group, and (optional) functional data
        identifiers.

        Parameters
        ----------
        experiment : ExperimentHandle

        Returns
        -------
        Json Object
            Json-like object, i.e., dictionary.
        """
        # Get the basic Json object from the super class
        json_obj = super(DefaultExperimentManager, self).to_json(experiment)
        # Add associated object references
        json_obj['subject'] = experiment.subject
        json_obj['images'] = experiment.images
        if not experiment.fmri_data is None:
            json_obj['fmri'] = experiment.fmri_data
        return json_obj
