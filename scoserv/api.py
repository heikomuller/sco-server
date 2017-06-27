"""SCO Web Server API -

The server API is a wrapper around the different components that make up the
SCO server. That is, the API encloses the SCO data store, model repository, and
workflow engine.

The SCO API class contains all the method calls that are available through the
Web API.
"""

import json
import os
import urllib2
import yaml

from scodata import SCODataStore
from scodata.mongo import MongoDBFactory
from scoengine import EngineException
from scoengine import RabbitMQClient
from scomodels import DefaultModelRegistry

import hateoas


class SCOServerAPI(object):
    """The server API implements all API calls that are accessible via the SCO
    Web API. The API contains the SCO data store, model repository, and workflow
    engine.

    All methods return dictionaries that can be serialized as Json.
    """
    def __init__(self, config, base_url):
        """Initialize the SCO data store, model repository, and workflow engine.
        Also creates a service description object.

        Parameters
        ----------
        config : dict
            Dictionary of configuration parameters
        base_url : string
            Base Url for API resource Urls
        """
        # Create MongoDB database connector
        mongo = MongoDBFactory(db_name=config['mongo.db'])
        # Instantiate the Standard Cortical Observer Data Store.
        self.db = SCODataStore(mongo, os.path.abspath(config['server.datadir']))
        # Initalize the Url factory
        self.refs = hateoas.HATEOASReferenceFactory(base_url)
        # Instantiate the RabbitMQ SCO workflow engine.
        self.engine = RabbitMQClient(
            host=config['rabbitmq.host'],
            port=config['rabbitmq.port'],
            virtual_host=config['rabbitmq.vhost'],
            queue=config['rabbitmq.queue'],
            user=config['rabbitmq.user'],
            password=config['rabbitmq.password'],
            reference_factory=self.refs
        )
        # Instantiate the model registry
        self.models = DefaultModelRegistry(mongo)
        # Widgets are read from a resource in either Json or Yaml format
        # (identified by the resource suffix; default is Json)
        if config['app.widgets'].endswith('.yaml'):
            widgets = yaml.load(read_resource(config['app.widgets']))['widgets']
        else:
            widgets = json.load(read_resource(config['app.widgets']))['widgets']
        # Initialize the server description object. Name and title are elements
        # in the config object. Homepage content is read from file. The file
        # name could either be a Url or a reference to a file on local disk.
        self.description = {
            'name' : config['app.name'],
            'title' : config['app.title'],
            'overview' : {
                'title': config['home.title'],
                'content': read_resource(config['home.content'])
            },
            'resources' : {
                'imageGroupOptions' : [
                    opt.to_json() for opt in self.db.image_groups_options()
                ],
                'widgets' : widgets
            },
            'links' : self.refs.service_references()
        }

    # --------------------------------------------------------------------------
    # Experiments
    # --------------------------------------------------------------------------

    def experiments_create(self, subject_id, image_group_id, properties):
        """Create an experiment object with subject, and image group. Objects
        are referenced by their unique identifiers.

        Raises ValueError if invalid arguments are given.

        Parameters
        ----------
        subject_id : string
            Unique identifier of subject
        image_group_id : string
            Unique identifier of image group
        properties : dict
            Set of experiment properties. Is required to contain at least the
            experiment name

        Returns
        -------
        dict
            Dictionary representing a successful response
        """
        return response_success(
            self.db.experiments_create(subject_id, image_group_id, properties),
            self.refs
        )

    def experiments_delete(self, experiment_id):
        """Delete experiment with given identifier in the database.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier

        Returns
        -------
        ExperimentHandle
            Handle for deleted experiment or None if identifier is unknown
        """
        return self.db.experiments_delete(experiment_id)

    def experiments_get(self, experiment_id):
        """Retrieve an experiment object from the data store.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier

        Returns
        -------
        dict
            Dictionary representing the experiment object or None if no
            experiment with the given identifier exists
        """
        # Get experiment object from database. Return None if not experiment
        # with given identifier exists
        experiment = self.db.experiments_get(experiment_id)
        if experiment is None:
            return None
        # Retrieve associated subject, image_group, and fMRI data (if present)
        # TODO: Handle cases where either of the objects has been deleted
        # By now we return None, i.e., the experiment does not exist if
        # the subject or image group has been deleted (CASCADE DELETE).
        obj = object_to_dict(experiment, self.refs)
        subject = self.subjects_get(experiment.subject_id)
        if subject is None:
            return None
        else:
            obj['subject'] = subject
        image_group = self.image_groups_get(experiment.image_group_id)
        if image_group is None:
            return None
        else:
            obj['images'] = image_group
        if not experiment.fmri_data_id is None:
            obj['fmri'] = self.experiments_fmri_get(experiment.identifier)
        # Return Json serialization of object.
        return obj

    def experiments_list(self, limit=-1, offset=0, properties=None):
        """Get a listing of all experiment objects in the data store.

        Parameters
        ----------
        limit : int, optional
            Limit the number of items in the returned listing
        offset : int, optional
            Start listing at the given index position (in order of items as
            defined by the data store)
        properties : list(string), optional
            List of additional properties to be included in the listing for
            each item

        Returns
        -------
        dict
            Dictionary representing a listing of experiment objects
        """
        return listing_to_dict(
            self.db.experiments_list(limit=limit, offset=offset),
            self.refs.experiments_reference(),
            self.refs,
            properties=properties
        )

    def experiments_upsert_property(self, experiment_id, properties):
        """Upsert property of given experiment.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ExperimentHandle
            Handle for updated object or None if object doesn't exist
        """
        return self.db.experiments_upsert_property(experiment_id, properties)

    # --------------------------------------------------------------------------
    # Functional Data
    # --------------------------------------------------------------------------

    def experiments_fmri_create(self, experiment_id, filename):
        """Create functional data object from given file and associate it with
        the given experiment.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        filename : File-type object
            Functional data file

        Returns
        -------
        dict
            Dictionary representing a successful response
        """
        return response_success(
            self.db.experiments_fmri_create(experiment_id, filename),
            self.refs
        )

    def experiments_fmri_delete(self, experiment_id):
        """Delete fMRI data object associated with given experiment.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier

        Returns
        -------
        FMRIDataHandle
            Handle for deleted data object or None if experiment is unknown or
            has no fMRI data object associated with it
        """
        # Get experiment fMRI to ensure that it exists
        return self.db.experiments_fmri_delete(experiment_id)

    def experiments_fmri_download(self, experiment_id):
        """Download functional MRI data file.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier

        Returns
        -------
        FileInfo
            Information about file on disk or None if requested resource does
            not exist
        """
        # Get download information for experiments fMRI data object and send the
        # data file. Raises 404 exception if no data file is associated with the
        # requested resource (or the experiment does not exist).
        return self.db.experiments_fmri_download(experiment_id)

    def experiments_fmri_get(self, experiment_id):
        """Get functional MRI data data object that is associated with a given
        experiment.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier

        Returns
        -------
        dict
            Dictionary representing fMRI resources or None if no fMRI data is
            associated with the given experiment.
        """
        # Get experiments fMRI object from database. Return None if no fMRI data
        # is associated with the given experiment
        fmri = self.db.experiments_fmri_get(experiment_id)
        if fmri is None:
            return None
        return object_to_dict(fmri, self.refs)

    def experiments_fmri_upsert_property(self, experiment_id, properties):
        """Upsert property of fMRI data object associated with given experiment.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        FMRIDataHandle
            Handle for updated object or None if object doesn't exist
        """
        # Update properties for fMRI object using the object identifier
        return self.db.experiments_fmri_upsert_property(
            experiment_id,
            properties
        )

    # --------------------------------------------------------------------------
    # Prediction Data
    # --------------------------------------------------------------------------

    def experiments_predictions_attachments_create(self, experiment_id, run_id, resource_id, filename):
        """Attach a given data file with a model run. The attached file is
        identified by the resource identifier. If a resource with the given
        identifier already exists it will be overwritten.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        model_id : string
            Unique identifier of model to run
        resource_id : string
            Unique attachment identifier
        filename : string
            Path to data file that is being attached. A copy of the file will
            be created

        Returns
        -------
        dict
            Dictionary representing a successful response. The result is None if
            the specified experiment or model run do not exist.
        """
        result = self.db.experiments_predictions_attachments_create(
            experiment_id, run_id, resource_id, filename
        )
        # Make sure that the result is not None. Otherwise, return None to
        # indicate an unknown experiment or model run
        if not result is None:
            return response_success(result, self.refs)
        else:
            return None

    def experiments_predictions_attachments_delete(self, experiment_id, run_id, resource_id):
        """Delete attached file with given resource identifier from a mode run.

        Raise ValueError if an image archive with the given resource identifier
        is attached to the model run instead of a data file.


        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        model_id : string
            Unique identifier of model to run
        resource_id : string
            Unique attachment identifier

        Returns
        -------
        boolean
            True, if file was deleted. False, if no attachment with given
            resource identifier existed.
        """
        return self.db.experiments_predictions_attachments_delete(
            experiment_id,
            run_id,
            resource_id
        )

    def experiments_predictions_attachments_download(self, experiment_id, run_id, resource_id):
        """Download a data file that has been attached with a successful model
        run.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        model_id : string
            Unique identifier of model to run
        resource_id : string
            Unique attachment identifier

        Returns
        -------
        FileInfo
            Information about attachmed file on disk or None if attachment with
            given resource identifier exists
        """
        return self.db.experiments_predictions_attachments_download(
            experiment_id,
            run_id,
            resource_id
        )

    def experiments_predictions_create(self, experiment_id, model_id, name, arguments=None, properties=None):
        """Create new model run for given experiment.

        Raises a ValueError is the specified model does not exist.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        model_id : string
            Unique identifier of model to run
        name : string
            User-provided name for the model run
        arguments : list(attribute.Attribute)
            List of arguments for model run
        properties : Dictionary, optional
            Set of model run properties.

        Returns
        -------
        dict
            Dictionary representing a successful response. The result is None if
            the specified experiment does not exist.
        """
        # Make sure that the referenced model exists.
        model = self.models.get_model(model_id)
        if model is None:
            raise ValueError('unknown model: ' + model_id)
        # Call create method of API to get a new model run object handle.
        model_run = self.db.experiments_predictions_create(
            experiment_id,
            model_id,
            model.parameters,
            name,
            arguments=arguments,
            properties=properties
        )
        # The result is None if experiment does not exists
        if model_run is None:
            return None
        # Start the model run
        try:
            self.engine.run_model(model_run)
        except EngineException as ex:
            # Delete model run from database if running the model failed.
            self.db.experiments_predictions_delete(
                model_run.experiment_id,
                model_run.identifier,
                erase=True
            )
            raise ValueError(ex.message)
        # Return success including list of references for new model run.
        return response_success(model_run, self.refs)

    def experiments_predictions_delete(self, experiment_id, run_id):
        """Delete given prediction for experiment.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        run_id : string
            Unique model run identifier

        Returns
        -------
        ModelRunHandle
            Handle for deleted model run or None if unknown
        """
        return self.db.experiments_predictions_delete(experiment_id, run_id)

    def experiments_predictions_download(self, experiment_id, prediction_id):
        """Download model run result data file.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        prediction_id : string
            Unique model run identifier

        Returns
        -------
        FileInfo
            Information about file on disk or None if requested resource does
            not exist
        """
        return self.db.experiments_predictions_download(
            experiment_id,
            prediction_id
        )

    def experiments_predictions_get(self, experiment_id, prediction_id):
        """Get model run object that is associated with a given experiment.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        prediction_id : string
            Unique model run identifier

        Returns
        -------
        dict
            Dictionary representing model run or None if no model run with given
            identifier exists or is associated with given experiment.
        """
        # Get model run object from database. Return None if model run does
        # not exist.
        model_run = self.db.experiments_predictions_get(
            experiment_id,
            prediction_id
        )
        if model_run is None:
            return None
        obj = object_to_dict(model_run, self.refs)
        # Add model identifier. If the model is None it has been deleted. In
        # this case the overall result will be None
        model =  self.models_get(model_run.model_id)
        if model is None:
            return None
        obj['model'] = model
        # Add experiment information
        obj['experiment'] = self.experiments_get(experiment_id)
        # Add state information
        obj['state'] =  str(model_run.state)
        if model_run.state.is_failed:
            obj['errors'] = model_run.state.errors
        # Add life cycle Timestamps
        obj['schedule'] = model_run.schedule
        # Add model run arguments
        obj['arguments'] = [
            {
                'name' : attr,
                'value' : model_run.arguments[attr].value
            } for attr in model_run.arguments
        ]
        # Add model run attachments
        obj['attachments'] = [
            {
                'id' : attachment,
                'mimeType' : model_run.attachments[attachment].mime_type,
                'links' : self.refs.experiments_prediction_attachment_references(
                    experiment_id,
                    prediction_id,
                    model_run.attachments[attachment]
                )
            } for attachment in model_run.attachments
        ]
        # Return complete serialization of model run
        return obj

    def experiments_predictions_list(self, experiment_id, limit=-1, offset=0, properties=None):
        """Get a listing of all model runs for a given experiment in the data
        store.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        limit : int, optional
            Limit the number of items in the returned listing
        offset : int, optional
            Start listing at the given index position (in order of items as
            defined by the data store)
        properties : list(string), optional
            List of additional properties to be included in the listing for
            each item

        Returns
        -------
        dict
            Dictionary representing a listing of model runs
        """
        return listing_to_dict(
            self.db.experiments_predictions_list(
                experiment_id,
                limit=limit,
                offset=offset
            ),
            self.refs.experiments_predictions_reference(experiment_id),
            self.refs,
            properties=properties
        )

    def experiments_predictions_update_state_active(self, experiment_id, run_id):
        """Update state of given prediction to active.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        run_id : string
            Unique model run identifier

        Returns
        -------
        ModelRunHandle
            Handle for updated model run or None is prediction is undefined
        """
        return self.db.experiments_predictions_update_state_active(
            experiment_id,
            run_id
        )

    def experiments_predictions_update_state_error(self, experiment_id, run_id, errors):
        """Update state of given prediction to failed. Set error messages that
        where generated by the failed run execution.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        run_id : string
            Unique model run identifier
        errors : List(string)
            List of error messages

        Returns
        -------
        ModelRunHandle
            Handle for updated model run or None is prediction is undefined
        """
        return self.db.experiments_predictions_update_state_error(
            experiment_id,
            run_id,
            errors
        )

    def experiments_predictions_update_state_success(self, experiment_id, run_id, result_file):
        """Update state of given prediction to success. Create a function data
        resource for the given result file and associate it with the model run.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        run_id : string
            Unique model run identifier
        result_file : string
            Path to model run result file

        Returns
        -------
        ModelRunHandle
            Handle for updated model run or None is prediction is undefined
        """
        return self.db.experiments_predictions_update_state_success(
            experiment_id,
            run_id,
            result_file
        )

    def experiments_predictions_upsert_property(self, experiment_id, run_id, properties):
        """Upsert property of a prodiction for an experiment.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        run_id : string
            Unique model run identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ModelRunHandle
            Handle for updated object or None if object doesn't exist
        """
        return self.db.experiments_predictions_upsert_property(
            experiment_id,
            run_id,
            properties
        )

    # --------------------------------------------------------------------------
    # Image Files
    # --------------------------------------------------------------------------

    def image_files_delete(self, image_id):
        """Delete image object with given identifier.

        Parameters
        ----------
        image_id : string
            Unique image identifier

        Returns
        -------
        ImageHandle
            Handle for deleted image or None if identifier is unknown
        """
        return self.db.image_files_delete(image_id)

    def image_files_download(self, image_id):
        """Download image data file.

        Parameters
        ----------
        image_id : string
            Unique image identifier

        Returns
        -------
        FileInfo
            Information about file on disk or None if requested resource does
            not exist
        """
        return self.db.image_files_download(image_id)

    def image_files_get(self, image_id):
        """Retrieve an image file object from the data store.

        Parameters
        ----------
        image_id : string
            Unique image file identifier

        Returns
        -------
        dict
            Dictionary representing the image file object or None if no
            image file with the given identifier exists
        """
        # Get image file object from database. Return None if not image file
        # with given identifier exist.
        img_file = self.db.image_files_get(image_id)
        if img_file is None:
            return None
        return object_to_dict(img_file, self.refs)

    def image_files_list(self, limit=-1, offset=0, properties=None):
        """Get a listing of all image file objects in the data store.

        Parameters
        ----------
        limit : int, optional
            Limit the number of items in the returned listing
        offset : int, optional
            Start listing at the given index position (in order of items as
            defined by the data store)
        properties : list(string), optional
            List of additional properties to be included in the listing for
            each item

        Returns
        -------
        dict
            Dictionary representing a listing of image file objects
        """
        return listing_to_dict(
            self.db.image_files_list(limit=limit, offset=offset),
            self.refs.image_files_reference(),
            self.refs,
            properties=properties
        )

    def image_files_upsert_property(self, image_id, properties):
        """Upsert property of given image.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        image_id : string
            Unique image object identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ImageHandle
            Handle for updated object or None if object doesn't exist
        """
        return self.db.image_files_upsert_property(image_id, properties)

    # --------------------------------------------------------------------------
    # Image Groups
    # --------------------------------------------------------------------------

    def image_groups_delete(self, image_group_id):
        """Delete image group object with given identifier.

        Parameters
        ----------
        image_group_id : string
            Unique image group identifier

        Returns
        -------
        ImageGroupHandle
            Handle for deleted image group or None if image_group_id is unknown
        """
        return self.db.image_groups_delete(image_group_id)

    def image_groups_download(self, image_group_id):
        """Download image group archive file.

        Parameters
        ----------
        image_group_id : string
            Unique image group identifier

        Returns
        -------
        FileInfo
            Information about file on disk or None if requested resource does
            not exist
        """
        return self.db.image_groups_download(image_group_id)

    def image_groups_get(self, image_group_id):
        """Retrieve an image group object from the data store.

        Parameters
        ----------
        image_group_id : string
            Unique image group identifier

        Returns
        -------
        dict
            Dictionary representing the image group object or None if no
            image group with the given identifier exists
        """
        # Get image group object from database. Return None if not image group
        # with given identifier exist.
        img_grp = self.db.image_groups_get(image_group_id)
        if img_grp is None:
            return None
        obj = object_to_dict(img_grp, self.refs)
        # Add list of contained images
        obj['images'] =  {
            'count' : len(img_grp.images),
            'links' : hateoas.self_reference_set(
                self.refs.image_group_images_list_reference(
                    img_grp.identifier
                )
            )
        }
        # Add image group options
        obj['options'] = [
            {
                'name' : attr,
                'value' : img_grp.options[attr].value
            } for attr in img_grp.options
        ]
        return obj

    def image_groups_images_list(self, image_group_id, limit=-1, offset=0):
        """Get a listing of all images in an image group objects.

        Parameters
        ----------
        limit : int, optional
            Limit the number of items in the returned listing
        offset : int, optional
            Start listing at the given index position (in order of items as
            defined by the data store)

        Returns
        -------
        dict
            Dictionary representing a listing of group images
        """
        # Get listing of group images. Return None if the given image group
        # identifier is unknown (i.e., listing is None)
        image_listing = self.db.image_group_images_list(
            image_group_id,
            limit=limit,
            offset=offset
        )
        if image_listing is None:
            return None
        # Generate list of object in listing
        items = []
        for obj in image_listing.items:
            # Create baseic object representation
            items.append({
                'id' : obj.identifier,
                'name' : obj.name,
                'folder' : obj.folder,
                'links' : self.refs.image_group_image_references(obj.identifier)
            })
        # Call generic item listing decorator
        return items_listing_to_dict(
            image_listing,
            items,
            None,
            self.refs.image_group_images_list_reference(image_group_id),
            links={
                hateoas.REF_KEY_IMAGE_GROUP :
                self.refs.image_group_reference(image_group_id)
            }
        )

    def image_groups_list(self, limit=-1, offset=0, properties=None):
        """Get a listing of all image group objects in the data store.

        Parameters
        ----------
        limit : int, optional
            Limit the number of items in the returned listing
        offset : int, optional
            Start listing at the given index position (in order of items as
            defined by the data store)
        properties : list(string), optional
            List of additional properties to be included in the listing for
            each item

        Returns
        -------
        dict
            Dictionary representing a listing of image group objects
        """
        return listing_to_dict(
            self.db.image_groups_list(limit=limit, offset=offset),
            self.refs.image_groups_reference(),
            self.refs,
            properties=properties
        )

    def image_groups_options(self):
        """Get a list of all supported image group options.

        Returns
        -------
        dict
            Dictionary representation of image group option definitions
        """
        return {
            'options' : [
                opt.to_json() for opt in self.db.image_groups_options()
            ]
        }

    def image_groups_update_options(self, image_group_id, options):
        """Update set of typed options associated with a given image group.

        Raises ValueError if invalid options are provided.

        Parameters
        ----------
        image_group_id : string
            Unique image group identifier
        options : list(dict('name':...,'value:...'))
            List of attribute values

        Returns
        -------
        ImageGroupHandle
            Handle for updated image group or None if identifier is unknown.
        """
        return self.db.image_groups_update_options(image_group_id, options)

    def image_groups_upsert_property(self, image_group_id, properties):
        """Upsert property of given image group.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        image_group_id : string
            Unique image group object identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ImageGroupHandle
            Handle for updated object or None if object doesn't exist
        """
        return self.db.image_groups_upsert_property(image_group_id, properties)

    # --------------------------------------------------------------------------
    # Upload Images
    # --------------------------------------------------------------------------

    def images_create(self, filename):
        """Create and image file or image group object from the given file. The
        type of the created database object is determined by the suffix of the
        given file. An ValueError exception is thrown if the file has an unknown
        suffix.

        Parameters
        ----------
        filename : File-type object
            File on local disk. Expected to be either an image file or an
            archive containing image.

        Returns
        -------
        dict
            Dictionary representing a successful response
        """
        return response_success(self.db.images_create(filename), self.refs)

    # --------------------------------------------------------------------------
    # Models
    # --------------------------------------------------------------------------

    def models_get(self, model_id):
        """Retrieve a model description from the model registry.

        Parameters
        ----------
        model_id : string
            Unique model identifier

        Returns
        -------
        dict
            Dictionary representing the model or None if no model with the
            given identifier exists
        """
        # Get subject from database. Return None if not subject with given
        # identifier exist.
        model = self.models.get_model(model_id)
        if model is None:
            return None
        # Model handle does not inherit from ObjectHandle. Thus, we cannot use
        # the default object_to_dict serialization.
        return self.model_to_dict(model)

    def models_list(self, limit=-1, offset=0, properties=None):
        """Get a listing of all models in the model registry.

        Parameters
        ----------
        limit : int, optional
            Limit the number of items in the returned listing
        offset : int, optional
            Start listing at the given index position (in order of items as
            defined by the data store)
        properties : list(string), optional
            List of additional properties to be included in the listing for
            each item

        Returns
        -------
        dict
            Dictionary representing a listing of models
        """
        return listing_to_dict(
            self.models.list_models(limit=limit, offset=offset),
            self.refs.models_reference(),
            self.refs,
            properties=properties
        )

    def models_upsert_property(self, model_id, properties):
        """Upsert properties of given model.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        model_id : string
            Unique model identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ModelHandle
            Handle for updated model or None if model doesn't exist
        """
        return self.models.upsert_object_property(model_id, properties)

    def model_to_dict(self, model):
        """Convert a model handle to a serializable dictionary.

        Parameters
        ----------
        model : ModelHandle
            Handle for model description in registry

        Returns
        -------
        dict
            Dictionary representing the model or None if no model with the
            given identifier exists
        """
        obj = object_to_dict(model, self.refs)
        obj['parameters'] = [
            attr.to_json() for attr in model.parameters
        ]
        obj['outputs'] = {
            'prediction' : model.outputs.prediction_filename,
            'attachments' : [
                {
                    'filename' : a.filename,
                    'mimeType' : a.mime_type
                } for a in model.outputs.attachments
            ]
        }
        return obj

    # --------------------------------------------------------------------------
    # Subjects
    # --------------------------------------------------------------------------

    def subjects_create(self, filename):
        """Create subject from given data files. Expects the file to be a
        Freesurfer archive.

        Raises ValueError if given file is not a valid subject file.

        Parameters
        ----------
        filename : File-type object
            Freesurfer archive file

        Returns
        -------
        dict
            Dictionary representing a successful response
        """
        return response_success(self.db.subjects_create(filename), self.refs)

    def subjects_delete(self, subject_id):
        """Delete subject with given identifier in the database.

        Parameters
        ----------
        subject_id : string
            Unique subject identifier

        Returns
        -------
        SubjectHandle
            Handle for deleted subject or None if identifier is unknown
        """
        return self.db.subjects_delete(subject_id)

    def subjects_download(self, subject_id):
        """Download subject archive file.

        Parameters
        ----------
        subject_id : string
            Unique subject identifier

        Returns
        -------
        FileInfo
            Information about file on disk or None if requested resource does
            not exist
        """
        return self.db.subjects_download(subject_id)

    def subjects_get(self, subject_id):
        """Retrieve a subject from the data store.

        Parameters
        ----------
        subject_id : string
            Unique subject identifier

        Returns
        -------
        dict
            Dictionary representing the subject or None if no subject with the
            given identifier exists
        """
        # Get subject from database. Return None if not subject with given
        # identifier exist.
        subject = self.db.subjects_get(subject_id)
        if subject is None:
            return None
        return object_to_dict(subject, self.refs)

    def subjects_list(self, limit=-1, offset=0, properties=None):
        """Get a listing of all subjects in the data store.

        Parameters
        ----------
        limit : int, optional
            Limit the number of items in the returned listing
        offset : int, optional
            Start listing at the given index position (in order of items as
            defined by the data store)
        properties : list(string), optional
            List of additional properties to be included in the listing for
            each item

        Returns
        -------
        dict
            Dictionary representing a listing of subjects
        """
        return listing_to_dict(
            self.db.subjects_list(limit=limit, offset=offset),
            self.refs.subjects_reference(),
            self.refs,
            properties=properties
        )

    def subjects_upsert_property(self, subject_id, properties):
        """Upsert property of given subject.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        subject_id : string
            Unique subject identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        SubjectHandle
            Handle for updated object or None if object doesn't exist
        """
        return self.db.subjects_upsert_property(subject_id, properties)

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------

    def service_description(self):
        """The service description object provides an overview of Web API and
        links to relevant resources and methods. The returned dictionary
        contains elements name, title, description (i.e., the homepage content),
        and a list of references to API resources.

        Returns
        -------
        dict
            Dictionary of API properties
        """
        # Model properties can be updates. Make a copy of the static service
        # description object and add model listing
        obj = {key: self.description[key] for key in self.description}
        obj['resources']['models'] = [
            self.model_to_dict(model)
                for model in self.models.list_objects().items
        ]
        return obj


# ------------------------------------------------------------------------------
#
# Helper Methods for Resource Serialization
#
# ------------------------------------------------------------------------------

def items_listing_to_dict(objects, items, properties, listing_url, links=None):
    """Generic serializer for a list of items. Used for object listings and
    group image listings.

    Parameters
    ----------
    objects : db.datastore.ObjectListing
        Listing of objects resulting from list_objects() call
    items : List(Json-like object)
        Json serialization of list items
    properties : List(string)
        List of property names or None.
    listing_url : string
        base Url for given object listing
    links : dict, optional
        Additional references to be included in the reference set for the
        object listing

    Returns
    -------
    Json-like object
        Object listing resource in Json format
    """
    # Generate listing navigation references
    nav = hateoas.PaginationReferenceFactory(
        objects,
        properties,
        listing_url
    ).navigation_references(links=links)
    # Return Json-like object contaiing items, references, and listing
    # arguments and statistics
    return {
        'items' : items,
        'offset' : objects.offset,
        'limit' : objects.limit,
        'count' : len(items),
        'totalCount' : objects.total_count,
        'links' : nav
    }


def listing_to_dict(objects, listing_url, refs, properties=None, links=None):
    """Create a dictionary representation for an object listing.

    The set of properties defines additional properties to include with every
    item in the listing. If the property set is None no additional properties
    will be included for objects in the listing.

    Parameters
    ----------
    objects : db.datastore.ObjectListing
        Listing of objects resulting from list_objects() call
    listing_url : string
        base Url for given object listing
    refs : hateoas.HATEOASReferenceFactory
        Url factory for API resources
    properties : List(string), optional
        List of properties to be included for each object or None.
    links : dict, optional
        Additional references to be included in the reference set for the
        object listing

    Returns
    -------
    dict
        Object listing resource
    """
    # Generate list of object in listing
    items = []
    for obj in objects.items:
        # Create baseic object representation
        item = {
            'id' : obj.identifier,
            'name' : obj.name,
            'timestamp' : str(obj.timestamp.isoformat()),
            'links' : refs.object_references(obj)
        }
        # Add elements in property set to object representation if present
        # in object properties
        if not properties is None:
            for prop in properties:
                if prop in obj.properties:
                    item[prop] = obj.properties[prop]
        # Add item to list
        items.append(item)
    # Call generic item listing decorator
    return items_listing_to_dict(
        objects,
        items,
        properties,
        listing_url,
        links=links
    )


def object_to_dict(obj, refs):
    """Basic dictionary representation for a given object. Contains the object
    identifier, name, timestamp, list of properties, and list of references.

    Parameters
    ----------
    obj : (sub-class of)ObjectHandle
        Object handle
    refs : hateoas.HATEOASReferenceFactory
        Url factory for API resources

    Returns
    -------
    dict
        Representation of object handle in Json format
    """
    # Generate basic object serialization
    properties = obj.properties
    return {
        'id' : obj.identifier,
        'name' : obj.name,
        'timestamp' : str(obj.timestamp.isoformat()),
        'properties' : [
            {'key' : key, 'value' : properties[key]} for key in properties
        ],
        'links' : refs.object_references(obj)
    }


def read_resource(resource):
    """Read content of a given resource. The resource could either be identified
    by a Url or it is a file on the local disk.

    Parameters
    ----------
    resource : string
        Resource identifier. Either a Url or the path to a file on disk

    Returns
    -------
    string
        Contents of the resource
    """
    # Check if resource is a Url
    for url_prefix in ['http://', 'https://', 'file://']:
        if resource.startswith(url_prefix):
            return urllib2.urlopen(resource).read()
    # Read local file if resource is not a Url
    with open(resource, 'r') as f:
        return f.read()


def response_success(obj, refs):
    """Generate response for successful object manipulation. If the given object
    is None, the result will be None.

    Parameters
    ----------
    obj : (sub-class of)ObjectHandle
        Object handle
    refs : hateoas.HATEOASReferenceFactory
        Url factory for API resources

    Returns
    -------
    dict
        Representation of successful object manipulation
    """
    if obj is None:
        return None
    return {
        'result' : 'SUCCESS',
        'links': refs.object_references(obj)
    }
