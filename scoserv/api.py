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
from scodata.attribute import AttributeDefinition
from scodata.mongo import MongoDBFactory
from scoengine import EngineException
from scoengine.model import ModelOutputs
from scoengine import SCOEngine

from content import ContentPage
import hateoas
from widget import WidgetRegistry, WidgetInput


"""Home page content identifier."""
PAGE_HOME = 'home'


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
        self.refs = hateoas.HATEOASReferenceFactory(base_url, config['app.doc'])
        # Instantiate the SCO workflow engine.
        self.engine = SCOEngine(mongo)
        # Instantiate the widget registry
        self.widgets = WidgetRegistry(mongo)
        # Initialize the set of content pages. Add default home page at the end.
        self.pages = {}
        page_descriptors = []
        for doc in config['doc.pages']:
            page = ContentPage(doc)
            self.pages[page.id] = page
            page_descriptors.append(page)
        home_page = ContentPage({
            'id': PAGE_HOME,
            'label': PAGE_HOME,
            'title': config['home.title'],
            'sortOrder': -1,
            'resource': config['home.content']
        })
        self.pages[PAGE_HOME] = home_page
        page_descriptors.append(home_page)
        # Initialize the server description object. Name and title are elements
        # in the config object. Homepage content is read from file. The file
        # name could either be a Url or a reference to a file on local disk.
        self.description = {
            'name': config['app.name'],
            'title': config['app.title'],
            'resources': {
                'imageGroupOptions': [
                    opt.to_dict() for opt in self.db.image_groups_options()
                ],
                'pages': [
                    page_descriptor_to_dict(page, self.refs)
                        for page in sorted(
                            page_descriptors,
                            key = lambda p: (p.sort_order, p.label)
                        )
                ]
            },
            'links': self.refs.service_references()
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
        # Get the model run to (1) ensure that it exists and (2) retrieve the
        # model definition for a list of defined attachments.
        model_run = self.db.experiments_predictions_get(experiment_id, run_id)
        if model_run is None:
            return None
        # Get the model definition for a list of attachments. If the resource
        # identifier matches a defined attachment use the defined MimeType for
        # the attachment. Otherwise, MimeType will be inferred from file suffix.
        model = self.engine.get_model(model_run.model_id)
        if model is None:
            return None
        mime_type = None
        for attach in model.outputs.attachments:
            if attach.filename == resource_id:
                mime_type = attach.mime_type
                break
        result = self.db.experiments_predictions_attachments_create(
            experiment_id,
            run_id,
            resource_id,
            filename,
            mime_type=mime_type
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
        model = self.engine.get_model(model_id)
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
            self.engine.run_model(
                model_run,
                self.refs.experiments_prediction_reference(
                    experiment_id,
                    model_run.identifier
                )
            )
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
                'filesize' : model_run.attachments[attachment].filesize,
                'links' : self.refs.experiments_prediction_attachment_references(
                    experiment_id,
                    prediction_id,
                    model_run.attachments[attachment]
                )
            } for attachment in sorted(model_run.attachments)
        ]
        # Add widgets
        obj['widgets'] = [];
        # Get all widgets that have been defined for the model that was run.
        # Widgets are keyed by attachment.
        model_widgets = self.widgets.find_widgets_for_model(model_run.model_id)
        for key in model_run.attachments:
            if key in model_widgets:
                for widget in model_widgets[key]:
                    if widget.engine_id == 'VEGALITE':
                        attachment = model_run.attachments[key]
                        url = self.refs.experiments_prediction_attachment_reference(
                            experiment_id,
                            prediction_id,
                            key
                        )
                        mime_type = attachment.mime_type
                        if mime_type == 'text/csv':
                            format_type = 'csv'
                        elif mime_type == 'text/tab-separated-values':
                            format_type = 'tsv'
                        else:
                            format_type = 'json'
                        code = {key : widget.code[key] for key in widget.code}
                        code['$schema'] = 'https://vega.github.io/schema/vega-lite/v2.json'
                        code['data'] = {
                            'url' : url,
                            'formatType' : format_type
                        }
                        obj['widgets'].append({
                            'engine' : widget.engine_id,
                            'title' : widget.title,
                            'code' : code
                        })
        # Return complete serialization of model run
        return obj

    def experiments_predictions_image_set_create(self, experiment_id, run_id, filename):
        """Upload a tar archive containing a prediction image set that was
        produced as the result of a successful model run.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier
        run_id : string
            Unique model run identifier
        filename : string
            Path to uploaded image set archive file

        Returns
        -------
        dict
            Dictionary serialization of a prediction image set descriptor
        """
        # Create prediction image set from given file. Returns None if the
        # specified model run does not exist or if its state is not SUCCESS.
        image_set = self.db.experiments_predictions_image_set_create(
            experiment_id,
            run_id,
            filename
        )
        if image_set is None:
            return None
        return object_to_dict(image_set, self.refs)

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
                'links' : self.refs.image_group_image_references(
                    obj.identifier,
                    os.path.basename(obj.filename)
                )
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
                opt.to_dict() for opt in self.db.image_groups_options()
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

    def models_delete(self, model_id):
        """Delete model with given identifier from registry.

        Parameters
        ----------
        model_id : string
            Unique model identifier

        Returns
        -------
        ModelHandle
            Handle for deleted model or None if unknown
        """
        return self.engine.delete_model(model_id)

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
        model = self.engine.get_model(model_id)
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
            self.engine.list_models(limit=limit, offset=offset),
            self.refs.models_reference(),
            self.refs,
            properties=properties
        )

    def models_register(self, model_id, properties, parameters, outputs, connector):
        """Register a new model with the engine. Expects connection information
        for RabbitMQ to submit model run requests to workers.

        Raises ValueError if the given model identifier is not unique.

        Parameters
        ----------
        model_id : string
            Unique model identifier
        properties : dict
            Dictionary of model properties
        parameters :  dict
            List of attribute definitions for model run parameters
        outputs : dict
            Description of model outputs
        connector : dict
            Connection information to communicate with model workers.

        Returns
        -------
        dict
            Dictionary representing the model
        """
        # Create attribute definitions. Make sure to catch KeyErrors.
        attributeDefs = []
        try:
            for doc in parameters:
                attributeDefs.append(AttributeDefinition.from_dict(doc))
        except KeyError as ex:
            raise ValueError(str(ex))
        return self.model_to_dict(
            self.engine.register_model(
                model_id,
                properties,
                attributeDefs,
                ModelOutputs.from_dict(outputs),
                connector
            )
        )

    def models_update_connector(self, model_id, connector):
        """Update the connector information for a given model.

        Raises ValueError if connector information is invalid.

        Parameters
        ----------
        model_id : string
            Unique model identifier
        connector : dict
            Dictionary of connector specific properties.

        Returns
        -------
        ModelHandle
            Handle for updated model or None if model doesn't exist
        """
        return self.model_to_dict(
            self.engine.update_model_connector(model_id, connector)
        )

    def models_upsert_property(self, model_id, properties):
        """Upsert properties of given model.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        model_id : string
            Unique model identifier
        properties : dict
            Dictionary of property names and their new values.

        Returns
        -------
        ModelHandle
            Handle for updated model or None if model doesn't exist
        """
        return self.engine.upsert_model_properties(model_id, properties)

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
        # Return None if model is None
        if model is None:
            return None
        obj = object_to_dict(model, self.refs)
        description = model.description
        if not description is None:
            obj['description'] = description
        obj['parameters'] = [
            attr.to_dict() for attr in model.parameters
        ]
        obj['outputs'] = {
            'prediction' : {
                    'filename' : model.outputs.prediction_file.filename,
                    'mimeType' : model.outputs.prediction_file.mime_type,
                    'path' : model.outputs.prediction_file.path
                },
            'attachments' : [
                {
                    'filename' : a.filename,
                    'mimeType' : a.mime_type,
                    'path' : a.path
                } for a in model.outputs.attachments
            ]
        }
        obj['connector'] = {
            key : model.connector[key]
                for key in model.connector if key != 'password'
        }
        return obj

    # --------------------------------------------------------------------------
    # Pages
    # --------------------------------------------------------------------------

    def pages_get(self, page_id):
        """Get body and title for the content page with the given identifier.

        Returns None if the page with the given identifier does not exist.

        Parameters
        ----------
        page_id : string
            Unique content page identifier

        Returns
        -------
        dict
            Dictionary serialization of the content page
        """
        if not page_id in self.pages:
            return None
        return page_to_dict(self.pages[page_id], self.refs)

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
        return {key: self.description[key] for key in self.description}

    # --------------------------------------------------------------------------
    # Widgets
    # --------------------------------------------------------------------------

    def widgets_add_input_descriptor(self, widget_id, obj):
        """Append input descriptor to a given visualization widget.

        Parameters
        ----------
        widget_id : string
            Unique widget identifier
        obj : dict
            Serialization of a widget input descriptor

        Returns
        -------
        widget.WidgetHandle
        """
        self.widgets.append_input_for_widget(
            widget_id,
            WidgetInput.from_dict(obj)
        )
        return self.widgets_get(widget_id)

    def widgets_create(self, engine, code, inputs, properties):
        """Creat a new widget in the database.

        Parameters
        ----------
        engine : string
            Visualization engine identifier
        code : dict
            Engine-specific code
        inputs : list({'model':'...', 'attachment':'...'})
            Input descriptors for the new widget
        properties : dict
            Set of experiment properties. Is required to contain at least the
            widget name

        Returns
        -------
        dict
        """
        return response_success(
            self.widgets.create_widget(
                properties,
                engine,
                code,
                [WidgetInput.from_dict(doc) for doc in inputs]
            ),
            self.refs
        )

    def widgets_delete(self, widget_id):
        """Delete visualization widget with given identifier from the database.

        Parameters
        ----------
        widget_id : string
            Unique widget identifier

        Returns
        -------
        widget.WidgetHandle
            Handle for deleted widget or None if identifier is unknown
        """
        return self.widgets.delete_widget(widget_id)

    def widgets_get(self, widget_id):
        """Retrieve a widget from the database.

        Parameters
        ----------
        widget_id : string
            Unique widget identifier

        Returns
        -------
        dict
            Dictionary representing the widget or None if no widget with the
            given identifier exists
        """
        # Get subject from database. Return None if not subject with given
        # identifier exist.
        widget = self.widgets.get_widget(widget_id)
        if widget is None:
            return None
        return self.widget_to_dict(widget)

    def widgets_list(self, limit=-1, offset=0, properties=None):
        """Get a listing of all widgets in the database.

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
            Dictionary representing a listing of widgets
        """
        return listing_to_dict(
            self.widgets.list_widgets(limit=limit, offset=offset),
            self.refs.widgets_reference(),
            self.refs,
            properties=properties
        )

    def widget_to_dict(self, widget):
        """Dictionary serialization for visualization widget.

        Parameters
        ----------
        widget : widget.WidgetHandle
            Widget handle

        Returns
        -------
        dict
        """
        obj = object_to_dict(widget, self.refs)
        obj['engine'] = widget.engine_id
        obj['code'] = widget.code
        obj['inputs'] = [inp.to_dict() for inp in widget.inputs]
        return obj

    def widgets_update(self, widget_id, code=None, inputs=None):
        """Update code and/or input descriptors for a widget in the database.

        Will return None if no widget with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique widget identifier
        code : dict, optional
            New engine-specific code. If None the existing code will not be
            changed
        inputs : list({'model':'...', 'attachment':'...'}), optional
            New list of widget inputs. If None the existing list will not be
            changed.

        Returns
        -------
        widget.WidgetHandle
        """
        if not inputs is None:
            descriptors = [WidgetInput.from_dict(doc) for doc in inputs]
        else:
            descriptors = None
        self.widgets.update_widget(widget_id, code=code, inputs=descriptors)
        return self.widgets_get(widget_id)

    def widgets_upsert_property(self, widget_id, properties):
        """Upsert property of given widget.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        widget_id : string
            Unique widget identifier
        properties : dict()
            Dictionary of property names and their new values.

        Returns
        -------
        widget.WidgetHandle
            Handle for updated object or None if object doesn't exist
        """
        return self.widgets.upsert_object_property(widget_id, properties)


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


def page_to_dict(page, refs):
    """Dictionary representation for a content page .

    Parameters
    ----------
    page : ContentPage
        Content Page handle
    refs : hateoas.HATEOASReferenceFactory
        Url factory for API resources

    Returns
    -------
    dict
        Dictionary representing a content page
    """
    obj = page_descriptor_to_dict(page, refs)
    obj['title'] = page.title
    obj['body'] = page.body
    return obj


def page_descriptor_to_dict(page, refs):
    """Dictionary representation for a content page descriptor.

    Parameters
    ----------
    page : ContentPage
        Content Page handle
    refs : hateoas.HATEOASReferenceFactory
        Url factory for API resources

    Returns
    -------
    dict
        Dictionary representing a page descriptor
    """
    return {
        'id' : page.id,
        'label' : page.label,
        'links' : refs.page_references(page.id)
    }


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
