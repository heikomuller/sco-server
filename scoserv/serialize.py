"""Serializer for resources that are accessible via the Web API."""

import hateoas
import utils


# ------------------------------------------------------------------------------
#
# JSON Serializer
#
# ------------------------------------------------------------------------------

class JsonWebAPISerializer(object):
    """Serializer for Web API resources that are accessed as Json objects. At
    this point Json is the only supported format by the Web API.
    """
    def __init__(self, base_url):
        """Initialize base Url for references to resources to satisfy the
        Hypermedia as the Engine of Application State (HATEOAS) constraint of
        the application architecture.

        Parameters
        ----------
        base_url : string
            Base Url for all resource references
        """
        # Initialize reference factory
        self.refs = hateoas.HATEOASReferenceFactory(base_url)

    def experiment_to_json(self, experiment, subject, image_group, fmri=None):
        """Json serialization for experiment.

        Parameters
        ----------
        experiment : db.subject.ExperimentHandle

        Returns
        -------
        Json-like object
            Representation of object handle in Json format
        """
        # Get basic object serialization
        json_obj = self.object_to_json(experiment)
        # Add information about associated subject
        json_obj['subject'] = self.subject_to_json(subject)
        # Add information about associated image group
        json_obj['images'] = self.image_group_to_json(image_group)
        # Add information about associated fMRI data (if present)
        if not fmri is None:
            json_obj['fmri'] = self.experiment_fmri_to_json(fmri)
        # Return Json serialization
        return json_obj

    def experiment_fmri_to_json(self, fmri):
        """Json serialization for fMRI object.

        Parameters
        ----------
        subject : db.subject.FMRIDataHandle
            fMRI object handle

        Returns
        -------
        Json-like object
            Representation of object handle in Json format
        """
        # Get basic object serialization
        json_obj = self.object_to_json(fmri)
        # Add experiment information
        json_obj['experiment'] = {
            'id' : fmri.experiment,
            'links': hateoas.self_reference_set(
                self.refs.experiment_reference(fmri.experiment)
            )
        }
        # Return complete serialization of fMRI object
        return json_obj


    def experiments_to_json(self, object_listing, properties):
        """Json serialization for experiments listing.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of experiments
        properties : List(string)
            List of additional properties to include with every object in the
            listing or None.

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
        return self.listing_to_json(
            object_listing,
            properties,
            self.refs.experiments_reference()
        )

    def experiment_prediction_to_json(self, model_run, experiment):
        """Json serialization for model run object.

        Parameters
        ----------
        model_run : db.prediction.ModelRunHandle
            Model run handle
        experiment : db.experiment.ExperimentHandle
            Handle for experiment the run is a prediction for

        Returns
        -------
        Json-like object
            Representation of object handle in Json format
        """
        # Get basic object serialization
        json_obj = self.object_to_json(model_run)
        # Add experiment information
        json_obj['experiment'] = {
            'id' : experiment.identifier,
            'name' : experiment.name,
            'links': hateoas.self_reference_set(
                self.refs.experiment_reference(experiment.identifier)
            )
        }
        # Add state information
        json_obj['state'] =  str(model_run.state)
        if model_run.state.is_failed:
            json_obj['errors'] = model_run.state.errors
        # Add life cycle Timestamps
        json_obj['schedule'] = model_run.schedule
        # Add model run arguments
        json_obj['arguments'] = [
            {
                'name' : attr,
                'value' : model_run.arguments[attr].value
            } for attr in model_run.arguments
        ]
        # Return complete serialization of model run
        return json_obj

    def experiment_predictions_to_json(self, object_listing, properties, experiment):
        """Json serialization for listing of model runs that are assocaited with
        a given experiment.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of model runs
        properties : List(string)
            List of additional properties to include with every object in the
            listing or None.
        experiment : string
            Unique experiment identifier

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
        return self.listing_to_json(
            object_listing,
            properties,
            self.refs.experiments_predictions_reference(experiment),
            links={
                hateoas.REF_KEY_EXPERIMENT:
                self.refs.experiment_reference(experiment)
            }
        )

    def image_file_to_json(self, img):
        """Json serialization for image file object.

        Parameters
        ----------
        img : db.image.ImageHandle
            Image file handle

        Returns
        -------
        Json-like object
            Representation of object handle in Json format
        """
        return self.object_to_json(img)

    def image_files_to_json(self, object_listing, properties):
        """Json serialization for image files listing.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of image files
        properties : List(string)
            List of additional properties to include with every object in the
            listing or None.

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
        return self.listing_to_json(
            object_listing,
            properties,
            self.refs.image_files_reference()
        )

    def image_group_images_to_json(self, image_listing, image_group_id):
        """Json serialization for listing of group images. Since group images
        are not ObjectHandles the method cannot use the default list
        serialization.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of images in an image group
        image_group_id : string
            Unique identifier of the image group that contains the listed images

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
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
        return self.items_listing_to_json(
            image_listing,
            items,
            None,
            self.refs.image_group_images_list_reference(image_group_id),
            links={
                hateoas.REF_KEY_IMAGE_GROUP :
                self.refs.image_group_reference(image_group_id)
            }
        )

    def image_group_to_json(self, img_grp):
        """Json serialization for image group object.

        Parameters
        ----------
        img_grp : db.image.ImageGroupHandle
            Image group handle

        Returns
        -------
        Json-like object
            Representation of object handle in Json format
        """
        # Get basic object serialization
        json_obj = self.object_to_json(img_grp)
        # Add list of contained images
        json_obj['images'] =  {
            'count' : len(img_grp.images),
            'links' : hateoas.self_reference_set(
                self.refs.image_group_images_list_reference(img_grp.identifier)
            )
        }
        # Add image group options
        json_obj['options'] = [
            {
                'name' : attr,
                'value' : img_grp.options[attr].value
            } for attr in img_grp.options
        ]
        # Return complete serialization of image group
        return json_obj

    def image_groups_to_json(self, object_listing, properties):
        """Json serialization for image groups listing.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of image groups
        properties : List(string)
            List of additional properties to include with every object in the
            listing or None.

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
        return self.listing_to_json(
            object_listing,
            properties,
            self.refs.image_groups_reference()
        )

    def items_listing_to_json(self, object_listing, items, properties, listing_url, links=None):
        """Generic serializer for a list of items. Used for object listings and
        group image listings.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of objects resulting from list_objects() call
        items : List(Json-like object)
            Json serialization of list items
        properties : List(string)
            List of property names or None.
        listing_url : string
            base Url for given object listing
        links : Dictionary, optional
            Optional dictionary of references to include in listings reference
            list

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
        # Generate listing navigation references
        nav = hateoas.PaginationReferenceFactory(
            object_listing,
            properties,
            listing_url
        ).navigation_references(links=links)
        # Return Json-like object contaiing items, references, and listing
        # arguments and statistics
        return {
            'items' : items,
            'offset' : object_listing.offset,
            'limit' : object_listing.limit,
            'count' : len(items),
            'totalCount' : object_listing.total_count,
            'links' : nav
        }

    def listing_to_json(self, object_listing, properties, listing_url, links=None):
        """Create Json serialization for object listing. The property set
        defines additional properties to include with every object in the
        listing. If property set is None no additional properties will be
        included for objects in the listing.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of objects resulting from list_objects() call
        properties : List(string)
            List of property names or None.
        listing_url : string
            base Url for given object listing

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
        # Generate list of object in listing
        items = []
        for obj in object_listing.items:
            # Create baseic object representation
            item = {
                'id' : obj.identifier,
                'name' : obj.name,
                'timestamp' : str(obj.timestamp.isoformat()),
                'links' : self.refs.object_references(obj)
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
        return self.items_listing_to_json(
            object_listing,
            items,
            properties,
            listing_url,
            links=links
        )

    def object_to_json(self, obj):
        """Basic Json serialization for given object.

        Parameters
        ----------
        obj : (sub-class of)ObjectHandle
            Object handle

        Returns
        -------
        Json-like object
            Representation of object handle in Json format
        """
        # Generate basic object serialization
        return {
            'id' : obj.identifier,
            'name' : obj.name,
            'timestamp' : str(obj.timestamp.isoformat()),
            'properties' : utils.to_list(obj.properties),
            'links' : self.refs.object_references(obj)
        }

    def response_success(self, obj):
        """Generate response for successful object manipulation.

        Parameters
        ----------
        obj : (sub-class of)ObjectHandle

        Returns
        -------
        Json-like object
            Representation of successful object manipulation in Json format
        """
        return {
            'result' : 'SUCCESS',
            'links': self.refs.object_references(obj)
        }

    def service_description(self, name, descriptors):
        """Service description containing web service name, textual description
        of service resources, and a list of references to various resources.

        Parameters
        ----------
        name : string
            Descriptive Web service name
        descriptors: Dictionary({'id':string, 'title':string,'text':string})
            Dictionary of descriptions for service components

        Returns
        -------
        Json-like object
            Service description resource in Json format
        """
        return {
            'name': name,
            'descriptors' : descriptors,
            'links' : self.refs.service_references()
        }

    def subject_to_json(self, subject):
        """Json serialization for subject.

        Parameters
        ----------
        subject : db.subject.SubjectHandle
            Subject handle

        Returns
        -------
        Json-like object
            Representation of object handle in Json format
        """
        return self.object_to_json(subject)

    def subjects_to_json(self, object_listing, properties):
        """Json serialization for subjects listing.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            Listing of subjects
        properties : List(string)
            List of additional properties to include with every object in the
            listing or None.

        Returns
        -------
        Json-like object
            Object listing resource in Json format
        """
        return self.listing_to_json(
            object_listing,
            properties,
            self.refs.subjects_reference()
        )
