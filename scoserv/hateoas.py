"""Collection of classes and methods to generate URL's for API resources."""

import reqexcpt as exceptions
import utils


# ------------------------------------------------------------------------------
# Query parameter for object listings
# ------------------------------------------------------------------------------

# List of attributes to include for each item in listings
QPARA_PROPERTIES = 'properties'
# Limit number of items in result
QPARA_LIMIT = 'limit'
# Set offset in collection
QPARA_OFFSET = 'offset'
# Model run state filter
QPARA_STATE = 'state'

# ------------------------------------------------------------------------------
# Decorator for pagination of object listing URL's
# ------------------------------------------------------------------------------

class PaginationDecorator:
    """Decorator for URLs. Provides methods to decorate a given URL for
    navigation to the first, last, previous, and next page in a listing of
    objects.
    """
    def __init__(self, url, offset, limit, total_count, properties):
        """Initialize the URL and the current values for relevant list navigation
        and retrieval query parameter.

        Parameters
        ----------
        url : string
            The base URL that is being decorated. The URL references an object
            listing. It is expected that the URL does not contain a query part.
        offset : int
            The current offset in the object listing (ignored if < 0)
        limit : int
            Maximum number listing of entries to be returned. A negative value
            indicates that all elements are to be returned.
        total_count : int
            Total number of elements in the object listing.
        properties:
            Value for the properties to be included parameter. Ignored if None
            or empty.
        """
        self.url = url
        self.offset = offset
        self.limit = limit
        self.total_count = total_count
        self.properties = properties

    def decorate(self, page_offset):
        """Get decorated URL to navigate to the first page in the referenced
        object listing.

        Parameters
        ----------
        page_offset : int
            Index of first element of the page that is to be displayed.

        Returns
        -------
        string
            Decorated object listing URL.
        """
        query = QPARA_OFFSET + '=' + str(page_offset)
        if self.limit >= 0:
            query += '&' + QPARA_LIMIT + '=' + str(self.limit)
        if not self.properties is None:
            query += '&' + QPARA_PROPERTIES + '=' + self.properties
        return self.url + '?' + query

    def first(self):
        """Get decorated URL to navigate to the first page in the referenced
        object listing.

        Returns
        -------
        string
            Decorated object listing URL.
        """
        return self.decorate(0)

    def last(self):
        """Get decorated URL to navigate to the last page in the referenced
        object listing.

        Returns
        -------
        string
            Decorated object listing URL.
        """
        # Check if the given values for limit and total_count allow more than
        # one page. Otherwiese, navigate to the first page.
        if self.limit >= 0 and self.limit < self.total_count:
            return self.decorate(self.total_count - self.limit)
        else:
            return self.first()

    def next(self):
        """Get decorated URL to navigate to the next page in the referenced
        object listing. The result is None if the next page would be empty
        because the offset is after the last element.

        Returns
        -------
        string
            Decorated object listing URL or None.
        """
        # Return None if offset or limit are not set, i.e., have
        # negative value.
        if self.offset < 0 or self.limit < 0:
            return None
        # Check whether there is a next page or not
        if (self.offset + self.limit) < self.total_count:
            return self.decorate(self.offset + self.limit)
        else:
            return None

    def prev(self):
        """Get decorated URL to navigate to the previous page in the referenced
        object listing.

        Returns
        -------
        string
            Decorated object listing URL.
        """
        # Return None if offset or limit are not set, i.e., have
        # negative value.
        if self.offset < 0 or self.limit < 0:
            return None
        # Check if the previous page is the first one.
        if self.limit <= self.offset:
            return self.decorate(self.offset - self.limit)
        else:
            return self.first()

class ObjectUrls(object):
    """Factory object for individual object type Urls. This class implements
    the general pattern for resource Urls.

    The Url factory is intended for 'strong' entities, i.e., objects that have
    a single key as their identifier.

    Attributes
    ----------
    base_url : string
        Base URL used for all URL's returned by the factory object.
    class_identifier : string
        Unique identifier for class objects in the API
    """
    def __init__(self, base_url, class_identifier):
        """ Initialize the URL factory by setting the base url.

        Parameters
        ----------
        base_url : string
            Base URL used for all URL's returned by the factory object.
        class_identifier : string
            Unique identifier for class objects in the API
        """
        self.base_url = base_url
        self.class_identifier = class_identifier

    def create(self):
        """Get Url to create object.

        Returns
        -------
        string
            Resource Url
        """
        return self.list()

    def delete(self, identifier):
        """ Get Url to delete object with given identifier.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique object identifier

        Returns
        -------
        string
            Resource Url
        """
        return self.get(identifier)

    def download(self, identifier):
        """ Get Url to download files associated with object having the given
        identifier.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique object identifier

        Returns
        -------
        string
            Resource Url
        """
        return self.get(identifier) + '/data'

    def get(self, identifier):
        """ Get Url for a particular object identified by the given identifier.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique object identifier

        Returns
        -------
        string
            Resource Url
        """
        return self.list() + '/' + identifier.keys[0]

    def list(self):
        """Get Url for object listing. This is also the base URL for all other
        object Urls.

        Returns
        -------
        string
            Resource Url.
        """
        return self.base_url + '/' + self.class_identifier

    def update(self, identifier):
        """ Get Url to update object identified by the given identifier.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique object identifier

        Returns
        -------
        string
            Resource Url
        """
        return self.get(identifier)

    def upsert_property(self, identifier):
        """ Get Url to upsert a property of the functional data object
        identified by the given identifier.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique functional data object identifier

        Returns
        -------
        string
            Functional data object URL
        """
        return self.get(identifier) + '/properties'


class ExperimentUrls(ObjectUrls):
    def __init__(self, base_url, class_identifiers):
        """ Initialize the URL factory by setting the base url and class
        identifier.

        Parameters
        ----------
        base_url : string
            Base URL used for all URL's returned by the factory object.
        class_identifiers : Dictionary()
            Dictionary of class identifiers for the experiment and its weak
            entities FMRI and PREDICTION. Expects a dictionary with three
            keys: 'self' (=class identifier for experiment objects), 'fmri'
            (= class identifier form functional MRI data objects), and
            'prediction' (= class identifier form prediction objects).
        """
        super(ExperimentUrls, self).__init__(base_url, class_identifiers['self'])
        self.class_identifiers = class_identifiers

    def fmris_download(self, identifier):
        """ Get Url to download a functional MRI data file that is associated
        with an experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique fMRI data object identifier

        Returns
        -------
        string
            Functional data file download URL
        """
        return self.fmris_get(identifier) + '/data'

    def fmris_get(self, identifier):
        """ Get Url to retrieve a functional MRI data object that is associated
        with an experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique fMRI data object identifier

        Returns
        -------
        string
            Functional data object URL
        """
        return self.fmris_upload(identifier) + '/' + identifier.keys[-1]

    def fmris_upload(self, identifier):
        """ Get Url to upload a functional MRI data file and associate it with
        the experiment that is identified by the given identifier.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique experiment object identifier

        Returns
        -------
        string
            Functional data file upload URL
        """
        return self.get(identifier) + '/' + self.class_identifiers['fmri']

    def fmris_upsert_property(self, identifier):
        """ Get Url to upsert property of a functional MRI data object that is
        associated with an experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique fMRI data object identifier

        Returns
        -------
        string
            Upsert data object property URL
        """
        return self.fmris_get(identifier) + '/properties'

    def predictions_create(self, identifier):
        """ Get Url to create a model run object that is associated with an
        experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique model run object identifier

        Returns
        -------
        string
            Create model run URL
        """
        return self.predictions_list(identifier)

    def predictions_delete(self, identifier):
        """ Get Url to delete a model run object and its prediction result.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique model run object identifier

        Returns
        -------
        string
            Delete model run URL
        """
        return self.predictions_get(identifier)

    def predictions_download(self, identifier):
        """ Get Url to download prediction result for a model run object that is
        associated with an experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique model run object identifier

        Returns
        -------
        string
            Download prediction result URL
        """
        return self.predictions_get(identifier) + '/data'

    def predictions_get(self, identifier):
        """ Get Url to retrieve a model run object that is associated with an
        experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique model run object identifier

        Returns
        -------
        string
            Retrieve model run URL
        """
        return self.predictions_list(identifier) + '/' + identifier.keys[-1]

    def predictions_list(self, identifier):
        """ Get Url to get a listing of all a model run objects that are
        associated with an experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique model run object identifier

        Returns
        -------
        string
            List model runs URL
        """
        return self.get(identifier) + '/' + self.class_identifiers['prediction']

    def predictions_upsert_property(self, identifier):
        """ Get Url to upsert property of a model run object that is associated
        with an experiment.

        Parameters
        ----------
        identifier : db.datastore.ObjectId
            Unique model run object identifier

        Returns
        -------
        string
            Upsert data object property URL
        """
        return self.predictions_get(identifier) + '/properties'


class UrlFactory:
    """Factory object for Web API URL's. Encapsulates generation of URL's for
    API resources in a single class.

    Attributes
    ----------
    base_url : string
        Base URL used for all URL's returned by the factory object.
    experiments : ObjectUrls
        Url factory for experiment resources
    images : ObjectUrls
        Url factory for image resources
    image_groups : ObjectUrls
        Url factory for image group resources
    subjects : ObjectUrls
        Url factory for subject resources
    """
    def __init__(self, base_url):
        """ Initialize the URL factory by setting the base url.

        Parameters
        ----------
        base_url : string
            Base URL used for all URL's returned by the factory object.
        """
        self.base_url = base_url
        # Remove trailing '/'
        while self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]
        self.experiments = ExperimentUrls(self.base_url, {
            'self' : 'experiments',
            'fmri' : 'fmri',
            'prediction' : 'predictions'
        })
        self.images = ObjectUrls(self.base_url, 'images/files')
        self.image_groups = ObjectUrls(self.base_url, 'images/groups')
        self.subjects = ObjectUrls(self.base_url, 'subjects')


# ------------------------------------------------------------------------------
# HATEOAS - Object references
# ------------------------------------------------------------------------------

class HATEOASReferenceFactory:
    """Factory object for object references. Generates list of references for
    API resources, e.g., self reference, reference to parent object, URL's for
    applicable API methods.

    Attrributes
    -----------
    urls : UrlFactory
        Factory for resource Urls'
    """
    def __init__(self, urls):
        """Initialize the factory object by providing the UrlFactory use for
        generating resource references.

        Parameters
        ----------
        urls : UrlFactory
            Factory for resource Url's
        """
        self.urls = urls

    def object_references(self, db_obj):
        """Get object specific list of references.

        Parameters
        ----------
        db_obj : (subclass of)datastore.DBObject

        Returns
        -------
        List
            List of reference objects, i.e., [{rel:..., href:...}].
        """
        links = {}
        # Object type specific references. Raises an exception if object type
        # is unknown.
        if db_obj.is_subject:
            links['delete'] = self.urls.subjects.delete(db_obj.identifier)
            links['download'] = self.urls.subjects.download(db_obj.identifier)
            links['upsert'] = self.urls.subjects.upsert_property(db_obj.identifier)
        elif db_obj.is_experiment:
            links['delete'] = self.urls.experiments.delete(db_obj.identifier)
            links['fmri.upload'] = self.urls.experiments.fmris_upload(db_obj.identifier)
            links['predictions.create'] = self.urls.experiments.predictions_create(db_obj.identifier)
            links['predictions.list'] = self.urls.experiments.predictions_list(db_obj.identifier)
            links['upsert'] = self.urls.experiments.upsert_property(db_obj.identifier)
        elif db_obj.is_fmri_data:
            links['download'] = self.urls.experiments.fmris_download(db_obj.identifier)
            links['upsert'] = self.urls.experiments.fmris_upsert_property(db_obj.identifier)
        elif db_obj.is_prediction:
            # Only include download link if model run finished successfully
            if db_obj.is_success:
                links['download'] = self.urls.experiments.predictions_download(db_obj.identifier)
            links['delete'] = self.urls.experiments.predictions_delete(db_obj.identifier)
            links['upsert'] = self.urls.experiments.predictions_upsert_property(db_obj.identifier)
        elif db_obj.is_image:
            links['delete'] = self.urls.images.delete(db_obj.identifier)
            links['download'] = self.urls.images.download(db_obj.identifier)
            links['upsert'] = self.urls.images.upsert_property(db_obj.identifier)
        elif db_obj.is_image_group:
            links['delete'] = self.urls.image_groups.delete(db_obj.identifier)
            links['download'] = self.urls.image_groups.download(db_obj.identifier)
            links['upsert'] = self.urls.image_groups.upsert_property(db_obj.identifier)
        else:
            raise exceptions.UnknownObjectType(db_obj.type)
        # Return concatenation of self reference and object specific references.
        return self.to_references(links) + self.object_self_reference(db_obj)

    def object_self_reference(self, db_obj):
        """Get object specific self reference as single item in a reference set.

        Parameters
        ----------
        db_obj : (subclass of)datastore.DBObject

        Returns
        -------
        List
            List of reference objects, i.e., [{rel:..., href:...}], with one
            item.
        """
        links = {}
        # Add self reference depending on object type. Raises an exception if
        # object type is unknown.
        if db_obj.is_subject:
            ref = self.urls.subjects.get(db_obj.identifier)
        elif db_obj.is_experiment:
            ref = self.urls.experiments.get(db_obj.identifier)
        elif db_obj.is_fmri_data:
            ref = self.urls.experiments.fmris_get(db_obj.identifier)
        elif db_obj.is_prediction:
            ref = self.urls.experiments.predictions_get(db_obj.identifier)
        elif db_obj.is_image:
            ref = self.urls.images.get(db_obj.identifier)
        elif db_obj.is_image_group:
            ref = self.urls.image_groups.get(db_obj.identifier)
        else:
            raise exceptions.UnknownObjectType(db_obj.type)
        # Return list with one (self) reference object.
        return self.to_references({'self' : ref})

    def service_references(self):
        """Get primary references to access resources and methods of the
        Web API.

        Returns
        -------
        List
            List of reference objects, i.e., [{rel:..., href:...}].
        """
        return self.to_references({
            'self' : self.urls.base_url,
            'experiments.create' : self.urls.experiments.create(),
            'experiments.list' : self.urls.experiments.list(),
            'subjects.list' : self.urls.subjects.list(),
            'subjects.upload' : self.urls.subjects.create(),
            'images.list' : self.urls.images.list(),
            'images.upload' : self.urls.images.create(),
            'imageGroups.list' : self.urls.image_groups.list(),
            'imageGroups.upload' : self.urls.images.create()
        })

    def to_references(self, dictionary):
        """Generate a HATEOAS reference listing from a dictionary. Keys in the
        dictionary define relationships ('rel') and associated values are
        URL's ('href').

        Parameters
        ----------
        dictionary : Dictionary
            Dictionary of references

        Returns
        -------
        List
            List of reference objects, i.e., [{rel:..., href:...}]
        """
        return utils.to_list(dictionary, label_key='rel', label_value='href')
