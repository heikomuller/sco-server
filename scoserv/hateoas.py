"""Collection of classes and methods to generate URL's for API resources."""

import utils


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Object listing element names
# ------------------------------------------------------------------------------

# Reference type label
LIST_KEY = 'rel'
# Reference value label
LIST_VALUE = 'href'

# ------------------------------------------------------------------------------
# Object listing query parameter
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
# Reference list keys
# ------------------------------------------------------------------------------

# Delete object
REF_KEY_DELETE = 'delete'
# Download data file
REF_KEY_DOWNLOAD = 'download'
# Model run listing's reference to it's experiment
REF_KEY_EXPERIMENT = 'experiment'
# Get experiments fMRI data
REF_KEY_FMRI_GET = 'fmri.get'
# Update experiments fMRI data
REF_KEY_FMRI_UPLOAD = 'fmri.upload'
# Group image listing's reference to group object
REF_KEY_IMAGE_GROUP = 'group'
# List experiment predictions
REF_KEY_PREDICTIONS_LIST = 'predictions.list'
# Create new predictive model runs
REF_KEY_PREDICTIONS_RUN = 'predictions.run'
# Self reference
REF_KEY_SELF = 'self'
# Update object options
REF_KEY_UPDATE_OPTIONS = 'options'
# Upsert object property
REF_KEY_UPSERT_PROPERTY = 'properties'

# Listing pagination navigators

# Navigate to first page in object listing
REF_KEY_PAGE_FIRST = 'first'
# Navigate to last page in object listing
REF_KEY_PAGE_LAST = 'last'
# Navigate to next page in object listing
REF_KEY_PAGE_NEXT = 'next'
# Navigate to previous page in object listing
REF_KEY_PAGE_PREVIOUS = 'prev'

# Service description references

# List experiments
REF_KEY_SERVICE_EXPERIMENTS_LIST = 'experiments.list'
# Create new experiment
REF_KEY_SERVICE_EXPERIMENTS_CREATE = 'experiments.create'
# Upload image file or image archive
REF_KEY_SERVICE_IMAGES_UPLOAD = 'images.upload'
# List image files
REF_KEY_SERVICE_IMAGE_FILES_LIST = 'images.files.list'
# List image groups
REF_KEY_SERVICE_IMAGE_GROUPS_LIST = 'images.groups.list'
# List subjects
REF_KEY_SERVICE_SUBJECTS_LIST = 'subjects.list'
# Create new subject via upload
REF_KEY_SERVICE_SUBJECTS_UPLOAD = 'subjects.upload'

# ------------------------------------------------------------------------------
# Url components
# ------------------------------------------------------------------------------

# Url component for experiments
URL_KEY_EXPERIMENTS = 'experiments'
# Url component for fMRI data
URL_KEY_FMRI = 'fmri'
# Url component for image objects
URL_KEY_IMAGES = 'images'
# Url component for image file objects
URL_KEY_IMAGE_FILES = 'files'
# Url component for image group objects
URL_KEY_IMAGE_GROUPS = 'groups'
# Url component for model run objects
URL_KEY_PREDICTIONS = 'predictions'
# Url component for subjects
URL_KEY_SUBJECTS = 'subjects'

# Url suffix for download references
URL_SUFFIX_DOWNLOAD = 'data'
#Url suffix for images in an image group
URL_SUFFIX_IMAGES = 'images'
#Url suffix for references to update object options
URL_SUFFIX_OPTIONS = 'options'
# Url suffix for references to upsert object properties
URL_SUFFIX_PROPERTIES = 'properties'


# ------------------------------------------------------------------------------
#
# Navigation references factory for object listing pagination
#
# ------------------------------------------------------------------------------

class PaginationReferenceFactory(object):
    """Factory for navigation references for object listings."""
    def __init__(self, object_listing, properties, url):
        """Initialize object listing properties that are used for pagination Url
        generation.

        Parameters
        ----------
        object_listing : db.datastore.ObjectListing
            The object listing result for which navigation references are
            generated
        properties: List(string)
            List of additional properties to be included in object listing.
        url : string
            Base Url for object listing
        """
        self.url = url
        self.offset = object_listing.offset
        self.limit = object_listing.limit
        self.total_count = object_listing.total_count
        self.properties = ','.join(properties) if not properties is None else None

    def decorate_listing_url(self, offset):
        """Get decorated URL to navigate object listing. Only the offset value
        changes for different navigation Url's.

        Parameters
        ----------
        offset : int
            Index of first element of the page that is to be displayed.

        Returns
        -------
        string
            Decorated object listing Url.
        """
        query = QPARA_OFFSET + '=' + str(offset)
        if self.limit >= 0:
            query += '&' + QPARA_LIMIT + '=' + str(self.limit)
        if not self.properties is None:
            query += '&' + QPARA_PROPERTIES + '=' + self.properties
        return self.url + '?' + query

    def navigation_references(self, links=None):
        """Set of navigation references for object listing.

        Parameters
        ----------
        links : Dictionary, optional
            Optional list of references to include in the listings references
            list

        Returns
        -------
        List
            List of reference objects, i.e., [{rel:..., href:...}]
        """
        # Include listing self reference
        nav = {REF_KEY_SELF : self.url}
        # Navigate to first page
        nav[REF_KEY_PAGE_FIRST] = self.decorate_listing_url(0)
        # Navigate to last page
        if self.limit > 0 and (self.total_count - self.limit) > 0:
            nav[REF_KEY_PAGE_LAST] = self.decorate_listing_url(self.total_count - self.limit)
        # Navigate to next page
        if self.offset >= 0 and self.limit > 0 and (self.offset + self.limit) < self.total_count:
            nav[REF_KEY_PAGE_NEXT] = self.decorate_listing_url(self.offset + self.limit)
        # Navigate to previous page
        if self.offset > 0 and self.limit > 0:
            if self.limit <= self.offset:
                nav[REF_KEY_PAGE_PREVIOUS] = self.decorate_listing_url(self.offset - self.limit)
            else:
                nav[REF_KEY_PAGE_PREVIOUS] = self.decorate_listing_url(0)
        # Merge navigation references with optional likns disctionary if given
        if not links is None:
            for rel in links:
                nav[rel] = links[rel]
        # Return list of references
        return to_references(nav)


# ------------------------------------------------------------------------------
#
# Hypermedia as the Engine of Application State (HATEOAS) - References
#
# ------------------------------------------------------------------------------

class HATEOASReferenceFactory:
    """Factory object for object references. Generates list of references for
    API resources, e.g., self reference, reference to parent object, URL's for
    applicable API methods.

    Attrributes
    -----------
    base_url : string
        Base Url for all resource references
    """
    def __init__(self, base_url):
        """Initialize the factory object by providing the base Url for
        generating resource references.

        Parameters
        ----------
        base_url : string
            Base Url for all resource references'
        """
        self.base_url = base_url
        # Remove trailing '/'
        while self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]

    def experiment_reference(self, experiment_id):
        """Self reference to experiment object.

        Parameters
        ----------
        experiments_id : string
            Unique experiment identifier

        Returns
        -------
        string
            Experiment Url
        """
        return self.experiments_reference() + '/' + experiment_id

    def experiments_reference(self):
        """Base Url for experiment objects.

        Returns
        -------
        string
            Experiment objects base Url
        """
        return self.base_url + '/' + URL_KEY_EXPERIMENTS

    def experiments_fmri_reference(self, experiment_id):
        """Base Url for experiments fMRI objects. fMRI's are weak entities.
        Their Urls are prefixed by the identifying experiment Url.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier

        Returns
        -------
        string
            Functional MRI objects base Url
        """
        # Url for identifying experiment
        experiment_url = self.experiment_reference(experiment_id)
        return experiment_url + '/' + URL_KEY_FMRI

    def experiments_predictions_reference(self, experiment_id):
        """Base Url for experiments model runs. Model runs are weak entities.
        Their Urls are prefixed by the identifying experiment Url.

        Parameters
        ----------
        experiment_id : string
            Unique experiment identifier

        Returns
        -------
        string
            Model run objects base Url
        """
        # Url for identifying experiment
        experiment_url = self.experiment_reference(experiment_id)
        return experiment_url + '/' + URL_KEY_PREDICTIONS

    def image_file_reference(self, image_id):
        """Self reference for image file with given identifier.

        Parameters
        ----------
        image_id : string
            Unique image file identifier

        Returns
        -------
        string
            Image File Url
        """
        return self.image_files_reference() + '/' + image_id

    def image_files_reference(self):
        """Base Url for image file objects.

        Returns
        -------
        string
            Image file objects base Url
        """
        return self.base_url + '/' + URL_KEY_IMAGES + '/' + URL_KEY_IMAGE_FILES

    def image_group_images_list_reference(self, identifier):
        """Base Url to list images in a given image group object.

        Parameters
        ----------
        identifier : string
            Unique image group identifier

        Returns
        -------
        string
            Group images listing Url
        """
        return self.base_url + '/' + URL_KEY_IMAGES + '/' + URL_KEY_IMAGE_GROUPS + '/' + identifier + '/' + URL_SUFFIX_IMAGES

    def image_group_reference(self, image_group_id):
        """Self reference for image group with given identifier.

        Parameters
        ----------
        image_group_id : string
            Unique image group identifier

        Returns
        -------
        string
            Image Group Url
        """
        return self.image_groups_reference() +'/' + image_group_id

    def image_groups_reference(self):
        """Base Url for image group objects.

        Returns
        -------
        string
            Image groups objects base Url
        """
        return self.base_url + '/' + URL_KEY_IMAGES + '/' + URL_KEY_IMAGE_GROUPS

    def object_references(self, obj):
        """List of references for given object. Object type will determine the
        references in the returned listing.

        Raises ValueError for objects of unknown types. For each new data type
        that is supported by the SCO data store this method should be extended.

        Parameters
        ----------
        obj : (sub-class of)ObjectHandle
            Handle for database object

        Returns
        -------
        List
            List of reference objects, i.e., [{rel:..., href:...}]
        """
        if obj.is_experiment:
            # Get base references.
            self_ref = self.experiment_reference(obj.identifier)
            refs = base_reference_set(self_ref)
            # Delete download reference from basic set
            del refs[REF_KEY_DOWNLOAD]
            # Add reference for predictions listing and to run new prediction
            prediction_url = self.experiments_predictions_reference(obj.identifier)
            refs[REF_KEY_PREDICTIONS_LIST] = prediction_url
            refs[REF_KEY_PREDICTIONS_RUN] = prediction_url
            # Add reference to fMRI data (if present) and to upload fMRI
            fmri_url = self.experiments_fmri_reference(obj.identifier)
            refs[REF_KEY_FMRI_UPLOAD] = fmri_url
            if not obj.fmri_data is None:
                refs[REF_KEY_FMRI_GET] = fmri_url
            # Return reference list
            return to_references(refs)
        elif obj.is_functional_data:
            # fMRI data objecs have the basic reference set
            self_ref = self.experiments_fmri_reference(obj.experiment)
            return to_references(base_reference_set(self_ref))
        elif obj.is_model_run:
            # Get base references.
            type_url = self.experiments_predictions_reference(obj.experiment)
            refs = base_reference_set(type_url + '/' + obj.identifier)
            # Remove download link if model run is not in SUCCESS state
            if not obj.state.is_success:
                del refs[REF_KEY_DOWNLOAD]
            # Return reference list
            return to_references(refs)
        elif obj.is_image:
            # Image files have the basic reference set
            self_ref = self.image_file_reference(obj.identifier)
            return to_references(base_reference_set(self_ref))
        elif obj.is_image_group:
            # Get basic reference set
            self_ref = self.image_group_reference(obj.identifier)
            refs = base_reference_set(self_ref)
            # Add reference to update image group options
            refs[REF_KEY_UPDATE_OPTIONS] = self_ref + '/' + URL_SUFFIX_OPTIONS
            # Return reference list
            return to_references(refs)
        elif obj.is_subject:
            # Subjects have the basic reference set
            self_ref = self.subject_reference(obj.identifier)
            return to_references(base_reference_set(self_ref))
        else:
            raise ValueError('unknown object type')

    def service_references(self):
        """Get primary references to access resources and methods of the
        Web API.

        Returns
        -------
        List
            List of reference objects, i.e., [{rel:..., href:...}].
        """
        return to_references({
            REF_KEY_SELF : self.base_url,
            REF_KEY_SERVICE_EXPERIMENTS_LIST : self.experiments_reference(),
            REF_KEY_SERVICE_EXPERIMENTS_CREATE : self.experiments_reference(),
            REF_KEY_SERVICE_IMAGES_UPLOAD : self.base_url + '/' + URL_KEY_IMAGES + '/upload',
            REF_KEY_SERVICE_IMAGE_FILES_LIST : self.image_files_reference(),
            REF_KEY_SERVICE_IMAGE_GROUPS_LIST : self.image_groups_reference(),
            REF_KEY_SERVICE_SUBJECTS_LIST : self.subjects_reference(),
            REF_KEY_SERVICE_SUBJECTS_UPLOAD : self.subjects_reference()
        })

    def subject_reference(self, subject_id):
        """Self reference for subject with given identifier.

        Parameters
        ----------
        subject_id : string
            Unique subject identifier

        Returns
        -------
        string
            Subject Url
        """
        return self.subjects_reference() +'/' + subject_id

    def subjects_reference(self):
        """Base Url for subject data objects.

        Returns
        -------
        string
            Subjects base Url
        """
        return self.base_url + '/' + URL_KEY_SUBJECTS



# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

def base_reference_set(self_ref):
    """Default set of references for database objects. Contains self reference,
    and references to API calls to delete, download, and property upsert.

    Parameters
    ----------
    self_ref : string
        Self reference for object for which reference list is generated

    Returns
    -------
    Dictionary
        Dictionary of references
    """
    return {
        REF_KEY_SELF : self_ref,
        REF_KEY_DELETE : self_ref,
        REF_KEY_DOWNLOAD : self_ref + '/' + URL_SUFFIX_DOWNLOAD,
        REF_KEY_UPSERT_PROPERTY : self_ref + '/' + URL_SUFFIX_PROPERTIES
    }


def self_reference_set(self_ref):
    """Reference list containing as single element a self-reference to a
    Web resource.

    Parameters
    ----------
    self_ref : string
        Object self reference Url

    Returns
    -------
    List
        List of reference objects, i.e., [{rel:..., href:...}]
    """
    return to_references({REF_KEY_SELF: self_ref})


def to_references(dictionary):
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
    return utils.to_list(dictionary, label_key=LIST_KEY, label_value=LIST_VALUE)
