"""Standard Cortical Observer - Data Store API. The API brings together data
stores for individual object types and implements the calls that are exposed
via the Web API.
"""

import os
import shutil
import tarfile
import tempfile

import datastore
import experiment
import funcdata
import image
import prediction
import subject


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

# Sets of valid suffixes for file upload
ARCHIVE_SUFFIXES = set(['.tar', '.tar.gz', '.tgz'])


# ------------------------------------------------------------------------------
#
# File Information Objects
#
# ------------------------------------------------------------------------------

class FileInfo(object):
    """Information about downloadable files. this is a triple containing the
    reference to the file, the file mime-type, and file name.

    Attributes
    ----------
    file : File-type object
        Reference to file on disk
    mime_type : string
        The file's mime-type
    name: string
        File name
    """
    def __init__(self, file, mime_type, name):
        """Initialize the file information attributes.

        Parameters
        ----------
        file : File-type object
            Reference to file on disk
        mime_type : string
            The file's mime-type
        name: string
            File name
        """
        self.file = file
        self.mime_type = mime_type
        self.name = name


# ------------------------------------------------------------------------------
#
# API
#
# ------------------------------------------------------------------------------

class SCODataStore(object):
    """Interface to data stores for individual objects. Implements API calls
    for the Web API.
    """
    def __init__(self, mongo, base_dir):
        """Initialize the SCO data store by creating instances of data stores
        for individual data types.

        Parameters
        ----------
        mongo : mongo.MongoDBFactory()
            MongoDB database object factory
        base_dir : string
            The directory for storing data files. Directory will be created
            if it does not exist. For different types of objects various
            sub-directories will also be created if they don't exist.
        """
        db = mongo.get_database()
        # Ensure that varios data (sub-)folders exist
        abs_base_dir = create_dir(base_dir)
        funcdata_dir = create_dir(os.path.join(abs_base_dir, 'funcdata'))
        images_dir = create_dir(os.path.join(abs_base_dir, 'images'))
        image_files_dir = create_dir(os.path.join(images_dir, 'files'))
        image_groups_dir = create_dir(os.path.join(images_dir, 'groups'))
        subjects_dir = create_dir(os.path.join(abs_base_dir, 'subjects'))
        # Create object stores
        self.experiments = experiment.DefaultExperimentManager(db.experiments, db.predictions)
        self.funcdata = funcdata.DefaultFunctionalDataManager(db.funcdata, funcdata_dir)
        self.images = image.DefaultImageManager(db.images, image_files_dir)
        self.image_groups = image.DefaultImageGroupManager(db.imagegroups, image_groups_dir, self.images)
        self.predictions = prediction.DefaultModelRunManager(db.predictions)
        self.subjects = subject.DefaultSubjectManager(db.subjects, subjects_dir)

    # --------------------------------------------------------------------------
    # Experiments
    # --------------------------------------------------------------------------

    def experiments_create(self, subject, images, properties):
        """Create an experiment object with subject, and image group. Objects
        are referenced by their unique identifiers. The API ensure that at time
        of creation all referenced objects exist. Referential consistency,
        however, is currently not enforced when objects are deleted.

        Expects experiment name in property list. Raises ValueError if no valid
        name is given.

        If any of the referenced objects do not exist a ValueError is thrown.

        Parameters
        ----------
        subject : string
            Unique identifier of subject
        images : string
            Unique identifier of image group
        properties : Dictionary
            Set of experiment properties. Is required to contain at least the
            experiment name

        Returns
        -------
        ExperimentHandle
            Handle for created experiment object in database
        """
        # Ensure that reference subject exists
        if self.subjects_get(subject) is None:
            raise ValueError('unknown subject: ' + subject)
        # Ensure that referenced image group exists
        if self.image_groups_get(images) is None:
            raise ValueError('unknown image group: ' + images)
        return self.experiments.create_object(subject, images, properties)

    def experiments_delete(self, identifier):
        """Delete experiment with given identifier in the database. At the
        moment, this has no impact on other database objects that reference the
        experiment.

        Parameters
        ----------
        identifier : string
            Unique experiment identifier

        Returns
        -------
        ExperimentHandle
            Handle for deleted experiment or None if identifier is unknown
        """
        return self.experiments.delete_object(identifier)

    def experiments_fmri_create(self, identifier, filename):
        """Create functional data object from given file and associate the
        object with the specified experiment.

        Parameters
        ----------
        identifier : string
            Unique experiment identifier
        filename : File-type object
            Functional data file

        Returns
        -------
        FMRIDataHandle
            Handle for created fMRI object or None if identified experiment
            is unknown
        """
        # Get the experiment to ensure that it exist before we even create the
        # functional data object
        experiment = self.experiments_get(identifier)
        if experiment is None:
            return None
        # Create functional data object from given file
        fmri = self.funcdata.create_object(filename)
        # Update experiment to associate it with created fMRI object. Assign
        # result to experiment. Should the experiment have been deleted in
        # parallel the result will be None
        experiment = self.experiments.update_fmri_data(identifier, fmri.identifier)
        if experiment is None:
            # Delete fMRI object's data directory
            shutil.rmtree(fmri.directory)
            # Delete functional data object from databases
            self.funcdata.delete_object(fmri.identifier, erase=True)
            return None
        else:
            return funcdata.FMRIDataHandle(fmri, identifier)

    def experiments_fmri_delete(self, identifier):
        """Delete fMRI data object associated with given experiment.

        Parameters
        ----------
        identifier : string
            Unique experiment identifier

        Returns
        -------
        FMRIDataHandle
            Handle for deleted data object or None if experiment is unknown or
            has no fMRI data object associated with it
        """
        # Get experiment fMRI to ensure that it exists
        fmri = self.experiments_fmri_get(identifier)
        if fmri is None:
            return None
        # Delete reference fMRI data object and set reference in experiment to
        # None. If the result of delete fMRI object is None we return None.
        # Alternatively, throw an exception to signal invalid database state.
        fmri = self.funcdata.delete_object(fmri.identifier)
        if not fmri is None:
            self.experiments.update_fmri_data(identifier, None)
        return funcdata.FMRIDataHandle(fmri, identifier)

    def experiments_fmri_download(self, identifier):
        """Download the fMRI data file associated with given experiment.

        Parameters
        ----------
        identifier : string
            Unique experiment identifier

        Returns
        -------
        FileInfo
            Information about fMRI file on disk or None if experiment is
            unknown or has no fMRI data associated with it
        """
        # Get experiment fMRI to ensure that it exists
        fmri = self.experiments_fmri_get(identifier)
        if fmri is None:
            return None
        # Return information about fmRI data file
        return FileInfo(
            fmri.data_file,
            fmri.properties[datastore.PROPERTY_MIMETYPE],
            fmri.properties[datastore.PROPERTY_FILENAME]
        )

    def experiments_fmri_get(self, identifier):
        """Get fMRI data object that is associated with the given experiment.

        Parameters
        ----------
        identifier : string
            unique experiment identifier

        Returns
        -------
        FMRIDataHandle
            Handle for fMRI data object of None if (a) the experiment is unknown
            or (b) has no fMRI data object associated with it.
        """
        # Get experiment to ensure that it exists
        experiment = self.experiments_get(identifier)
        if experiment is None:
            return None
        # Check if experiment has fMRI data
        if experiment.fmri_data is None:
            return None
        # Get functional data object handle from database.
        func_data = self.funcdata.get_object(experiment.fmri_data)
        # Create fMRI handle from functional data handle
        return funcdata.FMRIDataHandle(func_data, identifier)

    def experiments_fmri_upsert_property(self, identifier, properties):
        """Upsert property of fMRI data object associated with given experiment.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        identifier : string
            Unique experiment identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        FMRIDataHandle
            Handle for updated object of None if object doesn't exist
        """
        # Get experiment fMRI to ensure that it exists. Needed to get fMRI
        # data object identifier for given experiment identifier
        fmri = self.experiments_fmri_get(identifier)
        if fmri is None:
            return None
        # Update properties for fMRI object using the object identifier
        return self.funcdata.upsert_object_property(fmri.identifier, properties)

    def experiments_get(self, identifier):
        """Retrieve experiment with given identifier.

        Parameters
        ----------
        identifier : string
            Unique experiment identifier

        Returns
        -------
        ExperimentHandle
            Hadle for experiment object or None if identifier is unknown.
        """
        return self.experiments.get_object(identifier)

    def experiments_list(self, limit=-1, offset=-1):
        """Retrieve list of all experiments in the data store.

        Parameters
        ----------
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
            Listing of experiment handles
        """
        return self.experiments.list_objects(limit=limit, offset=offset)

    def experiments_predictions_create(self, experiment, name, arguments=None, properties=None):
        """Create new model run for given experiment.

        Parameters
        ----------
        experiment : string
            Unique experiment identifier
        name : string
            User-provided name for the model run
        arguments : List(attribute.Attribute)
            List of arguments for model run
        properties : Dictionary, optional
            Set of model run properties.

        Returns
        -------
        ModelRunHandle
            Handle for created model run or None if experiment is unknown
        """
        # Get experiment to ensure that it exists
        if self.experiments_get(experiment) is None:
            return None
        # Return created model run
        return self.predictions.create_object(
            name,
            experiment,
            arguments=arguments,
            properties=properties
        )

    def experiments_predictions_delete(self, experiment, prediction, erase=False):
        """Delete given prediction for experiment.

        Parameters
        ----------
        experiment : string
            Unique experiment identifier
        prediction : string
            Unique prediction identifier
        erase : Boolean, optional
            If true, the model run will be deleted from the database. Used in
            case the sco backend could not start a model run after the record
            had already been created in the database.

        Returns
        -------
        ModelRunHandle
            Handle for deleted model run or None if unknown
        """
        # Get model run to ensure that it exists
        model_run = self.experiments_predictions_get(experiment, prediction)
        if model_run is None:
            return None
        # Return resutl of deleting model run. Could also raise exception in
        # case of invalid database state (i.e., prediction does not exist)
        return self.predictions.delete_object(model_run.identifier, erase=erase)

    def experiments_predictions_download(self, experiment, prediction):
        """Donwload the results of a prediction for a given experiment.

        Parameters
        ----------
        experiment : string
            Unique experiment identifier
        prediction : string
            Unique prediction identifier

        Returns
        -------
        FileInfo
            Information about prediction result file on disk or None if
            prediction is unknown or has no result
        """
        # Get model run to ensure that it exists
        model_run = self.experiments_predictions_get(experiment, prediction)
        if model_run is None:
            return None
        # Make sure the run has completed successfully
        if not model_run.state.is_success:
            return None
        # Get functional data object for result. Return None if this is None.
        # Alternatively throw an exception to signal invalid database state.
        funcdata = self.funcdata.get_object(model_run.state.model_output)
        if funcdata is None:
            return None
        # Return information about the result file
        return FileInfo(
            funcdata.data_file,
            funcdata.properties[datastore.PROPERTY_MIMETYPE],
            funcdata.properties[datastore.PROPERTY_FILENAME]
        )

    def experiments_predictions_get(self, experiment, prediction):
        """Get prediction object with given identifier for given experiment.

        Parameters
        ----------
        experiment : string
            Unique experiment identifier
        prediction : string
            Unique prediction identifier

        Returns
        -------
        ModelRunHandle
            Handle for the model run or None if experiment or prediction is
            unknown
        """
        # Get experiment to ensure that it exists
        if self.experiments_get(experiment) is None:
            return None
        # Get predition handle to ensure that it exists
        model_run = self.predictions.get_object(prediction)
        if model_run is None:
            return None
        # Perform additional check that prediction is for given experiment
        if experiment != model_run.experiment:
            return None
        # Return model run object
        return model_run

    def experiments_predictions_list(self, experiment, limit=-1, offset=-1):
        """List of all predictions for given experiment.

        Parameters
        ----------
        experiment : string
            Unique experiment identifier
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
            Listing of model run handles
        """
        # Get experiment to ensure that it exists
        if self.experiments_get(experiment) is None:
            return None
        # Return list of predictions
        return self.predictions.list_objects(
            query={'experiment' : experiment},
            limit=limit,
            offset=offset
        )

    def experiments_predictions_update_state(self, experiment, prediction, state):
        """Update state of given prediction.

        Parameters
        ----------
        experiment : string
            Unique experiment identifier
        prediction : string
            Unique prediction identifier
        state : ModelRunState
            New model run state

        Returns
        -------
        ModelRunHandle
            Handle for updated model run or None is prediction is undefined
        """
        # Get prediction to ensure that it exists
        model_run = self.experiments_predictions_get(experiment, prediction)
        if model_run is None:
            return None
        # Update predition state
        return self.predictions.update_state(prediction, state)

    def experiments_predictions_upsert_property(self, experiment, prediction, properties):
        """Upsert property of a prodiction for an experiment.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        experiment : string
            Unique experiment identifier
        prediction : string
            Unique prediction identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ModelRunHandle
            Handle for updated object of None if object doesn't exist
        """
        # Get predition to ensure that it exists. Ensures that the combination
        # of experiment and prediction identifier is valid.
        if self.experiments_predictions_get(experiment, prediction) is None:
            return None
        # Return result of upsert for identifier model run
        return self.predictions.upsert_object_property(prediction, properties)

    def experiments_upsert_property(self, identifier, properties):
        """Upsert property of given experiment.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        identifier : string
            Unique experiment identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ExperimentHandle
            Handle for updated object of None if object doesn't exist
        """
        return self.experiments.upsert_object_property(identifier, properties)

    # --------------------------------------------------------------------------
    # Images
    # --------------------------------------------------------------------------

    def images_create(self, filename):
        """Create and image file or image group object from the given file. The
        type of the created database object is determined by the suffix of the
        given file. An ValueError exception is thrown if the file has an unknown
        suffix.

        Raises ValueError if invalid file is given.

        Parameters
        ----------
        filename : File-type object
            File on local disk. Expected to be either an image file or an
            archive containing image.

        Returns
        -------
        DataObjectHandle
            Handle for create dtabase object. Either an ImageHandle or an
            ImageGroupHandle
        """
        # Check if file is a single image
        suffix = get_filename_suffix(filename, image.VALID_IMGFILE_SUFFIXES)
        if not suffix is None:
            # Create image object from given file
            return self.images.create_object(filename)
        # The file has not been recognized as a valid image. Check if the file
        # is a valid tar archive (based on suffix).
        suffix = get_filename_suffix(filename, ARCHIVE_SUFFIXES)
        if not suffix is None:
            # Unpack the file to a temporary folder .
            temp_dir = tempfile.mkdtemp()
            try:
                tf = tarfile.open(name=filename, mode='r')
                tf.extractall(path=temp_dir)
            except (tarfile.ReadError, IOError) as err:
                # Clean up in case there is an error during extraction
                shutil.rmtree(temp_dir)
                raise ValueError(str(err))
            # Get names of all files with valid image suffixes and create an
            # object for each image object
            group = []
            for img_file in image.get_image_files(temp_dir, []):
                img_obj = self.images.create_object(img_file)
                folder = img_file[len(temp_dir):-len(img_obj.name)]
                group.append(image.GroupImage(
                    img_obj.identifier,
                    folder,
                    img_obj.name,
                    img_obj.image_file
                ))
            # Create image group
            name = os.path.basename(os.path.normpath(filename))[:-len(suffix)]
            img_grp = self.image_groups.create_object(name, group, filename)
            # Delete the temporary folder
            shutil.rmtree(temp_dir)
            return img_grp
        else:
            # Not a valid file suffix
            raise ValueError('invalid file suffix: ' + os.path.basename(os.path.normpath(filename)))

    def image_files_delete(self, identifier):
        """Delete image object with given identifier. At the moment, this has no
        impact on objects referencing the image.

        Parameters
        ----------
        identifier : string
            Unique image identifier

        Returns
        -------
        ImageHandle
            Handle for deleted image or None if identifier is unknown
        """
        return self.images.delete_object(identifier)

    def image_files_download(self, identifier):
        """Get data file for image with given identifier.

        Parameters
        ----------
        identifier : string
            Unique image identifier

        Returns
        -------
        FileInfo
            Information about image file on disk or None if identifier
            is unknown
        """
        # Retrieve image to ensure that it exist
        img = self.image_files_get(identifier)
        if img is None:
            # Return None if image is unknown
            return None
        else:
            # Reference and information for original uploaded file
            return FileInfo(
                img.image_file,
                img.properties[datastore.PROPERTY_MIMETYPE],
                img.properties[datastore.PROPERTY_FILENAME]
            )

    def image_files_get(self, identifier):
        """Get image with given identifier.

        Parameters
        ----------
        identifier : string
            Unique image object identifier

        Returns
        -------
        ImageHandle
            Handle for image object or None if identifier is unknown
        """
        return self.images.get_object(identifier)

    def image_files_list(self, limit=-1, offset=-1):
        """Retrieve list of all images in the data store.

        Parameters
        ----------
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
            Listing of image handles
        """
        return self.images.list_objects(limit=limit, offset=offset)

    def image_files_upsert_property(self, identifier, properties):
        """Upsert property of given image.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        identifier : string
            Unique image object identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ImageHandle
            Handle for updated object of None if object doesn't exist
        """
        return self.images.upsert_object_property(identifier, properties)

    def image_groups_delete(self, identifier):
        """Delete image group object with given identifier. At the moment, this
        has no impact on objects referencing the image group.

        Parameters
        ----------
        identifier : string
            Unique image group identifier

        Returns
        -------
        ImageGroupHandle
            Handle for deleted image group or None if identifier is unknown
        """
        return self.image_groups.delete_object(identifier)

    def image_groups_download(self, identifier):
        """Get data file for image group with given identifier.

        Parameters
        ----------
        identifier : string
            Unique image group identifier

        Returns
        -------
        FileInfo
            Information about image group archive file on disk or None if
            identifier is unknown
        """
        # Retrieve image group to ensure that it exist
        img_grp = self.image_groups_get(identifier)
        if img_grp is None:
            # Return None if image group is unknown
            return None
        else:
            # Reference and information for file image group was created from
            return FileInfo(
                img_grp.data_file,
                img_grp.properties[datastore.PROPERTY_MIMETYPE],
                img_grp.properties[datastore.PROPERTY_FILENAME]
            )

    def image_groups_get(self, identifier):
        """Get image group with given identifier.

        Parameters
        ----------
        identifier : string
            Unique image group object identifier

        Returns
        -------
        ImageGroupHandle
            Handle for image group object or None if identifier is unknown
        """
        return self.image_groups.get_object(identifier)

    def image_group_images_list(self, identifier, limit=-1, offset=-1):
        """List images in the given image group.

        Parameters
        ----------
        identifier : string
            Unique image group object identifier
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
            Listing of group images
        """
        return self.image_groups.list_images(
            identifier,
            limit=limit,
            offset=offset
        )

    def image_groups_list(self, limit=-1, offset=-1):
        """Retrieve list of all image groups in the data store.

        Parameters
        ----------
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
            Listing of image group handles
        """
        return self.image_groups.list_objects(limit=limit, offset=offset)

    def image_groups_update_options(self, identifier, options):
        """Update set of typed options associated with a given image group.

        Raises ValueError if invalid options are provided.

        Parameters
        ----------
        identifier : string
            Unique image group identifier
        options : List(attribute.Attribute)
            List of attribute instances

        Returns
        -------
        ImageGroupHandle
            Handle for updated image group or None if identifier is unknown.
        """
        return self.image_groups.update_object_options(identifier, options)

    def image_groups_upsert_property(self, identifier, properties):
        """Upsert property of given image group.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        identifier : string
            Unique image group object identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        ImageGroupHandle
            Handle for updated object of None if object doesn't exist
        """
        return self.image_groups.upsert_object_property(identifier, properties)

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
        SubjectHandle
            Handle for created subject in database
        """
        # Ensure that the file name has a valid archive suffix
        if get_filename_suffix(filename, ARCHIVE_SUFFIXES) is None:
            raise ValueError('invalid file suffix: ' + os.path.basename(os.path.normpath(filename)))
        # Create subject from archive. Raises exception if file is not a valid
        # subject archive
        return self.subjects.upload_file(filename)

    def subjects_delete(self, identifier):
        """Delete subject with given identifier in the database. At the moment,
        this has no impact on other database objects that reference the subject.

        Parameters
        ----------
        identifier : string
            Unique subject identifier

        Returns
        -------
        SubjectHandle
            Handle for deleted subject or None if identifier is unknown
        """
        return self.subjects.delete_object(identifier)

    def subjects_download(self, identifier):
        """Get data file for subject with given identifier.

        Parameters
        ----------
        identifier : string
            Unique subject identifier

        Returns
        -------
        FileInfo
            Information about subject's data file on disk or None if identifier
            is unknown
        """
        # Retrieve subject to ensure that it exist
        subject = self.subjects_get(identifier)
        if subject is None:
            # Return None if subject is unknown
            return None
        else:
            # Reference and information for original uploaded file
            return FileInfo(
                subject.data_file,
                subject.properties[datastore.PROPERTY_MIMETYPE],
                subject.properties[datastore.PROPERTY_FILENAME]
            )

    def subjects_get(self, identifier):
        """Retrieve subject with given identifier.

        Parameters
        ----------
        identifier : string
            Unique subject identifier

        Returns
        -------
        SubjectHandle
            Subject data object handle or None if identifier is unknown.
        """
        return self.subjects.get_object(identifier)

    def subjects_list(self, limit=-1, offset=-1):
        """Retrieve list of all subjects in the data store.

        Parameters
        ----------
        limit : int
            Limit number of results in returned object listing
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
            Listing of subject handles
        """
        return self.subjects.list_objects(limit=limit, offset=offset)

    def subjects_upsert_property(self, identifier, properties):
        """Upsert property of given subject.

        Raises ValueError if given property dictionary results in an illegal
        operation.

        Parameters
        ----------
        identifier : string
            Unique subject identifier
        properties : Dictionary()
            Dictionary of property names and their new values.

        Returns
        -------
        SubjectHandle
            Handle for updated object of None if object doesn't exist
        """
        return self.subjects.upsert_object_property(identifier, properties)

# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

def create_dir(directory):
    """Create given directory, if doesn't exist.

    Parameters
    ----------
    directory : string
        Directory path (can be relative or absolute)

    Returns
    -------
    string
        Absolute directory path
    """
    if not os.access(directory, os.F_OK):
        os.makedirs(directory)
    return os.path.abspath(directory)


def get_filename_suffix(filename, suffixes):
    """Ensure that the given file has a suffix that is in the given set of
    allowed suffixes. Returns the matched suffix or None.

    Parameters
    ----------
    filename : string
        Name of file
    suffixes : Iterable(string)

    Returns
    -------
    string
        Matched suffix from list of suffixes or None.
    """
    for s in suffixes:
        if filename.endswith(s):
            return s
