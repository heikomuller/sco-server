"""Standard Cortical Observer - Workflow Engine API. The main inteface to run
predictive models for experiments that are defined in the SCO Data Store.
"""
from multiprocessing import Process
import os
import shutil
import tarfile
import tempfile

import db.api as api
import db.prediction as runs

from neuropythy.freesurfer import add_subject_path
import sco


# ------------------------------------------------------------------------------
#
# Engine
#
# ------------------------------------------------------------------------------

class SCOEngine(object):
    """Implementation of a workflow engine that allows to run prodictive models
    using resources that are managed by the SCO Data Store. This simple
    implementation currently provides a single method to run the default
    predictive model.
    """
    def __init__(self, mongo, db_dir, env_dir):
        """Initialize the engine with database factory and references to data
        directories. The engine executes models using multi-processing and
        therefore needs to instantiate a new SCO data store for each process.
        This is also the palce where we currently set the path to the Freesurfer
        subject directory.

        Parameters
        ----------
        mongo : mongo.MongoDBFactory()
            MongoDB database object factory
        db_dir : string
            The directory for storing data files. Directory will be created
            if it does not exist. For different types of objects various
            sub-directories will also be created if they don't exist.
        env_dir : string
            Absolute path to Freesurfer sugject directory
        """
        self.mongo = mongo
        self.db_dir = db_dir
        add_subject_path(env_dir)

    def run_model(self, model_run):
        """Execute the predictive model for resources defined by the given
        model run.

        Parameters
        ----------
        model_run : ModelRunHandle
            Handle to model run
        """
        # Start a new thread to execute the model run
        Process(target=run_sco, args=(self.mongo, self.db_dir, model_run)).start()


# ------------------------------------------------------------------------------
#
# Helper methods
#
# ------------------------------------------------------------------------------

def run_sco(mongo, db_dir, model_run):
    """Execute the default SCO predictive model for a given experiment.

    Raises ValueError if run is not in idle state.

    Parameters
    ----------
    mongo : mongo.MongoDBFactory()
        MongoDB database object factory
    db_dir : string
        The directory for storing data files. Directory will be created
        if it does not exist. For different types of objects various
        sub-directories will also be created if they don't exist.
    model_run : ModelRunHandle
        Handle to model run
    """
    # Raise exception if run is not in idle state
    if not model_run.state.is_idle:
        raise ValueError('invalid run state')

    # Create instance of the SCO Data Store for the new process
    db = api.SCODataStore(mongo, db_dir)

    # Set run state to running
    db.experiments_predictions_update_state(
        model_run.experiment,
        model_run.identifier,
        runs.ModelRunActive()
    )

    try:
        # Get experiment. Raise exception if experiment does not exist.
        experiment = db.experiments_get(model_run.experiment)
        if experiment is None:
            raise ValueError('unknown experiment: ' + model_run.experiment)
        # Get associated subject. Raise exception if subject does not exist
        subject = db.subjects_get(experiment.subject)
        if subject is None:
            raise ValueError('unknown subject: ' + experiment.subject)
        # Get associated image group. Raise exception if image group does not exist
        image_group = db.image_groups_get(experiment.images)
        if image_group is None:
            raise ValueError('unknown image group: ' + experiment.images)
    except ValueError as ex:
        # In case of an exception set run state to failed and return
        db.experiments_predictions_update_state(
            model_run.experiment,
            model_run.identifier,
            runs.ModelRunFailed(errors=[str(ex)])
        )
        return

    # Compose run arguments
    # Get options
    opts = {}
    # Add image group options
    for attr in image_group.options:
        opts[attr] = image_group.options[attr].value
    # Add run options
    for attr in model_run.arguments:
        opts[attr] = model_run.arguments[attr].value
    # Get subject directory
    subject_dir = subject.data_directory
    # Create list of image files
    image_files = [img.filename for img in image_group.images]

    # Run model inside a generic try/except block to ensure that we catch all
    # exceptions and set run state to fail if necessary
    try:
        results = sco.calc_sco(
            opts,
            subject=subject_dir,
            stimulus_image_filenames=image_files
        )
        # Create tar file with results. The file will have an images listing called
        # images.txt and a predicted response file called prediction.mgz
        temp_dir = tempfile.mkdtemp()
        sco.export_predicted_response_volumes(results, export_path=temp_dir)
        # Overwrite the generated images file with folders and names of images
        # in image group
        with open(os.path.join(temp_dir, 'images.txt'), 'w') as f:
            for img in image_group.images:
                f.write(img.folder + img.name + '\n')
        # Create a tar file in the temp directory
        tar_file = os.path.join(temp_dir, 'results.tar.gz')
        with tarfile.open(tar_file, 'w:gz') as t:
            t.add(os.path.join(temp_dir, 'prediction.mgz'), arcname='prediction.mgz')
            t.add(os.path.join(temp_dir, 'images.txt'), arcname='images.txt')
        # Create functional data object from tar file
        funcdata = db.funcdata.create_object(tar_file)
        # Clean-up
        shutil.rmtree(temp_dir)
    except Exception as ex:
        # In case of an exception set run state to failed and return
        db.experiments_predictions_update_state(
            model_run.experiment,
            model_run.identifier,
            runs.ModelRunFailed(errors=[type(ex).__name__ + ': ' + str(ex)])
        )
        return

    # Update run state to success
    db.experiments_predictions_update_state(
        model_run.experiment,
        model_run.identifier,
        runs.ModelRunSuccess(funcdata.identifier)
    )
