"""Implements the run SCO model workflow for a given user request."""

import os
import shutil
import tarfile
import tempfile

import db.prediction as runs
from db.api import SCODataStore
from engine import REQUEST_EXPERIMENT_ID, REQUEST_RUN_ID
from mongo import MongoDBFactory
from server import DATA_DIR, ENV_DIR

from neuropythy.freesurfer import add_subject_path
import sco

# ------------------------------------------------------------------------------
#
# Run SCO predictive model
#
# ------------------------------------------------------------------------------

def sco_run(request):
    """Run SCO model for given request. Expects a Json object containing run
    and experiment identifier.

    Parameters
    ----------
    request : Json object
        Object containing run and experiment identifier
    """
    # Get identifier for run and experiment from request object
    experiment_id = request[REQUEST_EXPERIMENT_ID]
    run_id = request[REQUEST_RUN_ID]
    # Create instance of the SCO Data Store for the new process
    add_subject_path(ENV_DIR)
    db = SCODataStore(MongoDBFactory(), DATA_DIR)
    # Get model run handler from database. Raise exception if not in state IDLE
    # or RUNNING (the latter may have been caused by the worker failing before).
    try:
        model_run = db.experiments_predictions_get(experiment_id, run_id)
        if model_run is None:
            raise ValueError('unknown model run: ' +run_id + ':' + experiment_id)
        if not (model_run.state.is_idle or model_run.state.is_running):
            raise ValueError('invalid run state: ' + model_run.state)
    except ValueError as ex:
        # In case of an exception set run state to failed and return
        db.experiments_predictions_update_state(
            model_run.experiment,
            model_run.identifier,
            runs.ModelRunFailed(errors=[str(ex)])
        )
        return
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
    # Set run state to running
    db.experiments_predictions_update_state(
        model_run.experiment,
        model_run.identifier,
        runs.ModelRunActive()
    )
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
