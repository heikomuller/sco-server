import gzip
import neuropythy as neuro
import os
import shutil
import sys
import unittest

from neuropythy.freesurfer import add_subject_path

sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('../scoserv'))

import scoserv.mongo as mongo
import scoserv.db.api as api
from scoserv.engine import run_sco


API_DIR = '/tmp/sco'
DATA_DIR = './data'


class TestSCOEngine(unittest.TestCase):

    def setUp(self):
        """Connect to MongoDB and clear any existing collections. Ensure
        that data directory exists and is empty. Then create API."""
        self.SUBJECT_PATH = os.path.join(DATA_DIR, 'env/subjects')
        self.IMAGES_ARCHIVE = os.path.join(DATA_DIR, 'images/images.tar.gz')
        self.SUBJECT_FILE = os.path.join(DATA_DIR, 'subjects/ernie.tar.gz')
        self.FAIL_SUBJECT_FILE = os.path.join(DATA_DIR, 'subjects/subject.tar.gz')
        self.mongo = mongo.MongoDBFactory(db_name='scotest')
        db = self.mongo.get_database()
        db.experiments.drop()
        db.funcdata.drop()
        db.images.drop()
        db.imagegroups.drop()
        db.predictions.drop()
        db.subjects.drop()
        if os.path.isdir(API_DIR):
            shutil.rmtree(API_DIR)
        os.makedirs(API_DIR)
        # Add Freesurfer subject path
        add_subject_path(self.SUBJECT_PATH)
        self.api = api.SCODataStore(self.mongo, API_DIR)

    def test_missing_image_group(self):
        """Test successful model run."""
        # Create subject, image group and experiment
        subject = self.api.subjects_create(self.SUBJECT_FILE)
        images = self.api.images_create(self.IMAGES_ARCHIVE)
        experiment = self.api.experiments_create(subject.identifier, images.identifier, {'name':'My Experiment'})
        # Create new model run
        model_run = self.api.experiments_predictions_create(experiment.identifier, 'My Run')
        # Delete Image Group
        self.api.image_groups_delete(images.identifier)
        # Run the model
        run_sco(self.mongo, API_DIR, model_run)
        # Ensure that model run is in FAILED state
        model_run = self.api.experiments_predictions_get(experiment.identifier, model_run.identifier)
        self.assertTrue(model_run.state.is_failed)

    def test_missing_subject(self):
        """Test successful model run."""
        # Create subject, image group and experiment
        subject = self.api.subjects_create(self.SUBJECT_FILE)
        images = self.api.images_create(self.IMAGES_ARCHIVE)
        experiment = self.api.experiments_create(subject.identifier, images.identifier, {'name':'My Experiment'})
        # Create new model run
        model_run = self.api.experiments_predictions_create(experiment.identifier, 'My Run')
        # Delete subject
        self.api.subjects_delete(subject.identifier)
        run_sco(self.mongo, API_DIR, model_run)
        # Ensure that model run is in FAILED state
        model_run = self.api.experiments_predictions_get(experiment.identifier, model_run.identifier)
        self.assertTrue(model_run.state.is_failed)

    def test_successful_model_run(self):
        """Test successful model run."""
        # Create subject, image group and experiment
        subject = self.api.subjects_create(self.SUBJECT_FILE)
        images = self.api.images_create(self.IMAGES_ARCHIVE)
        experiment = self.api.experiments_create(subject.identifier, images.identifier, {'name':'My Experiment'})
        # Create new model run
        model_run = self.api.experiments_predictions_create(experiment.identifier, 'My Run')
        run_sco(self.mongo, API_DIR, model_run)
        # Ensure that model run is in SUCCESS state
        model_run = self.api.experiments_predictions_get(experiment.identifier, model_run.identifier)
        self.assertTrue(model_run.state.is_success)
        # Ensure that exception is thrown if model run is not in idle state
        with self.assertRaises(ValueError):
            run_sco(self.mongo, API_DIR, model_run)

    def test_topology_mismatch(self):
        """Test successful model run."""
        # Create subject, image group and experiment
        subject = self.api.subjects_create(self.FAIL_SUBJECT_FILE)
        images = self.api.images_create(self.IMAGES_ARCHIVE)
        experiment = self.api.experiments_create(subject.identifier, images.identifier, {'name':'My Experiment'})
        # Create new model run
        model_run = self.api.experiments_predictions_create(experiment.identifier, 'My Run')
        run_sco(self.mongo, API_DIR, model_run)
        # Ensure that model run is in FAILED state
        model_run = self.api.experiments_predictions_get(experiment.identifier, model_run.identifier)
        self.assertTrue(model_run.state.is_failed)


if __name__ == '__main__':
    # Pass data directory as optional parameter
    if len(sys.argv) == 2:
        DATA_DIR = sys.argv[1]
    sys.argv = sys.argv[:1]
    unittest.main()
