import gzip
import neuropythy as neuro
import os
import shutil
import sys
import unittest

from pymongo import MongoClient

sys.path.insert(0, os.path.abspath('..'))

import scoserv.db.api as api
import scoserv.db.attribute as attributes
import scoserv.db.datastore as datastore
import scoserv.db.prediction as prediction

API_DIR = '/tmp/sco'

SUBJECT_FILE = './data/subjects/subject.tar.gz'
IMAGE_FILE = './data/images/collapse.gif'
NON_IMAGE_FILE = './data/images/no-image.txt'
IMAGE_GROUP_FILE = './data/images/images.tar.gz'
FMRI_FILE = './data/fmris/fake-fmri.tar.gz'

class TestSCODataStoreAPIMethods(unittest.TestCase):

    def setUp(self):
        """Connect to MongoDB and clear any existing collections. Ensure
        that data directory exists and is empty. Then create API."""
        db = MongoClient().scotest
        db.experiments.drop()
        db.funcdata.drop()
        db.images.drop()
        db.imagegroups.drop()
        db.predictions.drop()
        db.subjects.drop()
        if os.path.isdir(API_DIR):
            shutil.rmtree(API_DIR)
        os.makedirs(API_DIR)
        self.api = api.SCODataStore(db, API_DIR)

    def test_experiment_api(self):
        # Create subject and image group
        subject = self.api.subjects_create(SUBJECT_FILE)
        img_grp = self.api.images_create(IMAGE_GROUP_FILE)
        #
        # Create experiment
        #
        experiment = self.api.experiments_create('Name', subject.identifier, img_grp.identifier)
        # Ensure it is of expected type
        self.assertTrue(experiment.is_experiment)
        # Ensure that creating experiment with missing subject or image group
        # raises an Exception
        with self.assertRaises(ValueError):
            self.api.experiments_create('Name', 'not-a-valid-idneitifer', img_grp.identifier)
        with self.assertRaises(ValueError):
            self.api.experiments_create('Name', subject.identifier, 'not-a-valid-idneitifer')
        #
        # Get
        #
        experiment = self.api.experiments_get(experiment.identifier)
        # Ensure it is of expected type
        self.assertTrue(experiment.is_experiment)
        # Ensure that get experiment with invalid identifier is None
        self.assertIsNone(self.api.experiments_get('not-a-valid-identifier'))
        #
        # fMRI
        #
        self.assertIsNone(self.api.experiments_fmri_get(experiment.identifier))
        #
        # List
        #
        self.assertEqual(self.api.experiments_list().total_count, 1)
        #
        # Upsert property
        #
        self.assertEqual(
            self.api.experiments_upsert_property(
                experiment.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_UPDATED
        )
        #
        # Delete
        #
        self.assertIsNotNone(self.api.experiments_delete(experiment.identifier))
        # Ensure that the list of experiments contains no elements
        self.assertEqual(self.api.experiments_list().total_count, 0)
        # Ensure that deleting a deleted experiment returns None
        self.assertIsNone(self.api.experiments_delete(experiment.identifier))
        # Updating the name of deleted experiment should return OP_NOT_EXISTS
        self.assertEqual(
            self.api.experiments_upsert_property(
                experiment.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_NOT_EXISTS
        )

    def test_experiment_fmri_api(self):
        # Create subject and image group and experiment
        subject = self.api.subjects_create(SUBJECT_FILE)
        img_grp = self.api.images_create(IMAGE_GROUP_FILE)
        experiment = self.api.experiments_create('Name', subject.identifier, img_grp.identifier)
        #
        # Create experiment fMRI object
        #
        fmri = self.api.experiments_fmri_create(experiment.identifier, FMRI_FILE)
        # Ensure that object is of expected type
        self.assertTrue(fmri.is_functional_data)
        # Ensure that creating fMRI for unknown experiment returns None
        self.assertIsNone(self.api.experiments_fmri_create('not-a-valid-identifier', FMRI_FILE))
        #
        # Get
        #
        fmri = self.api.experiments_fmri_get(experiment.identifier)
        # Ensure that object is of expected type
        self.assertTrue(fmri.is_functional_data)
        #
        # Download
        #
        self.assertTrue(os.path.isfile(self.api.experiments_fmri_download(experiment.identifier).file))
        #
        # Upsert property
        #
        self.assertEqual(
            self.api.experiments_fmri_upsert_property(
                experiment.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_UPDATED
        )
        #
        # Delete
        #
        self.assertIsNotNone(self.api.experiments_fmri_delete(experiment.identifier))
        # Ensure that the fMRI for experiment is None
        self.assertIsNone(self.api.experiments_fmri_get(experiment.identifier))
        # Ensure that deleting a deleted experiment returns None
        self.assertIsNone(self.api.experiments_fmri_delete(experiment.identifier))
        # Updating the name of deleted experiment should return OP_NOT_EXISTS
        self.assertEqual(
            self.api.experiments_fmri_upsert_property(
                experiment.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_NOT_EXISTS
        )
        # File info should be None
        self.assertIsNone(self.api.experiments_fmri_download(experiment.identifier))


    def test_experiment_prediction_api(self):
        # Create subject and image group and experiment
        subject = self.api.subjects_create(SUBJECT_FILE)
        img_grp = self.api.images_create(IMAGE_GROUP_FILE)
        experiment = self.api.experiments_create('Name', subject.identifier, img_grp.identifier)
        #
        # Create experiment prediction object
        #
        model_run = self.api.experiments_predictions_create(experiment.identifier, 'Name')
        # Ensure that object is of expected type
        self.assertTrue(model_run.is_model_run)
        # Ensure that creating fMRI for unknown experiment returns None
        self.assertIsNone(self.api.experiments_predictions_create('not-a-valid-identifier', 'Name'))
        # Create second experiment and prediction with arguments
        exp2 = self.api.experiments_create('Name', subject.identifier, img_grp.identifier)
        mr2 = self.api.experiments_predictions_create(
            exp2.identifier,
            'Name',
            [
                attributes.Attribute('gabor_orientations', 10),
                attributes.Attribute('max_eccentricity', 11),
                attributes.Attribute('normalized_pixels_per_degree', 0)
            ]
        )
        #
        # Get
        #
        model_run = self.api.experiments_predictions_get(experiment.identifier, model_run.identifier)
        # Ensure object is of expected type
        self.assertTrue(model_run.is_model_run)
        # Ensure invalud experiment and prediction combination is None
        self.assertIsNone(self.api.experiments_predictions_get(experiment.identifier, mr2.identifier))
        self.assertIsNone(self.api.experiments_predictions_get(exp2.identifier, model_run.identifier))
        self.assertIsNone(self.api.experiments_predictions_get('not-a-valid-identifier', mr2.identifier))
        self.assertIsNone(self.api.experiments_predictions_get(experiment.identifier, 'not-a-valid-identifier'))
        #
        # Download
        #
        self.assertIsNone(self.api.experiments_predictions_download(experiment.identifier, model_run.identifier))
        #
        # List
        #
        self.assertEqual(self.api.experiments_predictions_list(experiment.identifier).total_count, 1)
        #
        # Upsert properties
        #
        self.assertEqual(
            self.api.experiments_predictions_upsert_property(
                experiment.identifier,
                model_run.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_UPDATED
        )
        #
        # State
        #
        self.assertTrue(model_run.state.is_idle)
        model_run = self.api.experiments_predictions_update_state(
            experiment.identifier,
            model_run.identifier,
            prediction.ModelRunActive()
        )
        # Ensure that state change has happened and is persistent
        self.assertTrue(model_run.state.is_running)
        model_run = self.api.experiments_predictions_get(experiment.identifier, model_run.identifier)
        self.assertTrue(model_run.state.is_running)

    def test_image_files_api(self):
        """Test all image file related methods of API."""
        #
        # Create image object file
        #
        img = self.api.images_create(IMAGE_FILE)
        # Ensure that the created object is an image file
        self.assertTrue(img.is_image)
        # Ensure that creating image with invalid suffix raises Exception
        with self.assertRaises(ValueError):
            self.api.images_create(NON_IMAGE_FILE)
        #
        # Get image and ensure that it is still of expected type
        #
        img = self.api.image_files_get(img.identifier)
        self.assertTrue(img.is_image)
        # Ensure that getting an image with unknown identifier is None
        self.assertIsNone(self.api.image_files_get('not-a-valid-identifier'))
        #
        # Ensure that the list of images contains one element
        #
        self.assertEqual(self.api.image_files_list().total_count, 1)
        #
        # Ensure that the download file exists
        #
        self.assertTrue(os.path.isfile(self.api.image_files_download(img.identifier).file))
        # The download for a non-existing image should be None
        self.assertIsNone(self.api.image_files_download('not-a-valid-identifier'))
        #
        # Updating the image name should return OP_UPDATED
        #
        self.assertEqual(
            self.api.image_files_upsert_property(
                img.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_UPDATED
        )
        # Updating the file name should return OP_ILLEGAL
        self.assertEqual(
            self.api.image_files_upsert_property(
                img.identifier,
                datastore.PROPERTY_FILENAME,
                'Some.Name'
            ),
            datastore.OP_ILLEGAL
        )
        #
        # Assert that delete returns not None
        #
        self.assertIsNotNone(self.api.image_files_delete(img.identifier))
        # Ensure that the list of images contains no elements
        self.assertEqual(self.api.image_files_list().total_count, 0)
        # Ensure that deleting a deleted image returns None
        self.assertIsNone(self.api.image_files_delete(img.identifier))
        # Updating the name of deleted image should return OP_NOT_EXISTS
        self.assertEqual(
            self.api.image_files_upsert_property(
                img.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_NOT_EXISTS
        )

    def test_image_groups_api(self):
        """Test all image group related methods of API."""
        # Create image group object from file
        img_grp = self.api.images_create(IMAGE_GROUP_FILE)
        # Ensure that the created object is an image group
        self.assertTrue(img_grp.is_image_group)
        # Get image group and ensure that it is still of expected type
        img_grp = self.api.image_groups_get(img_grp.identifier)
        self.assertTrue(img_grp.is_image_group)
        # Ensure that getting an image group with unknown identifier is None
        self.assertIsNone(self.api.image_groups_get('not-a-valid-identifier'))
        # Ensure that the list of image groups contains one element
        self.assertEqual(self.api.image_groups_list().total_count, 1)
        # Ensure that the download file exists
        self.assertTrue(os.path.isfile(self.api.image_groups_download(img_grp.identifier).file))
        # The download for a non-existing image should be None
        self.assertIsNone(self.api.image_groups_download('not-a-valid-identifier'))
        # Updating the image group name should return OP_UPDATED
        self.assertEqual(
            self.api.image_groups_upsert_property(
                img_grp.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_UPDATED
        )
        # Updating the file name should return OP_ILLEGAL
        self.assertEqual(
            self.api.image_groups_upsert_property(
                img_grp.identifier,
                datastore.PROPERTY_FILENAME,
                'Some.Name'
            ),
            datastore.OP_ILLEGAL
        )
        # Ensure that updating options does not raise exception
        self.assertIsNotNone(self.api.image_groups_update_options(
            img_grp.identifier,
            [
                attributes.Attribute('stimulus_edge_value', 0.8),
                attributes.Attribute('stimulus_aperture_edge_value', 0.75)
            ]
        ))
        # Ensure that exception is raised if unknown attribute name is given
        with self.assertRaises(ValueError):
            self.api.image_groups_update_options(
                img_grp.identifier,
                [
                    attributes.Attribute('not_a_defined_attribute', 0.8),
                    attributes.Attribute('stimulus_edge_value', 0.75)
                ]
            )
        self.assertIsNotNone(self.api.image_groups_delete(img_grp.identifier))
        # Ensure that the list of image groups contains no elements
        self.assertEqual(self.api.image_groups_list().total_count, 0)
        # Ensure that deleting a deleted image group returns None
        self.assertIsNone(self.api.image_groups_delete(img_grp.identifier))
        # Updating the name of deleted image should return OP_NOT_EXISTS
        self.assertEqual(
            self.api.image_groups_upsert_property(
                img_grp.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_NOT_EXISTS
        )

    def test_subjects_api(self):
        """Test all subject related methods of API."""
        # Create temp subject file
        subject = self.api.subjects_create(SUBJECT_FILE)
        # Ensure that the created object is a subject
        self.assertTrue(subject.is_subject)
        # Get subject and ensure that it is still a subject
        subject = self.api.subjects_get(subject.identifier)
        self.assertTrue(subject.is_subject)
        # Ensure that getting a subject with unknown identifier is None
        self.assertIsNone(self.api.subjects_get('not-a-valid-identifier'))
        # Ensure that the list of subjects contains one element
        self.assertEqual(self.api.subjects_list().total_count, 1)
        # Ensure that the download file exists
        self.assertTrue(os.path.isfile(self.api.subjects_download(subject.identifier).file))
        # The download for a non-existing subject should be None
        self.assertIsNone(self.api.subjects_download('not-a-valid-identifier'))
        # Updating the subject name should return OP_UPDATED
        self.assertEqual(
            self.api.subjects_upsert_property(
                subject.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_UPDATED
        )
        # Updating the file name should return OP_ILLEGAL
        self.assertEqual(
            self.api.subjects_upsert_property(
                subject.identifier,
                datastore.PROPERTY_FILENAME,
                'Some.Name'
            ),
            datastore.OP_ILLEGAL
        )
        # Assert that delete returns not None
        self.assertIsNotNone(self.api.subjects_delete(subject.identifier))
        # Ensure that the list of subjects contains no elements
        self.assertEqual(self.api.subjects_list().total_count, 0)
        # Ensure that deleting a deleted subject returns None
        self.assertIsNone(self.api.subjects_delete(subject.identifier))
        # Updating the name of deleted subject should return OP_NOT_EXISTS
        self.assertEqual(
            self.api.subjects_upsert_property(
                subject.identifier,
                datastore.PROPERTY_NAME,
                'Some Name'
            ),
            datastore.OP_NOT_EXISTS
        )

if __name__ == '__main__':
    unittest.main()
