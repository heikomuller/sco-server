import gzip
import os
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

import scoserv.mongo as mongo
import scoserv.db.experiment as experiments

class TestExperimentManagerMethods(unittest.TestCase):

    def setUp(self):
        """Connect to MongoDB and clear an existing experiment collection.
        Create experiment manager"""
        m = mongo.MongoDBFactory(db_name='scotest')
        db = m.get_database()
        db.experiments.drop()
        self.mngr = experiments.DefaultExperimentManager(db.experiments)

    def test_experiment_create(self):
        """Test creation of experiment objects."""
        # Create an experiment from fake data
        experiment = self.mngr.create_object('NAME', 'subject-id', 'images-id')
        # Assert that object is active and is_image property is true
        self.assertTrue(experiment.is_active)
        self.assertTrue(experiment.is_experiment)
        # Ensure that other class type properties are false
        self.assertFalse(experiment.is_functional_data)
        self.assertFalse(experiment.is_image_group)
        self.assertFalse(experiment.is_image)
        self.assertFalse(experiment.is_model_run)
        self.assertFalse(experiment.is_subject)
        # Assert that getting the object will not throw an Exception
        identifier = experiment.identifier
        experiment = self.mngr.get_object(identifier)
        self.assertEqual(identifier, experiment.identifier)
        # Ensure that subject and image objects are referenced properly
        self.assertEqual(experiment.subject, 'subject-id')
        self.assertEqual(experiment.images, 'images-id')
        self.assertIsNone(experiment.fmri_data)
        # Update fMRI data
        experiment = self.mngr.update_fmri_data(identifier, 'fmri-id')
        self.assertIsNotNone(experiment)
        self.assertEqual(experiment.fmri_data, 'fmri-id')
        experiment = self.mngr.get_object(experiment.identifier)
        # Ensure that subject and image objects are referenced properly
        self.assertEqual(experiment.subject, 'subject-id')
        self.assertEqual(experiment.images, 'images-id')
        self.assertEqual(experiment.fmri_data, 'fmri-id')

        # Create an experiment from fake data this time with functional data
        experiment = self.mngr.create_object('NAME', 'subject-id', 'images-id', 'fmri-id')
        experiment = self.mngr.get_object(experiment.identifier)
        # Ensure that subject and image objects are referenced properly
        self.assertEqual(experiment.subject, 'subject-id')
        self.assertEqual(experiment.images, 'images-id')
        self.assertEqual(experiment.fmri_data, 'fmri-id')


if __name__ == '__main__':
    unittest.main()
