import gzip
import os
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

import scoserv.mongo as mongo
import scoserv.db.attribute as attributes
import scoserv.db.datastore as datastore
import scoserv.db.prediction as predictions

class TestPredictionManagerMethods(unittest.TestCase):

    def setUp(self):
        """Connect to MongoDB and clear an existing modelruns collection.
        Create the model run manager"""
        m = mongo.MongoDBFactory(db_name='scotest')
        db = m.get_database()
        db.predictions.drop()
        self.mngr = predictions.DefaultModelRunManager(db.modelruns)

    def test_experiment_create(self):
        """Test creation of experiment objects."""
        # Create an model run from fake data
        model_run = self.mngr.create_object('NAME', 'experiment-id')
        # Assert that object is active and is_image property is true
        self.assertTrue(model_run.is_active)
        self.assertTrue(model_run.is_model_run)
        # Ensure that other class type properties are false
        self.assertFalse(model_run.is_functional_data)
        self.assertFalse(model_run.is_image_group)
        self.assertFalse(model_run.is_image)
        self.assertFalse(model_run.is_experiment)
        self.assertFalse(model_run.is_subject)
        # Ensure that run state is IDLE
        self.assertTrue(model_run.state.is_idle)
        self.assertEquals(model_run.properties[datastore.PROPERTY_STATE], str(predictions.ModelRunIdle()))
        # Get object and ansure that all properties and state are still correct
        model_run = self.mngr.get_object(model_run.identifier)
        self.assertEqual(model_run.name, 'NAME')
        self.assertEqual(model_run.experiment, 'experiment-id')
        self.assertTrue(model_run.state.is_idle)
        self.assertEquals(model_run.properties[datastore.PROPERTY_STATE], str(predictions.ModelRunIdle()))

    def test_experiment_arguments(self):
        """Test creation of model runs without default parameters."""
        # Test default argument values
        model_run = self.mngr.create_object('NAME', 'experiment-id')
        self.assertEqual(model_run.arguments['gabor_orientations'].value, 8)
        self.assertEqual(model_run.arguments['max_eccentricity'].value, 12)
        self.assertFalse('normalized_pixels_per_degree' in model_run.arguments)
        # Create an model run from fake data with valid arguments
        model_run = self.mngr.create_object(
            'NAME',
            'experiment-id',
            [
                attributes.Attribute('gabor_orientations', 10),
                attributes.Attribute('max_eccentricity', 11),
                attributes.Attribute('normalized_pixels_per_degree', 0)
            ])
        # Ensure that default values are overriden
        self.assertEqual(model_run.arguments['gabor_orientations'].value, 10)
        self.assertEqual(model_run.arguments['max_eccentricity'].value, 11)
        # Create an model run with invalid arguments (duplicates)
        with self.assertRaises(ValueError):
            model_run = self.mngr.create_object(
                'NAME',
                'experiment-id',
                [
                    attributes.Attribute('max_eccentricity', 9),
                    attributes.Attribute('max_eccentricity', 11),
                    attributes.Attribute('normalized_pixels_per_degree', 0)
                ])
        # Create an model run with invalid arguments (unknown attribute)
        with self.assertRaises(ValueError):
            model_run = self.mngr.create_object(
                'NAME',
                'experiment-id',
                [
                    attributes.Attribute('not_a_known_attribute', 9),
                    attributes.Attribute('max_eccentricity', 11),
                    attributes.Attribute('normalized_pixels_per_degree', 0)
                ])
        # Create an model run with invalid arguments (invalid type)
        with self.assertRaises(ValueError):
            model_run = self.mngr.create_object(
                'NAME',
                'experiment-id',
                [
                    attributes.Attribute('gabor_orientations', 0.9),
                    attributes.Attribute('max_eccentricity', 11),
                    attributes.Attribute('normalized_pixels_per_degree', 0)
                ])

    def test_update_run_state(self):
        # Create an model run from fake data
        model_run = self.mngr.create_object('NAME', 'experiment-id')
        #
        # Set state to RUNNING
        #
        model_run = self.mngr.update_state(model_run.identifier, predictions.ModelRunActive())
        self.assertIsNotNone(model_run)
        self.assertTrue(model_run.state.is_running)
        self.assertEqual(model_run.properties[datastore.PROPERTY_STATE], str(predictions.ModelRunActive()))
        # Get object and ansure that state is still RUNNING
        model_run = self.mngr.get_object(model_run.identifier)
        self.assertTrue(model_run.state.is_running)
        self.assertEqual(model_run.properties[datastore.PROPERTY_STATE], str(predictions.ModelRunActive()))
        #
        # Set state to error
        #
        state = predictions.ModelRunFailed(['Some error'])
        model_run = self.mngr.update_state(model_run.identifier, state)
        self.assertIsNotNone(model_run)
        self.assertTrue(model_run.state.is_failed)
        self.assertEqual(model_run.properties[datastore.PROPERTY_STATE], str(state))
        self.assertTrue('Some error' in model_run.state.errors)
        # Get object and ansure that state is still FAILED
        model_run = self.mngr.get_object(model_run.identifier)
        self.assertTrue(model_run.state.is_failed)
        self.assertEqual(model_run.properties[datastore.PROPERTY_STATE], str(state))
        self.assertTrue('Some error' in model_run.state.errors)
        #
        # Set state to success. Will raise exception because run is incactive
        #
        state = predictions.ModelRunSuccess('preditcion-id')
        with self.assertRaises(ValueError):
            self.mngr.update_state(model_run.identifier, state)
        # Create an model run and ensure that we can set state to success for
        # active run
        model_run = self.mngr.create_object('NAME', 'experiment-id')
        model_run = self.mngr.update_state(model_run.identifier, predictions.ModelRunActive())
        state = predictions.ModelRunSuccess('preditcion-id')
        model_run = self.mngr.update_state(model_run.identifier, state)
        self.assertIsNotNone(model_run)
        self.assertTrue(model_run.state.is_success)
        self.assertEqual(model_run.properties[datastore.PROPERTY_STATE], str(state))
        self.assertEqual(model_run.state.model_output, 'preditcion-id')
        # Get object and ansure that state is still SUCCESS
        model_run = self.mngr.get_object(model_run.identifier)
        self.assertTrue(model_run.state.is_success)
        self.assertEqual(model_run.properties[datastore.PROPERTY_STATE], str(state))
        self.assertEqual(model_run.state.model_output, 'preditcion-id')

if __name__ == '__main__':
    unittest.main()
