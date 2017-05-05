import json
import os
import yaml
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

from pymongo import MongoClient
from scoserv.api import SCOServerAPI
from scoserv.init_model_repository import load_models

BASE_URL = 'localhost'

RESOURCES_DIR = './resources'

CONFIG_FILE = './data/config.yaml'
MODELS_FILE = './data/models.json'
FMRI_FILE = './data/fmris/fmri.nii'
IMAGES_FILE = './data/images/images.tar.gz'
SUBJECT_FILE = './data/subjects/subject.tar.gz'


class TestSCOAPI(unittest.TestCase):

    def setUp(self):
        """Initialize the Json serializer."""
        with open(CONFIG_FILE, 'r') as f:
            obj = yaml.load(f.read())
        config = {item['key']:item['value'] for item in obj['properties']}
        mongo = MongoClient()
        mongo.drop_database(config['mongo.db'])
        if os.path.isdir(RESOURCES_DIR):
            shutil.rmtree(RESOURCES_DIR)
        os.makedirs(RESOURCES_DIR)
        with open(MODELS_FILE, 'r') as f:
            models = json.load(f)
        load_models(models, mongo[config['mongo.db']].models)
        self.api = SCOServerAPI(config, BASE_URL)

    def test_experiment_serialization(self):
        """Test creation and serialization of experiments."""
        self.api.subjects_create(SUBJECT_FILE)
        subject_id = self.api.subjects_list()['items'][0]['id']
        self.api.images_create(IMAGES_FILE)
        image_group_id = self.api.image_groups_list()['items'][0]['id']
        response = self.api.experiments_create(subject_id, image_group_id, {'name':'Test'})
        self.verify_response(response)
        # Get experiments listing. Expect one entry
        listing = self.api.experiments_list()
        self.verify_object_listing(listing, 1)
        # Get the experiment
        experiment_id = listing['items'][0]['id']
        experiment = self.api.experiments_get(experiment_id)
        self.verify_object_handle(experiment, additional_elements=['images', 'subject'])
        # Attach fMRI data with experiment
        response = self.api.experiments_fmri_create(experiment_id, FMRI_FILE)
        self.verify_response(response)
        # Verify that the experiment now has fMRI information
        experiment = self.api.experiments_get(experiment_id)
        self.verify_object_handle(experiment, additional_elements=['images', 'subject', 'fmri'])

    def test_image_group_serialization(self):
        """Test creation and serialization for image groups."""
        response = self.api.images_create(IMAGES_FILE)
        self.verify_response(response)
        # Get image files listing. Expect four entries
        listing = self.api.image_files_list()
        self.verify_object_listing(listing, 4)
        for item in listing['items']:
            self.verify_listing_item(item)
        # Get image groups listing
        listing = self.api.image_groups_list()
        self.verify_object_listing(listing, 1)
        for item in listing['items']:
            self.verify_listing_item(item)
        # Get image group
        image_group = self.api.image_groups_get(listing['items'][0]['id'])
        self.verify_object_handle(image_group, additional_elements=['images', 'options'])

    def test_models_serialization(self):
        """Test serialization for model definitions."""
        # Get models listing. Expect two entries
        listing = self.api.models_list()
        self.verify_object_listing(listing, 2)
        for item in listing['items']:
            self.assertEqual(len(item), 4)
            self.assertTrue('id' in item)
            self.assertTrue('description' in item)
            self.assertTrue('name' in item)
            self.assertTrue('links' in item)
            model = self.api.models_get(item['id'])
            self.assertEqual(len(model), 6)
            self.assertTrue('id' in model)
            self.assertTrue('description' in model)
            self.assertTrue('name' in model)
            self.assertTrue('parameters' in model)
            self.assertTrue('outputs' in model)
            self.assertTrue('links' in model)

    def test_service_description(self):
        """Test serialization of service description."""
        desc = self.api.service_description()
        self.assertEqual(len(desc), 4)
        self.assertTrue('name' in desc)
        self.assertTrue('title' in desc)
        self.assertTrue('description' in desc)
        self.assertTrue('links' in desc)

    def test_subject_serialization(self):
        #"""Test creation and serialization of subjects."""
        response = self.api.subjects_create(SUBJECT_FILE)
        self.verify_response(response)
        # Get subjects listing
        listing = self.api.subjects_list()
        self.verify_object_listing(listing, 1)
        # Get subject
        subject_item = listing['items'][0]
        self.verify_listing_item(subject_item)
        subject = self.api.subjects_get(subject_item['id'])
        self.verify_object_handle(subject)

    def verify_listing_item(self, item):
        """Verify that an item in a object listing has all relevant elements"""
        self.assertEqual(len(item), 4)
        self.assertTrue('id' in item)
        self.assertTrue('timestamp' in item)
        self.assertTrue('name' in item)
        self.assertTrue('links' in item)

    def verify_object_handle(self, obj, additional_elements=[]):
        """Verify that a given object handle has the expected default and
        additional elements."""
        self.assertEqual(len(obj), 5 + len(additional_elements))
        self.assertTrue('id' in obj)
        self.assertTrue('timestamp' in obj)
        self.assertTrue('name' in obj)
        self.assertTrue('properties' in obj)
        self.assertTrue('links' in obj)
        for key in additional_elements:
            self.assertTrue(key in obj)

    def verify_object_listing(self, listing, expected_count):
        """Verify that a given object listing has all the expected elements."""
        self.assertEqual(len(listing), 6)
        self.assertTrue('offset' in listing)
        self.assertTrue('limit' in listing)
        self.assertTrue('items' in listing)
        self.assertTrue('count' in listing)
        self.assertTrue('totalCount' in listing)
        self.assertTrue('links' in listing)
        self.assertEqual(listing['count'], expected_count)
        self.assertEqual(listing['totalCount'], expected_count)

    def verify_response(self, response):
        """Verity that a response object has all the expected elements."""
        self.assertEqual(len(response), 2)
        self.assertTrue('result' in response)
        self.assertTrue('links' in response)


if __name__ == '__main__':
    unittest.main()
