import json
import os
from pymongo import MongoClient
import yaml
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

from scodata.mongo import MongoDBFactory
from scoengine import init_registry_from_json
from scoserv.api import SCOServerAPI

BASE_URL = 'localhost'

CONFIG_FILE = './data/config.yaml'

PROPERTIES = {'name' : 'My Widget', 'title' : 'My title'}
ENGINE = 'ENGINE'
CODE = {'run' : 'Hello World'}
INPUTS = [
    {'model': 'M1', 'attachment': 'A1'},
    {'model': 'M1', 'attachment': 'A2'},
    {'model': 'M2', 'attachment': 'A1'}
]

class TestWidgetAPI(unittest.TestCase):

    def setUp(self):
        """Initialize the Json serializer."""
        with open(CONFIG_FILE, 'r') as f:
            obj = yaml.load(f.read())
        self.config = {item['key']:item['value'] for item in obj['properties']}
        MongoClient().drop_database(self.config['mongo.db'])
        self.api = SCOServerAPI(self.config, BASE_URL)

    def tearDown(self):
        """Drop the test database."""
        MongoClient().drop_database(self.config['mongo.db'])

    def test_add_input_descriptor(self):
        """Test deletion of widgets."""
        self.api.widgets_create(ENGINE, CODE, INPUTS, {'name' : 'Test'})
        identifier = self.api.widgets_list()['items'][0]['id']
        widget = self.api.widgets_get(identifier)
        self.assertEquals(len(widget['inputs']), 3)
        # Append input
        self.assertIsNotNone(
            self.api.widgets_add_input_descriptor(
                identifier,
                {'model': 'M3', 'attachment': 'A1'}
            )
        )
        widget = self.api.widgets_get(identifier)
        self.assertEquals(len(widget['inputs']), 4)
        # Append same descriptor doesn't change anything
        self.assertIsNotNone(
            self.api.widgets_add_input_descriptor(
                identifier,
                {'model': 'M3', 'attachment': 'A1'}
            )
        )
        # Update unknown widget
        self.assertIsNone(
            self.api.widgets_add_input_descriptor(
                'SOMEID',
                {'model': 'M3', 'attachment': 'A1'}
            )
        )

    def test_create_widget(self):
        """Test creation and retrieval of widgets."""
        # The initial list of widgets should be empty
        self.assertEqual(len(self.api.widgets_list()['items']), 0)
        # Creating a widget without a name property will fail
        with self.assertRaises(ValueError):
            self.api.widgets_create(ENGINE, CODE, INPUTS, {'title' : 'Test'})
        widget = self.api.widgets_create(ENGINE, CODE, INPUTS, {'name' : 'Test'})
        self.assertEquals(len(widget), 2)
        self.assertTrue('result' in widget)
        self.assertTrue('links' in widget)
        self.assertEquals(widget['result'], 'SUCCESS')

    def test_delete_widget(self):
        """Test deletion of widgets."""
        self.api.widgets_create(ENGINE, CODE, INPUTS, {'name' : 'Test'})
        identifier = self.api.widgets_list()['items'][0]['id']
        self.assertIsNotNone(self.api.widgets_get(identifier))
        self.assertIsNotNone(self.api.widgets_delete(identifier))
        self.assertIsNone(self.api.widgets_delete(identifier))

    def test_get_widget(self):
        """Test creation, retrieval and deletion of widgets."""
        self.api.widgets_create(ENGINE, CODE, INPUTS, {'name' : 'Test'})
        identifier = self.api.widgets_list()['items'][0]['id']
        widget = self.api.widgets_get(identifier)
        self.assertEqual(len(widget), 8)
        self.assertTrue('id' in widget)
        self.assertTrue('name' in widget)
        self.assertTrue('timestamp' in widget)
        self.assertTrue('engine' in widget)
        self.assertTrue('code' in widget)
        self.assertTrue('inputs' in widget)
        self.assertTrue('properties' in widget)
        self.assertTrue('links' in widget)
        self.assertEqual(widget['name'], 'Test')
        self.assertEqual(widget['engine'], ENGINE)

    def test_widget_update(self):
        """Test deletion of widgets."""
        self.api.widgets_create(ENGINE, CODE, INPUTS, {'name' : 'Test'})
        identifier = self.api.widgets_list()['items'][0]['id']
        # Update Code
        self.assertIsNotNone(self.api.widgets_update(identifier, code={'run':'Me'}))
        widget = self.api.widgets_get(identifier)
        self.assertEquals(widget['code']['run'], 'Me')
        self.assertEquals(len(widget['inputs']), 3)
        # Update inputs
        self.assertIsNotNone(self.api.widgets_update(identifier, inputs=[{'model': 'M1', 'attachment': 'A1'}]))
        widget = self.api.widgets_get(identifier)
        self.assertEquals(widget['code']['run'], 'Me')
        self.assertEquals(len(widget['inputs']), 1)
        # Empty Update
        self.assertIsNotNone(self.api.widgets_update(identifier))
        widget = self.api.widgets_get(identifier)
        self.assertEquals(widget['code']['run'], 'Me')
        self.assertEquals(len(widget['inputs']), 1)
        # Update unknown widget
        self.assertIsNone(self.api.widgets_update('SOMEID'))

    def test_widget_upsert_property(self):
        """Test deletion of widgets."""
        self.api.widgets_create(ENGINE, CODE, INPUTS, {'name' : 'Test'})
        identifier = self.api.widgets_list()['items'][0]['id']
        self.assertIsNotNone(self.api.widgets_upsert_property(identifier, {'name' : 'My Name'}))
        widget = self.api.widgets_get(identifier)
        self.assertEquals(widget['name'], 'My Name')


if __name__ == '__main__':
    unittest.main()
