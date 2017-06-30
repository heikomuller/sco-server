import json
import os
import yaml
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

from pymongo import MongoClient
from scodata.mongo import MongoDBFactory
from scoserv.widget import WidgetHandle, WidgetInput, WidgetRegistry

PROPERTIES = {'name' : 'My Widget', 'title' : 'My title'}
ENGINE = 'ENGINE'
CODE = {'run' : 'Hello World'}
INPUTS = [
    WidgetInput('M1', 'A1'),
    WidgetInput('M1', 'A2'),
    WidgetInput('M2', 'A1')
]

class TestWidgets(unittest.TestCase):

    def setUp(self):
        """Initialize the MongoDB database."""
        MongoClient().drop_database('test_sco')
        self.db = WidgetRegistry(MongoDBFactory(db_name='test_sco'))

    def tearDown(self):
        """Delete data store directory and database."""
        MongoClient().drop_database('test_sco')

    def test_append_input_for_widget(self):
        """Test appending input descriptors to widgets."""
        widget = self.db.create_widget(PROPERTIES, ENGINE, CODE, INPUTS)
        w = self.db.append_input_for_widget(widget.identifier, WidgetInput('M3', 'A1'))
        self.assertEquals(len(w.inputs), 4)
        w = self.db.get_widget(widget.identifier)
        self.assertEquals(len(w.inputs), 4)
        # Appending an existing input should not change anything
        w = self.db.append_input_for_widget(widget.identifier, WidgetInput('M3', 'A1'))
        self.assertEquals(len(w.inputs), 4)
        # Appending to a non existing widget should return None
        self.assertIsNone(self.db.append_input_for_widget('SOMEID', WidgetInput('M3', 'A1')))

    def test_create_widget(self):
        """Test creation and retrieval of widgets."""
        widget = self.db.create_widget(PROPERTIES, ENGINE, CODE, INPUTS)
        self.assertEqual(widget.engine_id, ENGINE)
        self.assertTrue('run' in widget.code)
        self.assertEqual(widget.code['run'], 'Hello World')
        self.assertEqual(len(widget.inputs), 3)
        w = self.db.get_widget(widget.identifier)
        self.assertEqual(w.engine_id, widget.engine_id)
        self.assertTrue('run' in w.code)
        self.assertEqual(w.code['run'], 'Hello World')
        self.assertEqual(len(w.inputs), 3)

    def test_delete_widget(self):
        """Test creation, lsting, and deletion of widget."""
        w1 = self.db.create_widget(PROPERTIES, ENGINE, CODE, INPUTS)
        w2 = self.db.create_widget(PROPERTIES, ENGINE, CODE, INPUTS)
        self.assertNotEqual(w1.identifier, w2.identifier)
        ids = [w1.identifier, w2.identifier]
        listing = self.db.list_widgets().items
        self.assertEqual(len(listing), 2)
        for item in listing:
            self.assertTrue(item.identifier in ids)
        w_del = self.db.delete_widget(w1.identifier)
        self.assertEqual(w_del.identifier, w1.identifier)
        listing = self.db.list_widgets().items
        self.assertEqual(len(listing), 1)
        self.assertEqual(w2.identifier, listing[0].identifier)
        w_del = self.db.delete_widget(w2.identifier)
        self.assertEqual(w_del.identifier, w2.identifier)
        self.assertEqual(len(self.db.list_widgets().items), 0)
        self.assertIsNone(self.db.delete_widget(w2.identifier))

    def test_find_widget(self):
        """Test querying of widgets for a given model."""
        w1 = self.db.create_widget(PROPERTIES, ENGINE, CODE, INPUTS)
        w2 = self.db.create_widget(PROPERTIES, ENGINE, CODE, INPUTS)
        w3 = self.db.create_widget(
            PROPERTIES,
            ENGINE,
            CODE,
            [WidgetInput('M2', 'A1'), WidgetInput('M3', 'A2')]
        )
        widgets = self.db.find_widgets_for_model('M1')
        self.assertEquals(len(widgets), 2)
        self.assertTrue('A1' in widgets)
        self.assertEqual(len(widgets['A1']), 2)
        self.assertTrue('A2' in widgets)
        self.assertEqual(len(widgets['A2']), 2)
        widgets = self.db.find_widgets_for_model('M3')
        self.assertEquals(len(widgets), 1)
        self.assertEquals(widgets['A2'][0].identifier, w3.identifier)

    def test_update_widget(self):
        """Test update widgets function."""
        widget = self.db.create_widget(PROPERTIES, ENGINE, CODE, INPUTS)
        w = self.db.update_widget(widget.identifier, code={'run': 'Me'})
        self.assertEquals(w.identifier, widget.identifier)
        self.assertEquals(w.code['run'], 'Me')
        w = self.db.update_widget(widget.identifier, inputs=[WidgetInput('M3', 'A1')])
        w = self.db.get_widget(widget.identifier)
        self.assertEquals(w.code['run'], 'Me')
        self.assertEquals(len(w.inputs), 1)
        # Ensure that the update is reflected in search queries
        self.assertEquals(len(self.db.find_widgets_for_model('M1')), 0)
        self.assertEquals(len(self.db.find_widgets_for_model('M3')), 1)
        # Updating with no parameters should not change anything
        w = self.db.update_widget(w.identifier)
        self.assertEquals(w.code['run'], 'Me')
        self.assertEquals(len(w.inputs), 1)
        # Updating to a non existing widget should return None
        self.assertIsNone(self.db.update_widget('SOMEID'))

if __name__ == '__main__':
    unittest.main()
