import gzip
import os
import shutil
import sys
import unittest

from pymongo import MongoClient

sys.path.insert(0, os.path.abspath('..'))

import scoserv.db.funcdata as funcdata

FMRIS_DIR = '/tmp/sco/funcdata'
DATA_DIR = './data/fmris'
FMRI_ARCHIVE = 'fake-fmri.tar.gz'

class TestFuncDataManagerMethods(unittest.TestCase):

    def setUp(self):
        """Connect to MongoDB and clear an existing funcdata collection. Ensure
        that data directory exists and is empty. Create functional data
        manager."""
        db = MongoClient().scotest
        db.fmris.drop()
        if os.path.isdir(FMRIS_DIR):
            shutil.rmtree(FMRIS_DIR)
        os.makedirs(FMRIS_DIR)
        self.mngr = funcdata.DefaultFunctionalDataManager(db.fmris, FMRIS_DIR)

    def test_funcdata_create(self):
        """Test creation of functional data objects from files."""
        # Create a functional data object from an archive file
        tmp_file = os.path.join(FMRIS_DIR, FMRI_ARCHIVE)
        shutil.copyfile(os.path.join(DATA_DIR, FMRI_ARCHIVE), tmp_file)
        fmri = self.mngr.create_object(tmp_file)
        # Assert that object is active and is_image property is true
        self.assertTrue(fmri.is_active)
        self.assertTrue(fmri.is_functional_data)
        # Ensure that other class type properties are false
        self.assertFalse(fmri.is_experiment)
        self.assertFalse(fmri.is_image_group)
        self.assertFalse(fmri.is_image)
        self.assertFalse(fmri.is_model_run)
        self.assertFalse(fmri.is_subject)
        # Assert that getting the object will not throw an Exception
        self.assertEqual(self.mngr.get_object(fmri.identifier).identifier, fmri.identifier)

if __name__ == '__main__':
    unittest.main()
