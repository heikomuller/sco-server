import gzip
import neuropythy as neuro
import os
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

import scoserv.mongo as mongo
import scoserv.db.subject as subjects

SUBJECT_DIR = '/tmp/sco/subjects'
SUBJECT_FILE = './data/subjects/subject.tar.gz'
FALSE_SUBJECT_FILE = './data/subjects/false-subject.tar.gz'

class TestSubjectManagerMethods(unittest.TestCase):

    def setUp(self):
        """Connect to MongoDB and clear an existing subjects collection. Ensure
        that data directory exists and is empty. Then create subject manager."""
        m = mongo.MongoDBFactory(db_name='scotest')
        db = m.get_database()
        db.subjects.drop()
        if os.path.isdir(SUBJECT_DIR):
            shutil.rmtree(SUBJECT_DIR)
        os.makedirs(SUBJECT_DIR)
        self.mngr = subjects.DefaultSubjectManager(db.subjects, SUBJECT_DIR)

    def test_subjects_upload(self):
        """Test creation of subject objects in the database through file
        upload."""
        # Create temp subject file
        subject = self.mngr.upload_file(SUBJECT_FILE)
        # Ensure that the created object is active
        self.assertTrue(subject.is_active)
        # Ensure that is_subject property s True
        self.assertTrue(subject.is_subject)
        # Ensure that other class type properties are false
        self.assertFalse(subject.is_experiment)
        self.assertFalse(subject.is_image)
        self.assertFalse(subject.is_functional_data)
        self.assertFalse(subject.is_model_run)
        self.assertFalse(subject.is_image_group)
        # Ensure that a file with the same name as the upload file exists in the
        # upload directory
        upload_file = os.path.join(SUBJECT_DIR, subject.identifier)
        upload_file = os.path.join(upload_file, subjects.UPLOAD_DIRECTORY)
        # Ensure that the subjects uplaod directory is equal to upload_file
        # directory
        self.assertEquals(subject.upload_directory, upload_file)
        upload_file = os.path.join(upload_file, os.path.basename(SUBJECT_FILE))
        self.assertTrue(os.path.isfile(upload_file))
        # Ensure that data directory exists and is a Freesurfer directory
        subject_dir = os.path.join(SUBJECT_DIR, subject.identifier)
        subject_dir = os.path.join(subject_dir, subjects.DATA_DIRECTORY)
        self.assertIsNotNone(neuro.freesurfer_subject(subject_dir))
        # Ensure that the subjects data directory equals subject_dir
        self.assertEqual(subject.data_directory, subject_dir)
        # Ensure that subjects can be created from uncompressed tar files
        # Create uncompressed copy of subject first
        tmp_file = os.path.join(SUBJECT_DIR, 's.tar')
        f_in = gzip.open(SUBJECT_FILE, 'rb')
        f_out = open(tmp_file, 'wb')
        f_out.write( f_in.read() )
        f_in.close()
        f_out.close()
        subject = self.mngr.upload_file(tmp_file)
        # Ensure that a file with the same name as the upload file exists in the
        # upload directory
        upload_file = os.path.join(SUBJECT_DIR, subject.identifier)
        upload_file = os.path.join(upload_file, 'upload')
        upload_file = os.path.join(upload_file, 's.tar')
        self.assertTrue(os.path.isfile(upload_file))
        # Ensure that data directory exists and is a Freesurfer directory
        subject_dir = os.path.join(SUBJECT_DIR, subject.identifier)
        subject_dir = os.path.join(subject_dir, 'data')
        # Ensure that fake file upload raises exception
        with self.assertRaises(ValueError):
            self.mngr.upload_file(FALSE_SUBJECT_FILE)
        # Ensure that erase works as well
        self.assertIsNotNone(self.mngr.delete_object(subject.identifier, erase=True))

    def test_subjects_get_list_delete(self):
        # Create temp subject file
        subject = self.mngr.upload_file(SUBJECT_FILE)
        # Ensure that there is exactly one subject in the database with the
        # same id as the created subject
        listing = self.mngr.list_objects()
        self.assertEqual(listing.total_count, 1)
        self.assertEqual(len(listing.items), 1)
        self.assertEqual(listing.items[0].identifier, subject.identifier)
        # Create a second subject from the same file
        subject = self.mngr.upload_file(SUBJECT_FILE)
        # Ensure that the listing now has two elements
        listing = self.mngr.list_objects()
        self.assertEqual(listing.total_count, 2)
        self.assertEqual(len(listing.items), 2)
        # Ensure that exists_object method returns True
        self.assertTrue(self.mngr.exists_object(subject.identifier))
        # Ensure that identifier and directory are correct when subject is
        # retrieved from database
        s = self.mngr.get_object(subject.identifier)
        self.assertEqual(subject.identifier, s.identifier)
        self.assertEqual(subject.directory, s.directory)
        # Delete last object and ensure that listing has only one item left
        self.mngr.delete_object(subject.identifier)
        listing = self.mngr.list_objects()
        self.assertEqual(listing.total_count, 1)
        self.assertEqual(len(listing.items), 1)
        # Ensure that exists_object method returns True
        self.assertFalse(self.mngr.exists_object(subject.identifier))
        # Ensure that getting deleted object is None
        self.assertIsNone(self.mngr.get_object(subject.identifier))
        # Ensure that deleting deleted object is None
        self.assertIsNone(self.mngr.delete_object(subject.identifier))
        # Ensure that erase works as well
        subject = self.mngr.upload_file(SUBJECT_FILE)
        self.assertIsNotNone(self.mngr.delete_object(subject.identifier, erase=True))

if __name__ == '__main__':
    unittest.main()
