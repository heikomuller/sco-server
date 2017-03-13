import os
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

import scoserv.hateoas as hateoas
import scoserv.serialize as serialize
import scoserv.utils as utils
import scodata.attribute as attr
import scodata.datastore as ds
import scodata.experiment as experiment
import scodata.funcdata as funcdata
import scodata.image as image
import scodata.prediction as runs
import scodata.subject as subject


BASE_URL = ':url:'
OBJECT_ID = 'oid'
OBJECT_DIR = 'odir'
OBJECT_NAME = 'oname'


class TestJsonSerializer(unittest.TestCase):

    def setUp(self):
        """Initialize the Json serializer."""
        self.serializer = serialize.JsonWebAPISerializer(BASE_URL + '///')

    def test_experiment_serialization(self):
        """Test serialization of experimets."""
        e = experiment.ExperimentHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            'SUBJECT_ID',
            'IMAGES_ID'
        )
        s = subject.SubjectHandle(
            OBJECT_ID,
            {
                ds.PROPERTY_NAME: OBJECT_NAME,
                ds.PROPERTY_FILETYPE: subject.FILE_TYPE_FREESURFER_DIRECTORY,
            },
            OBJECT_DIR
        )
        ig = image.ImageGroupHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            OBJECT_DIR,
            [
                image.GroupImage('id1', 'name1', '/', ''),
                image.GroupImage('id2', 'name2', '/', '')
            ],
            {'stimulus_pixels_per_degree': attr.Attribute('stimulus_pixels_per_degree', 0.88)}
        )
        json = self.serializer.experiment_to_json(e, s, ig)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('subject' in json)
        self.assertTrue('images' in json)
        self.assertEqual(len(json), 7)
        # Ensure basic values are correct
        self.assertEqual(json['id'], OBJECT_ID)
        self.assertEqual(json['name'], OBJECT_NAME)
        # Make sure properties are set correctly
        properties = utils.from_list(json['properties'])
        self.assertEqual(len(properties), 1)
        self.assertEqual(properties[ds.PROPERTY_NAME], OBJECT_NAME)
        # Ensure subject is represented properly
        self.assertTrue('id' in json['subject'])
        self.assertTrue('links' in json['subject'])
        self.assertEqual(len(json['subject']), 5)
        links = utils.from_list(json['subject']['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        # Ensure image group is represented properly
        self.assertTrue('id' in json['images'])
        self.assertTrue('links' in json['images'])
        self.assertEqual(len(json['images']), 7)
        links = utils.from_list(json['images']['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertTrue(hateoas.REF_KEY_PREDICTIONS_LIST in links)
        self.assertTrue(hateoas.REF_KEY_PREDICTIONS_RUN in links)
        self.assertTrue(hateoas.REF_KEY_FMRI_UPLOAD in links)
        self.assertEqual(len(links), 6)
        self_ref = '/'.join([BASE_URL, hateoas.URL_KEY_EXPERIMENTS, OBJECT_ID])
        self.assertEqual(links[hateoas.REF_KEY_SELF ], self_ref)

    def test_experiment_fmri_serialization(self):
        """Test serialization of experimets."""
        e = experiment.ExperimentHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            'SUBJECT_ID',
            'IMAGES_ID',
            fmri_data='FMRI_ID'
        )
        s = subject.SubjectHandle(
            OBJECT_ID,
            {
                ds.PROPERTY_NAME: OBJECT_NAME,
                ds.PROPERTY_FILETYPE: subject.FILE_TYPE_FREESURFER_DIRECTORY,
            },
            OBJECT_DIR
        )
        ig = image.ImageGroupHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            OBJECT_DIR,
            [
                image.GroupImage('id1', 'name1', '/', ''),
                image.GroupImage('id2', 'name2', '/', '')
            ],
            {'stimulus_pixels_per_degree': attr.Attribute('stimulus_pixels_per_degree', 0.88)}
        )
        fmri = funcdata.FMRIDataHandle(
            funcdata.FunctionalDataHandle(
                OBJECT_ID,
                {ds.PROPERTY_NAME: OBJECT_NAME},
                OBJECT_DIR
            ),
            'EXPERIMENT_ID'
        )
        json = self.serializer.experiment_to_json(e, s, ig, fmri=fmri)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('subject' in json)
        self.assertTrue('images' in json)
        self.assertTrue('fmri' in json)
        self.assertEqual(len(json), 8)
        # Ensure fMRI object is represented properly
        self.assertTrue('id' in json['fmri'])
        self.assertTrue('links' in json['fmri'])
        self.assertEqual(len(json['fmri']), 6)
        links = utils.from_list(json['fmri']['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertTrue(hateoas.REF_KEY_PREDICTIONS_LIST in links)
        self.assertTrue(hateoas.REF_KEY_PREDICTIONS_RUN in links)
        self.assertTrue(hateoas.REF_KEY_FMRI_UPLOAD in links)
        self.assertTrue(hateoas.REF_KEY_FMRI_GET in links)
        self.assertEqual(len(links), 7)
        # Test fMRI serialization
        fmri = funcdata.FMRIDataHandle(
            funcdata.FunctionalDataHandle(
                OBJECT_ID,
                {ds.PROPERTY_NAME: OBJECT_NAME},
                OBJECT_DIR
            ),
            'EXPERIMENT_ID'
        )
        json = self.serializer.experiment_fmri_to_json(fmri)
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('experiment' in json)
        self.assertEqual(len(json), 6)
        # Ensure experiment is represented properly
        self.assertTrue('id' in json['experiment'])
        self.assertTrue('links' in json['experiment'])
        self.assertEqual(len(json['experiment']), 2)
        # Ensure experiment self reference is given
        links = utils.from_list(json['experiment']['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        # Ensure basic values are correct
        self.assertEqual(json['id'], OBJECT_ID)
        self.assertEqual(json['name'], OBJECT_NAME)
        # Make sure properties are set correctly
        properties = utils.from_list(json['properties'])
        self.assertEqual(len(properties), 1)
        self.assertEqual(properties[ds.PROPERTY_NAME], OBJECT_NAME)
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_DOWNLOAD in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertEqual(len(links), 4)
        self_ref = '/'.join([BASE_URL, hateoas.URL_KEY_EXPERIMENTS, 'EXPERIMENT_ID', hateoas.URL_KEY_FMRI])
        self.assertEqual(links[hateoas.REF_KEY_SELF ], self_ref)

    def test_experiment_prediction_serialization(self):
        """Test serialization of image file objects."""
        e = experiment.ExperimentHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            'SUBJECT_ID',
            'IMAGES_ID'
        )
        mr = runs.ModelRunHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            runs.ModelRunIdle(),
            e.identifier,
            {'normalized_pixels_per_degree': attr.Attribute('normalized_pixels_per_degree', 0.88)}
        )
        json = self.serializer.experiment_prediction_to_json(mr, e)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('state' in json)
        self.assertTrue('arguments' in json)
        self.assertTrue('experiment' in json)
        self.assertTrue('schedule' in json)
        self.assertEqual(len(json), 9)
        # Ensure basic values are correct
        self.assertEqual(json['id'], OBJECT_ID)
        self.assertEqual(json['name'], OBJECT_NAME)
        self.assertEqual(json['state'], 'IDLE')
        # Make sure properties are set correctly
        properties = utils.from_list(json['properties'])
        self.assertEqual(len(properties), 1)
        self.assertEqual(properties[ds.PROPERTY_NAME], OBJECT_NAME)
        # Ensure options are represented properly
        self.assertEqual(len(json['arguments']), 1)
        arg = json['arguments'][0]
        self.assertEqual(arg['name'], 'normalized_pixels_per_degree')
        self.assertEqual(arg['value'], 0.88)
        # Ensure experiment is represented properly
        self.assertTrue('id' in json['experiment'])
        self.assertTrue('links' in json['experiment'])
        self.assertEqual(len(json['experiment']), 3)
        # Ensure experiment self reference is given
        links = utils.from_list(json['experiment']['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertEqual(len(links), 4)
        self_ref = '/'.join([BASE_URL, hateoas.URL_KEY_EXPERIMENTS, 'oid', hateoas.URL_KEY_PREDICTIONS, OBJECT_ID])
        self.assertEqual(links[hateoas.REF_KEY_SELF ], self_ref)
        # Check serialization for active run
        mr.state = runs.ModelRunActive()
        json = self.serializer.experiment_prediction_to_json(mr, e)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('state' in json)
        self.assertTrue('arguments' in json)
        self.assertTrue('experiment' in json)
        self.assertTrue('schedule' in json)
        self.assertEqual(len(json), 9)
        # Ensure basic values are correct
        self.assertEqual(json['state'], 'RUNNING')
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertEqual(len(links), 4)
        # Check serialization for error run
        mr.state = runs.ModelRunFailed(['E1', 'E2'])
        json = self.serializer.experiment_prediction_to_json(mr, e)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('state' in json)
        self.assertTrue('arguments' in json)
        self.assertTrue('errors' in json)
        self.assertTrue('experiment' in json)
        self.assertTrue('schedule' in json)
        self.assertEqual(len(json), 10)
        # Ensure error messages are there
        self.assertEqual(len(json['errors']), 2)
        # Ensure basic values are correct
        self.assertEqual(json['state'], 'FAILED')
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertEqual(len(links), 4)
        # Check serialization for error run
        mr.state = runs.ModelRunSuccess('OUTPUT')
        json = self.serializer.experiment_prediction_to_json(mr, e)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('state' in json)
        self.assertTrue('arguments' in json)
        self.assertTrue('experiment' in json)
        self.assertTrue('schedule' in json)
        self.assertEqual(len(json), 9)
        # Ensure basic values are correct
        self.assertEqual(json['state'], 'SUCCESS')
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_DOWNLOAD in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertEqual(len(links), 5)

    def test_group_images_listing_references(self):
        # Create set of 2 item
        items = [
            image.GroupImage(
                OBJECT_ID,
                OBJECT_NAME,
                OBJECT_DIR,
                ''
            ),
            image.GroupImage(
                OBJECT_ID,
                OBJECT_NAME,
                OBJECT_DIR,
                ''
            )
        ]
        object_listing = ds.ObjectListing(items, 7, 2, 8)
        # Get Json serialization of object listing
        json = self.serializer.image_group_images_to_json(object_listing, OBJECT_ID)
        # Ensure thatthere are two list items and that they have elements
        # id, name and folder
        self.assertEqual(len(json['items']), 2)
        for i in range(2):
            item = json['items'][i]
            self.assertTrue('id' in item)
            self.assertTrue('name' in item)
            self.assertTrue('folder' in item)
        # Make sure links are present and correct. Expect only two links because
        # limit is -1
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_FIRST in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_PREVIOUS in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_LAST in links)
        self.assertTrue(hateoas.REF_KEY_IMAGE_GROUP in links)
        self.assertEqual(len(links), 5)

    def test_image_file_serialization(self):
        """Test serialization of image file objects."""
        img = image.ImageHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            OBJECT_DIR
        )
        json = self.serializer.image_file_to_json(img)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertEqual(len(json), 5)
        # Ensure basic values are correct
        self.assertEqual(json['id'], OBJECT_ID)
        self.assertEqual(json['name'], OBJECT_NAME)
        # Make sure properties are set correctly
        properties = utils.from_list(json['properties'])
        self.assertEqual(len(properties), 1)
        self.assertEqual(properties[ds.PROPERTY_NAME], OBJECT_NAME)
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_DOWNLOAD in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertEqual(len(links), 4)
        self_ref = '/'.join([BASE_URL, hateoas.URL_KEY_IMAGES, hateoas.URL_KEY_IMAGE_FILES, OBJECT_ID])
        self.assertEqual(links[hateoas.REF_KEY_SELF ], self_ref)

    def test_image_group_serialization(self):
        """Test serialization of image group objects."""
        img_grp = image.ImageGroupHandle(
            OBJECT_ID,
            {ds.PROPERTY_NAME: OBJECT_NAME},
            OBJECT_DIR,
            [
                image.GroupImage('id1', 'name1', '/', ''),
                image.GroupImage('id2', 'name2', '/', '')
            ],
            {'stimulus_pixels_per_degree': attr.Attribute('stimulus_pixels_per_degree', 0.88)}
        )
        json = self.serializer.image_group_to_json(img_grp)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertTrue('images' in json)
        self.assertTrue('options' in json)
        self.assertEqual(len(json), 7)
        # Ensure basic values are correct
        self.assertEqual(json['id'], OBJECT_ID)
        self.assertEqual(json['name'], OBJECT_NAME)
        # Make sure properties are set correctly
        properties = utils.from_list(json['properties'])
        self.assertEqual(len(properties), 1)
        self.assertEqual(properties[ds.PROPERTY_NAME], OBJECT_NAME)
        # Ensure image list is represented properly
        self.assertEquals(json['images']['count'], 2)
        self.assertTrue('links' in json['images'])
        links = utils.from_list(json['images']['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertEqual(len(links), 1)
        # Ensure options are represented properly
        self.assertEqual(len(json['options']), 1)
        op = json['options'][0]
        self.assertEqual(op['name'], 'stimulus_pixels_per_degree')
        self.assertEqual(op['value'], 0.88)
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_DOWNLOAD in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertTrue(hateoas.REF_KEY_UPDATE_OPTIONS in links)
        self.assertEqual(len(links), 5)
        self_ref = '/'.join([BASE_URL, hateoas.URL_KEY_IMAGES, hateoas.URL_KEY_IMAGE_GROUPS, OBJECT_ID])
        self.assertEqual(links[hateoas.REF_KEY_SELF ], self_ref)

    def test_object_listing_references(self):
        # Create set of 2 item
        items = [
            image.ImageHandle(
                OBJECT_ID,
                {ds.PROPERTY_NAME: OBJECT_NAME},
                OBJECT_DIR
            ),
            image.ImageHandle(
                OBJECT_ID,
                {ds.PROPERTY_NAME: OBJECT_NAME},
                OBJECT_DIR
            )
        ]
        object_listing = ds.ObjectListing(items, 0, -1, 8)
        # Get Json serialization of object listing
        json = self.serializer.image_files_to_json(object_listing, None)
        # Make sure links are present and correct. Expect only two links because
        # limit is -1
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_FIRST in links)
        self.assertEqual(len(links), 2)
        # Change in offset should not change reference set
        object_listing = ds.ObjectListing(items, 5, -1, 8)
        # Get Json serialization of object listing
        json = self.serializer.image_files_to_json(object_listing, None)
        # Make sure links are present and correct. Expect only two links because
        # limit is -1
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_FIRST in links)
        self.assertEqual(len(links), 2)
        # Change limit to 2. Expect next and last page but no prev
        object_listing = ds.ObjectListing(items, 0, 2, 8)
        # Get Json serialization of object listing
        json = self.serializer.image_files_to_json(object_listing, None)
        # Make sure links are present and correct. Expect only two links because
        # limit is -1
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_FIRST in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_NEXT in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_LAST in links)
        self.assertEqual(len(links), 4)
        # Change offset to 7. Expect prev and last page but no next
        object_listing = ds.ObjectListing(items, 7, 2, 8)
        # Get Json serialization of object listing
        json = self.serializer.image_files_to_json(object_listing, None)
        # Make sure links are present and correct. Expect only two links because
        # limit is -1
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_FIRST in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_PREVIOUS in links)
        self.assertTrue(hateoas.REF_KEY_PAGE_LAST in links)
        self.assertEqual(len(links), 4)

    def test_subject_serialization(self):
        """Test serialization of subjects."""
        s = subject.SubjectHandle(
            OBJECT_ID,
            {
                ds.PROPERTY_NAME: OBJECT_NAME,
                ds.PROPERTY_FILETYPE: subject.FILE_TYPE_FREESURFER_DIRECTORY,
            },
            OBJECT_DIR
        )
        json = self.serializer.subject_to_json(s)
        # Ensure all basic elements are present
        self.assertTrue('id' in json)
        self.assertTrue('timestamp' in json)
        self.assertTrue('name' in json)
        self.assertTrue('properties' in json)
        self.assertTrue('links' in json)
        self.assertEqual(len(json), 5)
        # Ensure basic values are correct
        self.assertEqual(json['id'], OBJECT_ID)
        self.assertEqual(json['name'], OBJECT_NAME)
        # Make sure properties are set correctly
        properties = utils.from_list(json['properties'])
        self.assertEqual(len(properties), 2)
        self.assertEqual(properties[ds.PROPERTY_NAME], OBJECT_NAME)
        self.assertEqual(properties[ds.PROPERTY_FILETYPE], subject.FILE_TYPE_FREESURFER_DIRECTORY)
        # Make sure links are present and correct
        links = utils.from_list(json['links'], label_key=hateoas.LIST_KEY, label_value=hateoas.LIST_VALUE)
        self.assertTrue(hateoas.REF_KEY_SELF in links)
        self.assertTrue(hateoas.REF_KEY_DELETE in links)
        self.assertTrue(hateoas.REF_KEY_DOWNLOAD in links)
        self.assertTrue(hateoas.REF_KEY_UPSERT_PROPERTY in links)
        self.assertEqual(len(links), 4)
        self_ref = '/'.join([BASE_URL, hateoas.URL_KEY_SUBJECTS, OBJECT_ID])
        self.assertEqual(links[hateoas.REF_KEY_SELF ], self_ref)


if __name__ == '__main__':
    unittest.main()
