import gzip
import os
import shutil
import sys
import unittest

sys.path.insert(0, os.path.abspath('..'))

import scoserv.mongo as mongo
import scoserv.db.attribute as attributes
import scoserv.db.image as images

TMP_DIR = '/tmp/sco/images'
DATA_DIR = './data'
IMAGES_DIR = 'images'
IMAGES_ARCHIVE = 'images/images.tar.gz'

class TestImageManagerMethods(unittest.TestCase):

    def setUp(self):
        """Connect to MongoDB and clear an existing images and image group
        collection. Ensure that data directory exists and is empty. Then create
        object managers."""
        m = mongo.MongoDBFactory(db_name='scotest')
        db = m.get_database()
        db.images.drop()
        db.imagegroups.drop()
        if os.path.isdir(TMP_DIR):
            shutil.rmtree(TMP_DIR)
        os.makedirs(TMP_DIR)
        self.mngr_images = images.DefaultImageManager(db.images, TMP_DIR)
        self.mngr_groups = images.DefaultImageGroupManager(db.imagegroups, TMP_DIR, self.mngr_images)

    def test_images_create(self):
        """Test creation of images and image groups."""
        # Create an image object for each image file in the DATA_DIR
        img_list = []
        images_dir = os.path.join(DATA_DIR, IMAGES_DIR)
        for f in os.listdir(images_dir):
            prop_name = str(f)
            pos = prop_name.rfind('.')
            if pos >= 0:
                suffix = prop_name[pos:].lower()
                # Create temp copy of the file
                tmp_file = os.path.join(TMP_DIR, f)
                shutil.copyfile(os.path.join(images_dir, f), tmp_file)
                if suffix in images.VALID_IMGFILE_SUFFIXES:
                    img = self.mngr_images.create_object(tmp_file)
                    # Assert that object is active and is_image property is true
                    self.assertTrue(img.is_active)
                    self.assertTrue(img.is_image)
                    # Ensure that other class type properties are false
                    self.assertFalse(img.is_experiment)
                    self.assertFalse(img.is_image_group)
                    self.assertFalse(img.is_functional_data)
                    self.assertFalse(img.is_model_run)
                    self.assertFalse(img.is_subject)
                    img_list.append(img.identifier)
                else:
                    with self.assertRaises(ValueError):
                        self.mngr_images.create_object(tmp_file)
                    os.remove(tmp_file)
        # We expect four images to be created
        self.assertEqual(len(img_list), 4)
        # Ensure that the list of images in the database equals the number of
        # elements in images
        self.assertEqual(self.mngr_images.list_objects().total_count, len(img_list))
        # Ensure that image files for created image objects exist and create
        # GroupImageObjects
        group = []
        for img_id in img_list:
            img = self.mngr_images.get_object(img_id)
            self.assertTrue(os.path.isfile(img.image_file))
            group.append(images.GroupImage(img_id, '/', img.name, ''))
        # Create image group for image objects
        tmp_file = os.path.join(TMP_DIR, os.path.basename(IMAGES_ARCHIVE))
        shutil.copyfile(os.path.join(DATA_DIR, IMAGES_ARCHIVE), tmp_file)
        img_group = self.mngr_groups.create_object('NAME', group, tmp_file)
        # Ensure that object is active and is_image_group property is true
        self.assertTrue(img_group.is_active)
        self.assertTrue(img_group.is_image_group)
        # Ensure that other class type properties are false
        self.assertFalse(img_group.is_experiment)
        self.assertFalse(img_group.is_image)
        self.assertFalse(img_group.is_functional_data)
        self.assertFalse(img_group.is_model_run)
        self.assertFalse(img_group.is_subject)
        # Get image group from database and ensure that there are four files
        # in the list, one for each of the images in img_list
        img_group = self.mngr_groups.get_object(img_group.identifier)
        self.assertEquals(len(img_group.images), 4)
        grp_images = {}
        for group_image in img_group.images:
            self.assertTrue(group_image.identifier in img_list)
            grp_images[group_image.identifier] = group_image
        for img_id in img_list:
            self.assertTrue(img_id in grp_images)
        # Ensure that the group identifier is correct for all images in
        # img_group
        for img_id in img_list:
            self.assertTrue(img_group.identifier in self.mngr_groups.get_collections_for_image(img_id))
        # Ensure the group image listing will only contain three elements
        # despite limit being 10
        listing = self.mngr_groups.list_images(img_group.identifier, limit=10, offset=1)
        self.assertEqual(len(listing.items), 3)

    def test_images_update_attributes(self):
        """Test functionality of updating attributes associated with an image
        group."""
        # Create image group with fake image
        group = [images.GroupImage('1', '/', 'NAME', '')]
        tmp_file = os.path.join(TMP_DIR, os.path.basename(IMAGES_ARCHIVE))
        shutil.copyfile(os.path.join(DATA_DIR, IMAGES_ARCHIVE), tmp_file)
        img_group = self.mngr_groups.create_object(
            'NAME',
            group,
            tmp_file,
            options=[attributes.Attribute('stimulus_edge_value', 0.8)]
        )
        # Ensure that updating attributes does not raise exception
        attrs = [
            attributes.Attribute('stimulus_edge_value', 0.8),
            attributes.Attribute('stimulus_aperture_edge_value', 0.75)
        ]
        self.mngr_groups.update_object_options(img_group.identifier, attrs)
        # Ensure that exception is raised if unknown attribute name is given
        with self.assertRaises(ValueError):
            self.mngr_groups.update_object_options(
                img_group.identifier,
                [
                    attributes.Attribute('not_a_defined_attribute', 0.8),
                    attributes.Attribute('stimulus_edge_value', 0.75)
                ]
            )
        # Ensure that exception is raised if duplicate attribute names are in
        # update list
        with self.assertRaises(ValueError):
            self.mngr_groups.update_object_options(
                img_group.identifier,
                [
                    attributes.Attribute('stimulus_edge_value', 0.8),
                    attributes.Attribute('stimulus_edge_value', 0.75)
                ]
            )
        # Ensure that exception is raised if invlid value type is given
        with self.assertRaises(ValueError):
            self.mngr_groups.update_object_options(
                img_group.identifier,
                [
                    attributes.Attribute('stimulus_edge_value', 0.8),
                    attributes.Attribute('stimulus_aperture_edge_value', 'abc')
                ]
            )


if __name__ == '__main__':
    # Pass data directory as optional parameter
    if len(sys.argv) == 2:
        DATA_DIR = sys.argv[1]
    sys.argv = sys.argv[:1]
    unittest.main()
