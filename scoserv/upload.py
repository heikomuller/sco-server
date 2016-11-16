# ------------------------------------------------------------------------------
# File Upload Manager
#
# Handle file uploads. This is a wrapper around Anatomy manager and Image
# managers. Primarily used to handler archives of image files.
# ------------------------------------------------------------------------------

import os
import shutil
import tarfile
import tempfile


# ------------------------------------------------------------------------------
# Upload Manager
#
# Wrapper around managers for storing anatomy and image files. For archives
# of image files a image collection is created that contains all the images
# in the archive.
# ------------------------------------------------------------------------------
class UploadManager:
    # --------------------------------------------------------------------------
    # Initialize references to Anatomy manager, Image manager, Image collection
    # manager.
    #
    # anatomy_manager::AnatomyManager
    # image_manager::ImageManager
    # image_collection_manager::ImageCollectionManager
    # --------------------------------------------------------------------------
    def __init__(self, anatomy_manager, image_manager, image_collection_manager):
        self.anatomy_manager = anatomy_manager
        self.image_manager = image_manager
        self.image_collection_manager = image_collection_manager

    # --------------------------------------------------------------------------
    # Upload an brain anatomy MRI file. We currently only support Freesurfer
    # archives. Thus, this method is a simple wrapper around the upload method
    # of the anatomy manager. In future implementations we can extend the method
    # with parameter that help distinguish between different file types.
    #
    # filename::string
    #
    # returns AnatomyHandle
    # --------------------------------------------------------------------------
    def upload_anatomy(self, filename):
        return self.anatomy_manager.upload_freesurfer_archive(filename)

    # --------------------------------------------------------------------------
    # Upload an image file or archive containing images. The file type is
    # determined from the file suffix. For image archives a image collection
    # is created.
    #
    # filename::string
    #
    # returns ImageHandle or ImageCollectionHandle
    # --------------------------------------------------------------------------
    def upload_images(self, filename):
        # Test if file is a tar file (based on suffiex .tar, tgz, tar.gz)
        if filename.endswith('.tar') or filename.endswith('.tgz') or filename.endswith('.tar.gz'):
            # Unpack the archive and create objects for each image file
            temp_dir = tempfile.mkdtemp()
            try:
                tf = tarfile.open(name=filename, mode='r')
                tf.extractall(path=temp_dir)
            except (tarfile.ReadError, IOError) as err:
                # Clean up in case there is an error during extraction
                shutil.rmtree(temp_dir)
                raise err
            images = []
            for f in get_image_files(temp_dir, []):
                print f
                img = self.image_manager.create_from_file(f)
                images.append(img.identifier)
            # Raise an error if the archive does not contain any image files
            if len(images) == 0:
                raise ValueError('Not an image archive: ' + filename)
            # Remove the extracted image archive
            shutil.rmtree(temp_dir)
            # Create an image collection that contains all created images. The
            # collection name is derived from the file name.
            filename = os.path.basename(os.path.normpath(filename))
            if filename.endswith('.tar') or filename.endswith('.tgz'):
                coll_name = filename[:-4]
            else:
                # .tar.gz
                coll_name = filename[:7]
            return self.image_collection_manager.create_collection(coll_name, images, filename=filename)
        else:
            # Get the file suffix and test if it is a valid image suffix
            pos = filename.rfind('.')
            if pos == -1:
                raise ValueError('Unknown file format: ' + filename)
            suffix = filename[pos:]
            if not suffix in VALID_IMGFILE_SUFFIXES:
                raise ValueError('Unknown file format: ' + filename)
            return self.image_manager.create_from_file(filename)


# ------------------------------------------------------------------------------
#
# HELPER METHODS
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Recursively iterate through directory tree and list all files that have a
# valid image file suffix
#
# directory::directory
# files::[string]
# ------------------------------------------------------------------------------
def get_image_files(directory, files):
    # For each file in the directory test if it is a valid image file or a
    # sub-directory.
    for f in os.listdir(directory):
        abs_file = os.path.join(directory, f)
        if os.path.isdir(abs_file):
            # Recursively iterate through sub-directories
            get_image_files(abs_file, files)
        else:
            # Add to file collection if has valid suffix
            if '.' in f and '.' + f.rsplit('.', 1)[1] in VALID_IMGFILE_SUFFIXES:
                files.append(abs_file)
    return files
