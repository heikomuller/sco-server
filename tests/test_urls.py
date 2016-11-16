import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import scoserv.hateoas as api
import scoserv.datastore as datastore
import scoserv.subject as subject

#pagination = api.PaginationDecorator('MYURL', 95, 10, 100, 'A,B,C')
pagination = api.PaginationDecorator('MYURL', 85, 10, 100, 'A,B,C')

print pagination.first()
print pagination.last()
print pagination.next()
print pagination.prev()


urls = api.UrlFactory('http://localhost/sco')
print urls.subjects_delete('1234567890')
print urls.subjects_download('1234567890')
print urls.subjects_get('1234567890')
print urls.subjects_list()
print urls.subjects_upload()
print urls.subjects_upsert_property('1234567890')

subj = subject.SubjectHandle(
    '1234567890',
    {
        datastore.PROPERTY_NAME:'My Subject',
        datastore.PROPERTY_FILENAME:'my-file.tar'
    },
    'dir',
    subject.FILE_TYPE_FREESURFER_DIRECTORY)

print api.HATEOASReferenceFactory(urls).object_references(subj)
