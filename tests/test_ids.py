import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import scoserv.db.datastore as datastore

id = datastore.ObjectId('bla')

print id
print repr(id)
print str(id)
print '-------------------'

id = datastore.ObjectId(('bla', 'blu'))

print id
print repr(id)
print str(id)
print '-------------------'

id = datastore.ObjectId(['bla', 'blu'])

print id
print repr(id)
print str(id)
print '-------------------'

id = datastore.ObjectId({'bla':'blu'})
