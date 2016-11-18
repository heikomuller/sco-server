from pymongo import MongoClient

db = MongoClient().scotest

obj = {
    'identifier' : ['A', 'B'],
    'name' : 'My Name'
}
db.coll.insert_one(obj)

for document in db.coll.find({'identifier' : ['A', 'B']}):
    print document

print '----------------------'

for document in db.coll.find({'identifier' : ['A', 'C']}):
    print document

print '----------------------'


for document in db.coll.find({'identifier' : ['A']}):
    print document

print '----------------------'

db.coll.drop()
