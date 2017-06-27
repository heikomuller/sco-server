"""Helper module to load an initial set of SCO models into the model
repository. Expects the model definitions in a .json file.
"""

import json
import yaml
import sys

from scodata.mongo import MongoDBFactory
from scomodels import init_registry_from_json


if __name__ == '__main__':
    # Expect the configuration file as first and the model definition file as
    # second argument. An optional third argument contains the clear collection
    # flag (default: False)
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print 'Usage: <config-file> <model-defs-file> {<clear-collection-flag>}'
        sys.exit()
    # Read configuration file (YAML)
    with open(sys.argv[1], 'r') as f:
        obj = yaml.load(f)
        config = {item['key']:item['value'] for item in obj['properties']}
    # Get Mongo client factory
    mongo = MongoDBFactory(db_name=config['mongo.db'])
    # Set clear collection flag if given
    if len(sys.argv) == 4:
        clear_collection = (sys.argv[3].upper() == 'TRUE')
    else:
        clear_collection = False
    # Load models
    init_registry_from_json(
        mongo,
        sys.argv[2],
        clear_collection=clear_collection
    )
