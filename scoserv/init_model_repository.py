"""Helper module to load an initial set of SCO modesl into the model
repository. Expects the model definitions in a .json file.
"""

import json
import yaml
import sys

from scodata.mongo import MongoDBFactory
import scomodels


def load_models(model_defs, mongo_collection, clear_collection=False):
    """Load model defininitions into given mongo collection. The clear flag
    indicates whether existing models in the collection should be deleted.

    Parameters
    ----------
    model_fefs : list()
        List of model definitions in Json-like format
    mongo_collection : MongoDB collection
        MongoDB collection where models are stored
    clear_collection : boolean
        If true, collection will be dropped before models are created
    """
    # Drop collection if clear flag is set to True
    if clear_collection:
        mongo_collection.drop()
    # Create model registry
    registry = scomodels.DefaultModelRegistry(mongo_collection)
    for i in range(len(model_defs)):
        model = registry.from_json(model_defs[i])
        registry.register_model(
            model.identifier,
            model.properties,
            model.parameters
        )


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
    # Read model definition file (JSON)
    with open(sys.argv[2], 'r') as f:
        models = json.load(f)
    # Set clear collection flag if given
    if len(sys.argv) == 4:
        clear_collection = (sys.argv[3].upper() == 'TRUE')
    else:
        clear_collection = False
    # Get Mongo client factory
    mongo = MongoDBFactory(db_name=config['mongo.db'])
    # Load models
    load_models(models, mongo.get_database().models, clear_collection=clear_collection)
