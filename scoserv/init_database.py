"""Helper module to create an empty database with an initial set of SCO models.
Expects the model definitions in a .json file.
"""

import json
import yaml
import sys

from scodata.mongo import MongoDBFactory
import scomodels


def load_models(model_defs, mongo_collection):
    """Load model defininitions into given mongo collection. The clear flag
    indicates whether existing models in the collection should be deleted.

    Parameters
    ----------
    model_fefs : list()
        List of model definitions in Json-like format
    mongo_collection : MongoDB collection
        MongoDB collection where models are stored
    """
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
    if len(sys.argv) != 3:
        print 'Usage: <config-file> <model-defs-file>'
        sys.exit()
    # Read configuration file (YAML)
    with open(sys.argv[1], 'r') as f:
        obj = yaml.load(f)
        config = {item['key']:item['value'] for item in obj['properties']}
    # Read model definition file (JSON)
    with open(sys.argv[2], 'r') as f:
        models = json.load(f)
    # Get Mongo client factory
    mongo = MongoDBFactory(db_name=config['mongo.db'])
    # Load models
    load_models(models, mongo.get_database().models)
