"""MongoDB client factory - The SCO Engine uses multi-processing to execute
model runs. Thus, it should create a new instance of the mongo client for
each process. The MongoDBFactory is used as a central place to establish
connection to MongoDB for the SCO Data Store and the SCO Engine.
"""

from pymongo import MongoClient


class MongoDBFactory(object):
    """Factory pattern to establish connection to default mongo database used
    by the current implementation of the SCO Web API.
    """
    def __init__(self, db_name='scoserv'):
        """Initialize the database name.

        Parameters
        ----------
        db_name : string, optional
            Name of the database (default: scoserv)
        """
        self.db_name = db_name
        
    def get_database(self):
        """Create a new default mongo database object.

        Returns
        -------
        MongoDb.database
            MongoDB database object
        """
        return MongoClient()[self.db_name]
