"""Collection of custom exceptions for the Standard Cortical Observer Web API.
"""


class UnknownObjectType(Exception):
    """Exception to signal that a DBObject of unknown type is encountered."""
    def __init__(self, type):
        """Initialize the exception with the unknown object type.

        Parameters
        ----------
        type : string
            String representation of database object type
        """
        self.type = type

    def __str__(self):
        return 'Unknown object type: ' + self.type
