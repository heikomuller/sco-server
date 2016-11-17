"""Collection of custom exceptions for the Standard Cortical Observer Web API.
"""


# ------------------------------------------------------------------------------
#
# API Request Exceptions
#
# ------------------------------------------------------------------------------

class APIRequestException(Exception):
    """Base class for API exceptions."""
    def __init__(self, message, status_code):
        """Initialize error message and status code.

        Parameters
        ----------
        message : string
            Error message.
        status_code : int
            Http status code.
        """
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code

    def to_dict(self):
        """Dictionary representation of the exception.

        Returns
        -------
        Dictionary
        """
        return {'message' : self.message}

class InvalidRequest(APIRequestException):
    """Exception for invalid requests that have status code 400."""
    def __init__(self, message):
        """Initialize the message and status code (400) of super class.

        Parameters
        ----------
        message : string
            Error message.
        """
        super(InvalidRequest, self).__init__(message, 400)


class ResourceNotFound(APIRequestException):
    """Exception for file not found situations that have status code 404."""
    def __init__(self, object_id, object_type=None):
        """Initialize the message and status code (404) of super class.

        Parameters
        ----------
        object_id : string
            Identifier of unknown resource
        object_type : string, optional
            Optional name of expected type of unknown resource
        """
        # Build the response message depending on whether object type is given
        message = 'unknown identifier'
        if not object_type is None:
            message += ' for type ' + object_type
        message += ': ' + object_id
        # Initialize the super class
        super(ResourceNotFound, self).__init__(message, 404)


class UnknownObjectType(InvalidRequest):
    """Exception to signal that a DBObject of unknown type is encountered."""
    def __init__(self, object_type):
        """Initialize the exception with the unknown object type.

        Parameters
        ----------
        object_type : string
            String representation of database object type
        """
        super(UnknownObjectType, self).__init__('unknown object type: ' + object_type)
