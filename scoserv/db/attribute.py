"""Typed attributes - Collection of classes and helper methods to handle typed
attributes and their constraints.

Typed attributes support key-value pairs where values are not simply string but
of different type, e.g., float, int, array of floats. Attribute definitions are
used to define such attrbutes.

Use of these classes is intended for typed value lists, i.e., image group
options and model run arguments.
"""

from abc import abstractmethod


# ------------------------------------------------------------------------------
#
# Constants for type names
#
# ------------------------------------------------------------------------------

ATTRTYPE_ARRAY = 'array'
ATTRTYPE_FLOAT = 'float'
ATTRTYPE_INTEGER = 'int'


# ------------------------------------------------------------------------------
#
# Attribute definitions and instances
#
# ------------------------------------------------------------------------------

class Attribute(object):
    """A typed attribute is an instantiation of an object's property that has a
    value of particular type. The expected type of the property is defined in
    the attribute definition.

    Typed attributes are used to represent properties of database objects (e.g.,
    image groups) that require a certain type (e.g., float value) with given
    constraints.

    Attributes
    ----------
    name : string
        Property name

    value : any
        Associated value for the property. Can be of any type
    """
    def __init__(self, name, value):
        """Initialize the type property instance by passing arguments for name
        and value.

        Parameters
        ----------
        name : string
            Property name

        value : any
            Associated value for the property. Can be of any type
        """
        self.name = name
        self.value = value


class AttributeDefinition(object):
    """Definition of a typed object property. Each property has a (unique) name
    and attribute type from a set of predefined data types. The attribute
    definition also includes an optional default value. The type of the value
    is dependen on the attribute type.

    Attributes
    ----------
    name : string
        Attribute name
    attrtype : datastore.AttributeType
        Attribute type from controlled set of data types
    default_value : any, optional
        Default value for instance of this type
    """
    def __init__(self, name, attrtype, default_value=None):
        """Initialize the object.

        Parameters
        ----------
        name : string
            Attribute name
        attrtype : datastore.AttributeType
            Attribute type from controlled set of data types
        default_value : any, optional
            Default value for instance of this type
        """
        # If the default value is given make sure that it is valid for given
        # attribute type. Otherwise, throw ValueError
        if not default_value is None:
            if not attrtype.validate(default_value):
                raise ValueError('Default value is not of attribute type ' + attrtype.name)
        self.name = name
        self.type = attrtype
        self.default_value = default_value

    def validate(self, value):
        """Validate whether a given variable value is of type represented by
        the attribute type associated with this definition.

        Throws ValueError is valus is not valid.

        Parameters
        ----------
        value : any
            Value to be tested

        Returns
        -------
        Boolean
            Value is of valid type
        """
        return self.type.validate(value)


class AttributeType(object):
    """Object representing the type of an attrbute. Types can be simple, e.g.,
    float and integer, or complex, e.g., array of n-tuples of a simple type.

    Each attribute type implements a method to validate that a given variable
    holds a value that is a valid instance of the type.

    Attributes
    ----------
    name : string
        Text representation of type name
    """
    def __init__(self, name):
        """Initialize the type name. the name is used to uniquely identify the
        type. For each implementation of this class a is_ofType() method
        should be added to the class definition.

        Parameters
        ----------
        name = string
            Type name
        """
        self.name = name

    @property
    def is_array(self):
        """Flag indicating whether this is an instance of type ARRAY.

        Returns
        -------
        Boolean
            True, if name equals ATTRTYPE_ARRAY
        """
        return self.name == ATTRTYPE_ARRAY

    @property
    def is_float(self):
        """Flag indicating whether this is an instance of type FLOAT.

        Returns
        -------
        Boolean
            True, if name equals ATTRTYPE_FLOAT
        """
        return self.name == ATTRTYPE_FLOAT

    @property
    def is_int(self):
        """Flag indicating whether this is an instance of type INTEGER.

        Returns
        -------
        Boolean
            True, if name equals ATTRTYPE_INTEGER
        """
        return self.name == ATTRTYPE_INTEGER

    @abstractmethod
    def validate(self, value):
        """Validate whether a given variable value is of type represented by
        this attribute type instance.

        Throws ValueError is valus is not valid.

        Parameters
        ----------
        value : any
            Value to be tested

        Returns
        -------
        Boolean
            Value is of valid type
        """
        pass


class ArrayType(AttributeType):
    """Specification of array attribute data type. This type represents arrays
    of n-tuples all having the same value type. It is expected, that the value
    type is a 'simple' attribute type, i.e., float or integer (at the moment).

    Attributes
    ----------
    value_type : AttributeType
        Type of values in each n-tuple
    """
    def __init__(self, value_type):
        """Initialize object by setting the (super) class name attribute to
        ATTRTYPE_ARRAY.

        Parameters
        ----------
        value_type : AttributeType
            Type of values in each n-tuple
        """
        super(ArrayType, self).__init__(ATTRTYPE_ARRAY)
        self.value_type = value_type

    def validate(self, value):
        """Override AttributeType.validate. Check if the given value is an
        array of n-tuples and that all values within tuples are of the type
        that is given by value_type.

        It is also ensured that all n-tuples are of same length n. Tuple length
        is not a fixed argument for array types to support the use case where
        list are either 1-tuples or 2-tupes as for stimulus_gamma in image
        groups.
        """
        # Make sure that the value is a list
        if isinstance(value, basestring):
            # If the value is a string we currently expect a list of floats
            array = []
            values = value.split(',')
            for v in values:
                array.append(self.value_type.validate(v))
            return array
        elif not isinstance(value, list):
            raise ValueError('not a list')
        # Make sure that all elements in the list are lists or tuples of
        # length tuple_length and each of the values if of the specified
        # attribute type. Also ensure that all tuples are of the same length
        # (which is unknown at the start -> common_length)
        common_length = -1
        for t in value:
            if not isinstance(t, list) and not isinstance(t, tuple):
                raise ValueError('element is not a list')
            if common_length == -1:
                common_length = len(t)
            elif len(t) != common_length:
                raise ValueError('element is not of expected length')
            for v in t:
                self.value_type.validate(v)
        # Return True if all tests where passed successfully
        return value


class FloatType(AttributeType):
    """Specification of float attribute data type."""
    def __init__(self):
        """Initialize object by setting the (super) class name attribute to
        ATTRTYPE_FLOAT.
        """
        super(FloatType, self).__init__(ATTRTYPE_FLOAT)

    def validate(self, value):
        """Override AttributeType.validate. Check if the given value is an
        instance of type float.
        """
        if isinstance(value, float) or isinstance(value, int):
            return value
        elif isinstance(value, basestring):
            return float(value)
        else:
            raise ValueError('not a float')


class IntegerType(AttributeType):
    """Specification of integer attribute data type."""
    def __init__(self):
        """Initialize object by setting the (super) class name attribute to
        ATTRTYPE_INTEGER.
        """
        super(IntegerType, self).__init__(ATTRTYPE_INTEGER)

    def validate(self, value):
        """Override AttributeType.validate. Check if the given value is an
        instance of type int.
        """
        if isinstance(value, int):
            return value
        elif isinstance(value, basestring):
            return int(value)
        else:
            raise ValueError('not a float')


# ------------------------------------------------------------------------------
#
# Helper methods
#
# ------------------------------------------------------------------------------

def get_and_validate_attributes(attributes, attr_defs):
    """Create a dictionary of options from the attribute list. The dictionary
    allows to detect duplicate definitions of the same attribute. Raises
    exception if attribute list is not in accordance with attribute
    definitions.

    Parameters
    ----------
    attributes : List(attribute.Attribute), optional
        List of image group options. If None, default values will be used.
    attr_defs : Dictionary(attribute.AttributeDefinition)
        Dictionary of attribute definitions to validate agains.

    Returns
    -------
    Dictionary
        Dictionary of attribute instances keyed by their name
    """
    options = {}
    for attr in attributes:
        if attr.name in options:
            raise ValueError('duplicate attribute: ' + attr.name)
        if not attr.name in attr_defs:
            raise ValueError('unknown attribute: ' + attr.name)
        attr_def = attr_defs[attr.name]
        # Only add attribute if value is not empty
        if isinstance(attr.value, basestring):
            if attr.value == '':
                continue
        try:
            attr_val = attr_def.validate(attr.value)
        except ValueError as ex:
            raise ValueError('invalid value for attribute: ' + attr.name)
        options[attr.name] = Attribute(attr.name, attr_val)
    return options


def get_default_attributes(attr_definitions):
    """Generate a dictionary of attribute values from a set of attribute
    definitions using default values.

    The result will include an attribute for each definition that has a default
    value defined.

    Parameters
    ----------
    attr_definitions : dict(AttributeDefinition)
        Dictionary of attribute definitions keyed by their name

    Returns
    -------
    dict(Attribute)
        List of attribute instance for definitions that have a default value
    """
    attributes = {}
    for key in attr_definitions:
        attr_def = attr_definitions[key]
        # Create  attribute instance with definitions default value if defined
        if not attr_def.default_value is None:
            attributes[attr_def.name] = Attribute(
                attr_def.name,
                attr_def.default_value
            )
    return attributes


def attributes_from_json(document):
    """Convert a Json representation of a set of attribute instances into a
    dictionary.

    Parameters
    ----------
    document : Json object
        Json serialization of attribute instances

    Returns
    -------
    dict(Attribute)
        Dictionary of attribute instance objects keyed by their name
    """
    attributes = dict()
    for attr in document:
        name = str(attr['name'])
        attributes[name] = Attribute(
            name,
            attr['value']
        )
    return attributes


def attributes_to_json(attributes):
    """Transform a dictionary of attribute instances into a list of Json
    objects, i.e., list of key-value pairs.

    Parameters
    ----------
    attributes : dict(Attribute)
        Dictionary of attribute instances

    Returns
    -------
    list(dict(name:..., value:...))
        List of key-value pairs.
    """
    result = []
    for key in attributes:
        result.append({
            'name' : key,
            'value' : attributes[key].value
        })
    return result
