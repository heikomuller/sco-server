"""Visualization Widgets - Widgets are maintained like other database objects.
Each widget is rendered by a widget engine. A widget has a engine-specific
specification (code) and a list of inputs that enable to widget to be displayed.
These inputs are (model, attachment name)-pairs.
"""

import datetime
import uuid

from scodata.datastore import ObjectHandle, MongoDBStore


class WidgetInput(object):
    """Widget inputs are pairs of model identifier and attachment name. The
    inputs define which widgets are displayable for a given model run.

    Attributes
    ----------
    model_id : string
        Model identifier
    attachment_name : string
        Attachment name
    """
    def __init__(self, model_id, attachment_name):
        """Initialize the input descriptor.

        Parameters
        ----------
        model_id : string
            Model identifier
        attachment_name : string
            Attachment name
        """
        self.model_id = model_id
        self.attachment_name = attachment_name

    def equals(self, inp):
        """Test if two input descriptors have equal model identifier and
        attachment names.

        Parameters
        ----------
        inp : WidgetInput
            Widget input descriptor

        Returns
        -------
        bool
        """
        return self.model_id == inp.model_id and self.attachment_name == inp.attachment_name

    @staticmethod
    def from_dict(doc):
        """Create object instance from dictionary. Raises a ValueError if one
        of the expected keys is missing.

        Parameters
        ----------
        doc : dict
            Dictionary serialization of a widget input objects

        Returns
        -------
        WidgetInput
        """
        for key in ['model', 'attachment']:
            if not key in doc:
                raise ValueError('missing element: ' + key)
        return WidgetInput(doc['model'], doc['attachment'])

    def to_dict(self):
        """Dictionary serialization for this object:

        Returns
        -------
        dict
        """
        return {
            'model' : self.model_id,
            'attachment' : self.attachment_name
        }

class WidgetHandle(ObjectHandle):
    """Handle for a visuaization widget. In addition to the general object
    properties, widgets contain a reference to the visualization engine that
    is able to render them, a engine-specific code object, and a list of
    input descriptors that define whether a widget is displayable for a given
    model run or not.

    Attributes
    ----------
    engine_id : string
        Identifier of the visualization engine_id
    code : dict
        Engine-specific information
    inputs : list(WidgetInput)
        List of inputs that allow the widget to be rendered.
    """
    def __init__(self, identifier, properties, engine_id, code, inputs, timestamp=None):
        """Initialize the widget handle.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        properties : Dictionary
            Dictionary of subject specific properties
        engine_id : string
            Identifier of the visualization engine_id
        code : dict
            Engine-specific information
        inputs : list(WidgetInput)
            List of inputs that allow the widget to be rendered.
        timestamp : datetime, optional
            Time stamp of object creation (UTC).
        """
        # Initialize super class
        super(WidgetHandle, self).__init__(
            identifier,
            timestamp,
            properties,
            is_active=True
        )
        # Initialize local object variables
        self.engine_id = engine_id
        self.code = code
        self.inputs = inputs

    @property
    def is_widget(self):
        """Override the is_widget property of the base class."""
        return True


class WidgetRegistry(MongoDBStore):
    """Default implementation for widget registry. Uses MongoDB as storage
    backend and makes use of the SCO datastore implementation. Provides
    wrappers for delete, get, and list model operations.
    """
    def __init__(self, mongo):
        """Initialize the MongoDB collection where widgets are being stored.

        Parameters
        ----------
        mongo : scodata.MongoDBFactory
            MongoDB connector
        """
        super(WidgetRegistry, self).__init__(mongo.get_database().widgets)

    def append_input_for_widget(self, identifier, input_descriptor):
        """Append an input descriptor to the list of inputs for a widget. Will
        return None if no widget with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique widget identifier
        input_descriptor : WidgetInput
            Descriptor of input that is to be appended.

        Returns
        -------
        WidgetHandle
        """
        # Get the widget handle to ensure that it exists
        widget = self.get_widget(identifier)
        if widget is None:
            return None
        # Ensure that no input with the same properties as the given one exists
        for inp in widget.inputs:
            if inp.equals(input_descriptor):
                return widget
        widget.inputs.append(input_descriptor)
        self.replace_object(widget)
        return widget

    def create_widget(self, properties, engine, code, inputs):
        """Create an experiment object for the subject and image group. Objects
        are referenced by their identifier. The reference to a functional data
        object is optional.

        Raises ValueError if no valid experiment name is given in property list.

        Parameters
        ----------
        properties : Dictionary
            Dictionary of widget specific properties.
        engine_id : string
            Identifier of the visualization engine_id
        code : dict
            Engine-specific information
        inputs : list(WidgetInput)
            List of inputs that allow the widget to be rendered.

        Returns
        -------
        WidgetHandle
            Handle for created widget object in database
        """
        # Create a new object identifier.
        identifier = str(uuid.uuid4()).replace('-','')
        obj = WidgetHandle(identifier, properties, engine, code, inputs)
        self.insert_object(obj)
        return obj

    def delete_widget(self, identifier):
        """Delete the widget with given identifier in the database. Returns the
        handle for the deleted widget or None if object identifier is unknown.

        Widgets are always erased from the database when deleted.

        Parameters
        ----------
        identifier : string
            Unique widget identifier

        Returns
        -------
        WidgetHandle
        """
        return self.delete_object(identifier, erase=True)

    def find_widgets_for_model(self, model_id):
        """Retrieve all widgets in the database that take inputs generated by
        a given model.

        Returns a dictionary of lists of widgets that are keyed by attachment
        names. Each list is  list of widgets that will take the particular
        attachment for the given model as input.

        Parameters
        ----------
        model_id : string
            Unique model identifier

        Returns
        -------
        dict
            Dictionary of lists of widget handles.
        """
        result = {}
        # Use MongoDB query to get all documents that contain an input
        # descriptor for the given model
        for doc in self.collection.find({'inputs.model' : model_id}):
            widget = self.from_dict(doc)
            for inp in widget.inputs:
                if inp.model_id == model_id:
                    key = inp.attachment_name
                    if key in result:
                        result[key].append(widget)
                    else:
                        result[key] = [widget]
        return result

    def from_dict(self, document):
        """Create a widget handle from a given Json document.

        Parameters
        ----------
        document : dict
            Serialization for widget handle

        Returns
        -------
        WidgetHandle
        """
        return WidgetHandle(
            document['_id'],
            document['properties'],
            document['engine'],
            document['code'],
            [WidgetInput.from_dict(doc) for doc in document['inputs']],
            timestamp=datetime.datetime.strptime(
                document['timestamp'],
                '%Y-%m-%dT%H:%M:%S.%f'
            )
        )

    def get_widget(self, identifier):
        """Retrieve widget with given identifier from the database.

        Parameters
        ----------
        identifier : string
            Unique widget identifier

        Returns
        -------
        WidgetHandle
            Handle for widget with given identifier or None if no widget
            with identifier exists.
        """
        return self.get_object(identifier, include_inactive=False)

    def list_widgets(self, limit=-1, offset=-1):
        """List widgets in the database. Takes optional parameters limit and
        offset for pagination.

        Parameters
        ----------
        limit : int
            Limit number of widgets in the result set
        offset : int
            Set offset in list (order as defined by object store)

        Returns
        -------
        ObjectListing
        """
        return self.list_objects(limit=limit, offset=offset)

    def to_dict(self, widget):
        """Create a Json-like object for a widget.

        Parameters
        ----------
        widget : WidgetHandle

        Returns
        -------
        dict
            Json-like object representation
        """
        # Get the basic Json object from the super class
        obj = super(WidgetRegistry, self).to_dict(widget)
        # Add model parameter
        obj['engine'] = widget.engine_id
        obj['code'] = widget.code
        obj['inputs'] = [
            inp.to_dict() for inp in widget.inputs
        ]
        return obj

    def update_widget(self, identifier, code=None, inputs=None):
        """Update parts of a widget. One can either update the widget code or
        the inputs. Both can also be updated together. The widget enfine cannot
        be changed.

        Will return None if no widget with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique widget identifier
        code : dict, optional
            New engine-specific code. If None the existing code will not be
            changed
        inputs : list(WidgetInput), optional
            New list of widget inputs. If None the existing list will not be
            changed.

        Returns
        -------
        WidgetHandle
        """
        # Get the widget handle to ensure that it exists
        widget = self.get_widget(identifier)
        if widget is None:
            return None
        # Return handle if both optional parameters are None
        if code is None and inputs is None:
            return widget
        # Update the handle based on the given parameters
        if not code is None:
            widget.code = code
        if not inputs is None:
            widget.inputs = inputs
        self.replace_object(widget)
        return widget
