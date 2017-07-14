"""Module containing classes to manage content pages and access their
content.
"""

import urllib2


class ContentPage(object):
    """Representation of a content page. Content pages have unique identifier,
    short labels, titles, and a body. The content of the body is read from file
    or from an Url on demand.

    Attributes
    ----------
    id : string
        Unique content page identifier
    label : string
        Short label (e.g. for display in drop-down menu)
    title : string
        (Longer) Title (e.g., to be used as Html page title or headline)
    body : string
        Content of the page
    """
    def __init__(self, page):
        """Initialize the page from a page descriptor. The descriptor is
        expected to contain the following fields: id, label, tittle, and
        content.

        Raises ValueError if descriptor is invalid.

        Parameters
        ----------
        page : dict
            Page descriptor
        """
        for key in ['id', 'label', 'title', 'sortOrder', 'resource']:
            if not key in page:
                raise ValueError('missing element in page descriptor: ' + key)
        self.id = page['id']
        self.label = page['label']
        self.title = page['title']
        self.sort_order = page['sortOrder']
        self.resource = page['resource']

    @property
    def body(self):
        """Read the page body from the content specifier that was provided as
        part of the page descriptor when the object was initialized. Reads
        either from a local file or a Url.

        Returns
        -------
        string
        """
        # Simple distinction between local file or Url based on the content
        # descrptior's prefix (could be improved in the future)
        for url_prefix in ['http://', 'https://', 'file://']:
            if self.resource.startswith(url_prefix):
                return urllib2.urlopen(self.resource).read()
        # Read local file if resource is not a Url
        with open(self.resource, 'r') as f:
            return f.read()
