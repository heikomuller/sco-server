# Standard Cortical Observer - Web API

The Standard Cortical Observer Web API (SCO-API) provides access to files and data objects in the SCO-DS and allows to run predictive models using these data objects.


## Hypermedia as the Engine of Application State (HATEOAS)

Following the Hypermedia as the Engine of Application State (HATEOAS) constraint, all resources returned by the SCO-API have a list of references associated with them (usually accessible through element *links* in their Json serialization). Reference lists are arrays of objects with the following two elements:

- rel: Identifier for reference type
- href: Url for SCO-API call


### Object listings

Object listings have following references in the reference list for navigation:

- self: Object listing base reference
- first: Navigate to first page in object listing
- last: Navigate to last page in object listing (only if there is  a last page)
- next: Navigate to next page in object listing (only if there is  a next page)
- prev: Navigate to previous page in object listing (only if there is a previous page)


### Object references

The following is a listing of references associated with different types of objects that are accessible via the SCO-API.

**Experiments**

- self: Self reference to object (HTTP GET)
- delete: Delete object (HTTP DELETE)
- properties: Upsert object properties (HTTP POST)
- predictions.list: List experimetn predictions (HTTP GET)
- predictions.run: Start new predictive model run (HTTP POST)
- fmri.upload: Associate fMRI data with experiment (HTTP POST)
- fmri.get: Get associated fMRI data object if present (HTTP GET)

**Experiments fMRI Data**

- self: Self reference to object (HTTP GET)
- delete: Delete object (HTTP DELETE)
- download: Download data file (HTTP GET)
- properties: Upsert object properties (HTTP POST)

**Model Runs**

- self: Self reference to object (HTTP GET)
- delete: Delete object (HTTP DELETE)
- download: Download data file (only if run state is SUCCESS) (HTTP GET)
- properties: Upsert object properties (HTTP POST)

**Image Files**

- self: Self reference to object (HTTP GET)
- delete: Delete object (HTTP DELETE)
- download: Download data file (HTTP GET)
- properties: Upsert object properties (HTTP POST)

**Image Groups**

- self: Self reference to object (HTTP GET)
- delete: Delete object (HTTP DELETE)
- download: Download data file (HTTP GET)
- options: Update object options (HTTP POST)
- properties: Upsert object properties (HTTP POST)

**Subjects**

- self: Self reference to object (HTTP GET)
- delete: Delete object (HTTP DELETE)
- download: Download data file (HTTP GET)
- properties: Upsert object properties (HTTP POST)
