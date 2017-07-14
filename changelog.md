# Standard Cortical Observer Web Server API - Changelog

### 0.1.0 - 2017-03-13

* Initial Version after splitting web server code into two repositories

### 0.2.0 - 2017-03-14

* Moved SCO workflow engine into separate repository

### 0.3.0 - 2017-03-16

* Create individual API calls to update model run state (and upload results)

### 0.4.0 - 2017-03-18

* Read image group parameter and model definitions from files
* Parser for different types of image group and model run parameter
* Add model reference to model run

### 0.5.0 - 2017-05-05

* Add model repository to API
* Move generation of request attribute lists back to sco datastore
* Add module to load list of models into database
* Configuration from YAML (default on Web)
* Add lists of valid attachments and list of widget definitions


### 0.5.1 - 2017-05-19

* Add RabbitMQ virtual host parameter to server configuration
* Update Url patterns for file download
* Fixed bug in API when deleting failed model runs
* Add init_database script
* Add entry for image list file attachment


### 0.6.0 - 2017-06-27

* Adjust to changes in data store and model store
* Bug-fix when deleting unknown attachment

### 0.7.0 - 2017-06-28

* Merge sco-models into sco-engine

### 0.8.0 - 2017-06-30

* Add visualization widgets as database objects

### 0.9.0 - 2017-07-01

* Add update model connector API call
* Adjust to renaming of from_json/to_json to from_dict/to_dict
* Add API documentation to service links


### 0.9.1 - 2017-07-02

* Replace is_widget property of widget resources with type according to changes in sco-datastore
* Change download Url for model run attachments
* Add widgets for successful model run resources

### 0.10.0 - 2017-07-03

* Change download Url's for file resources to end with the name of the downloaded file

### 0.10.1 - 2017-07-03

* Add attachments.create reference for all model runs
* Make proper use of mime type information from the model definition for attachment uploads

### 0.11.0 - 2017-07-13

* Add content pages
* Read page content on access
