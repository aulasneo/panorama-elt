# Change log
## 0.0.1
Initial version
## 0.1.1
Complete refactor of the modules and the settings file.
Now datasource and datalake settings are more clearly decoupled.
## 0.1.2
Add support for csv file datasource
## 0.1.3
Add support for xls file datasources
Add set-tables command to query the datasources for existing tables and update the settings
## 0.1.4
Fix bug in date parsing
Fix connection to MongoDB: use direct connection and secondaryPreferred to avoid issues in certain clusters
Reorganize packages
Create and document Dockerfile
## 0.1.5
Fix a bug in date parsing

Allow "value" key in the fields definition to override the field with a constant value.
The field will not be queried, but will be filled with the constant value. 
E.g., can be set to 'null' if a field is missing the the DB but still needed in the datalake.

## 0.1.6
Fix bug