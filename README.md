# panorama-elt
Tools to extract data load to the datalake and transform into a source of truth for Panorama analytics.

## Introduction

[Panorama](https://www.aulasneo.com/panorama-analytics/) is the analytics solution developed by [Aulasneo](https://www.aulasneo.com) for Open edX. 
It is a complete stack that includes data extraction, load, transformation, 
visualization and analysis. The data extracted is used to build a datalake that can easily
combine multiple LMS installations and even other sources of data.

This utility is in charge of connecting to the MySQL and MongoDB tables and extracting 
the most relevant tables. Then it uploads the data to the datalake and updates all tables and partition.

## Requirements

- Linux system
- Python 3.5 or newer
- Python3 distutils (`sudo apt install python3-distutils`)
- make (`sudo apt install make`)
- virtualenv (`pip install virtualenv`)

`make install` will create a new virtual environment and install all further dependencies

listed in `requirements.txt` 
## Installation
Panorama EL can be installed in the same application host, in the databases host or in 
a bastion host. The requirement is to have connection to the databases to collect data
and to S3 that hosts the datalake.

1. Clone the repo
```shell
git clone https://github.com/aulasneo/panorama-elt
```
2. Install
```shell
make install
```
Important: do not run from inside a virtual environment
This command will:
- Create a `virtualenv` in the `venv` directory with python3 interpreter
- Install python dependencies
- Copy the settings file (if it doesn't exist) from the template

_Note_: it is possible to split the installation in multiple hosts, e.g. in one to 
access the MySQL tables and in another for the MongoDB course structures.

## Setting up the datalake
The `PanoramaDatalake` class provided is set up to connect to a AWS datalake.
However, it's methods can be overridden for other datalake technologies.

To set up your AWS datalake, you will need to:
- create or use an IAM user or role with permissions to access the S3 buckets, KMS if encrypted, Glue and Athena.
- create one S3 bucket to store the data and another as the Athena queries results location
  - we recommend to use encrypted buckets, and to have strict access policies to them
- create the Panorama database in Athena with `CREATE DATABASE panorama`
- create the Athena workgroup to keep the queries isolated from other projects
  - set the 'Query result location' to the bucket created for this workgroup

See the _first run_ section bellow to complete the datalake setup. 

### User permissions to work with AWS datalake

In order to work with a AWS datalake, you will need to create a user (e.g. _panorama-elt_)
and assign a policy (named e.g. _PanoramaELT_) with at least the following permissions.

Replace **\<region>** and **\<account id>** with proper values. 

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "glue:BatchCreatePartition",
                "glue:GetDatabase",
                "athena:StartQueryExecution",
                "glue:CreateTable",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:DeleteTable",
                "glue:GetPartitions",
                "glue:UpdateTable"
            ],
            "Resource": [
                "arn:aws:athena:<region>:<account id>:workgroup/panorama",
                "arn:aws:glue:<region>:<account id>:database/panorama",
                "arn:aws:glue:<region>:<account id>:catalog",
                "arn:aws:glue:<region>:<account id>:table/panorama/*"
            ]
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }
    ]
}
```

If you have encrypted S3 buckets with KMS, you may need to add permissions to get
the KMS keys.

## Configuration

By default, the settings file is `panorama_settings.yaml`. You can override this 
setting by using the `--settings` option.

Edit the settings file and update the credentials information of the datasources.
For Open edX usage, you will have to set the following variables:

#### MySQL configuration
- mysql_username
- mysql_password
- mysql_host
- mysql_database

#### Course structures (MongoDB) configuration
- mongodb_host
- mongodb_username
- mongodb_password
- mongodb_database

#### Datalake configuration

- panorama_raw_data_bucket: destination bucket to store the datalake files
- datalake_database
- datalake_workgroup

- aws_access_key
- aws_secret_access_key
- aws_region (of the athena engine)

#### Identify the LMS 

- In the _datalake.base_partitions_ section, edit the _value_ corresponding to the _lms_ key with the url of the LMS
  (e.g.: lms.example.com)

## Running the scripts
***Before running any command, please see the next section to do the first run***

The scripts must be run from the virtual environment to run.
By default, running `panorama.py` from the command line will use the python interpreter
in the virtual environment installed `venv/bin/python`

## First run

### Create raw tables and views in the datalake catalog

Before running the commands that upload the data to the datalake, you should run the following command
to create the tables in the datalake. Failure to do that will cause errors when the upload routines tries
to update partitions in nonexistent tables.

```shell
panorama.py create-datalake-tables --all
```
By default, these tables are named `<base prefix>_raw_<table name>`

Then create the table views. Table views has exactly the same fields as the underlying raw table, 
but with numeric and date fields converted. Also strings of value _'NULL'_ are replaced with _null_ values.

```shell
panorama.py create-datalake-views --all
```

By default, these tables are named `<base prefix>_table_<table name>`

### Initial data upload
If there are tables with incremental updates enabled, you will have to make an initial run
to upload all partitions, even those that didn't change lately.
If the tables are large, this can consume lots of CPU resources and take a long time to complete.
Consider doing this out of business hours as this may impact the host performance.

_Note_: You can start doing incremental updates, and do the first run later.

To run a full update, use the `--force` option:

```shell
panorama.py extract-and-load --all --force
```

_Note_: the course structures table is not partitioned.

## Periodic updates

To run the script once an hour, add a line similar to the following one to the crontab:

```shell
0 * * * * cd panorama-elt && venv/bin/python panorama.py extract-and-load --all >> panorama.log
```

## Running in non-Open edX environments

The settings file is prepared for Open edX environments, for it has the most common
tables used and all the field information for the recommended setup.
It also includes the scripts to obtain the course structures from MongoDB,
which is specific to Open edX installation.

However, the MySQL script is suitable for any other installation using MySQL.

To configure Panorama EL for other MySQL installation follow these steps:
-Configure the mysql connection settings
- Set the `base_prefix` variable to a word that identifies your system (e.g.: wordpress, erp, forms, etc.)
- Run `panorama.py set-tables -t <table list>`, including a comma-separated list of the tables to extract. Do not leave spaces between the tables
- Run `panorama.py set-tables-fields {--all|--datasource=<datasource name>|--table=<table name>}` to retrieve each table fields from the database
- Optionally, set each table's datalake table name and/or table partitions

## Datalake directory structure

For each table (or for each field-based partition in each table when enabled), one file in csv format
will be generated and uploaded. The file will have the same name as the table, with '.csv' extension.

Each CSV file will be uploaded to the following directory structure:

```
s3://<bucket>/[<base prefix>/]<table name>/[<base partitions>/][field partitions/]<table name>.csv
```
Where:
- bucket: is the bucket name, configured in the `panorama_raw_data_bucket` setting
- base prefix: (optional) subdirectory to hold tables of a same kind of system. E.g.: openedx. 
It can receive files from multiple sources, as long as the table names are the same and share a field structure 
- table name: base location of the datalake table. All text files inside this directory must have exactly the same column structure
- base partitions: partitions common to a same installation, in Hive format. 
These are not based on fields in the data sources, but will appear as fileds in the datalake.
For multiple Open edX installations, the default is to use 'lms' as field name and the LMS_HOST as the value, which is the LMS url.
E.g.: 'lms=openedx.example.com'
- field partitions: (optional) For large tables, it's possible to split the datasource in multiple csv files.
The field will be removed from the csv file, but will appear as a partition field in the datalake.
In Open edX installations, the default setting is to partition courseware_studentmodule table by course_id.

## Advanced configuration

###Setting field partitions

To partition a table by a field, add a _partitions_ section to the table's element in _tables_, 
and list all the fields to partition under the _partition_fields_ key, as in the example:

```yaml
tables:
  - name: courseware_studentmodule
    datalake_table_name: courseware_studentmodule_raw
    partitions:
      partition_fields:
      - course_id
    fields:
      ...
```

The fields specified must be a field in the datasource table.
One different csv file will be generated for each distinct value in the specified column,
and will be placed in a subdirectory in Hive format like `<field name>=<value>`.

Be aware that this may create a huge number of subdirectories, so plan the table
fields partitions carefully.

### Incremental updates

It is possible to make incremental updates to the tables. To enable it, the table must be partitioned by field.

To enable incremental updates:
- set the field (which must be of _datetime_ format) with the timestamp information
- set the interval, using MySQL interval format. As a recommendation, use an interval
double to the query period. E.g., if you are querying the tables hourly, set a 2 hour interval

E.g.:
```yaml
tables:
  - name: courseware_studentmodule
    datalake_table_name: courseware_studentmodule_raw
    partitions:
      interval: 2 hour
      partition_fields:
      - course_id
      timestamp_field: modified
    fields:
      ...
```

####How it works?

First it will query all distinct values in the fields defined as partition fields that have records 
with timestamp in the last interval from the current time.
Then only the field based partitions detected will be updated. Note that the whole partition will be updated.

## Running inside a docker container

### Building the image

Panorama can be run from inside a docker container. There is a Dockerfile included for that purpose.
The container must bind mount the `/config` directory to hold the coniguration file.

To build the image, run:
```shell
docker build -t aulasneo/panorama-elt:$(python -c "from panorama_elt.__about__ import __version__; print(__version__)") -t aulasneo/panorama-elt:latest .
```

To bind mount the configuration directory and run the image to get a shell prompt, run:

```shell
docker run --name panorama -it -v $(pwd)/config:/config  panorama-elt:latest bash
```
Then, from inside the container's shell, you can run panorama commands indicating the location
of the settings file. E.g.:

```shell
:/# python panorama.py --settings=/config/panorama_openedx_settings.yaml test-connections
```

Note that the container must have access to the datasources and the datalake to work.

## License

This software is licenced under Apache 2.0 license. Please see LICENSE for more details.

## Contributing

Contributions are welcome! Please submit your PR and we will check it.
For questions, please send an email to <mailto:andres@aulasneo.com>.

