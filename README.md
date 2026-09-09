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
- Python 3.12
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
will be generated and uploaded. The file will have the same name as the table, with '.csv' extension,
unless `datalake_s3_table` is configured for that table.

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

### Datalake table names and S3 table names

By default, the source table name is used both for the S3 table directory and for the generated datalake
table/view names. For example, with `base_prefix: moodle` and source table `mdl_course`, the default S3
location is:

```text
s3://<bucket>/moodle/mdl_course/<partitions>/mdl_course.csv
```

Use `datalake_s3_table` to keep the source table name for extraction while using a different table name in
S3 and in the default Athena table/view names:

```yaml
tables:
  - name: mdl_course
    datalake_s3_table: course
    fields:
      ...
```

This stores data under:

```text
s3://<bucket>/moodle/course/<partitions>/course.csv
```

With the same example, the default Athena names become `moodle_raw_course` and `moodle_table_course`.
You can still override those explicitly with `datalake_table_name` and `datalake_table_view`.

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
The image includes the Panorama ELT code and Python dependencies only. Keep the settings file outside the
image and bind mount it at runtime so settings changes do not require rebuilding the image.

To build the image, run:
```shell
docker build -t aulasneo/panorama-elt:$(python3 -c "from panorama_elt.__about__ import __version__; print(__version__)") -t aulasneo/panorama-elt:latest .
```

### Runtime directories

The container should usually use these bind mounts:

- `/config`: settings files, for example `/config/panorama_openedx_settings.yaml`
- `/work`: temporary CSV work files generated during extraction

The application writes temporary CSV files to the current working directory and deletes them after upload.
Set the container working directory to `/work` to control where those files are created.

For example, if the host settings file is in `/etc/panorama` and temporary files should be written under
`/var/lib/panorama/work`, use:

```shell
docker run --rm \
  -v /etc/panorama:/config:ro \
  -v /var/lib/panorama/work:/work \
  -w /work \
  aulasneo/panorama-elt:latest \
  --settings /config/panorama_openedx_settings.yaml test-connections
```

Mount `/config` as read-only for regular extraction jobs. Mount it as read-write only for commands that
update the settings file, such as `set-tables` and `set-tables-fields`.

Note that the container must have network access to the datasources and the datalake to work.

### Periodic extract-and-load from cron

After creating the datalake tables and views, schedule `extract-and-load` from the host crontab.
This example runs every hour, keeps settings outside the image, and writes logs on the host:

```cron
0 * * * * docker run --rm -v /etc/panorama:/config:ro -v /var/lib/panorama/work:/work -w /work aulasneo/panorama-elt:latest --settings /config/panorama_openedx_settings.yaml extract-and-load --all >> /var/log/panorama/panorama.log 2>&1
```

For the first full upload of partitioned tables, run the same container command once with `--force`:

```shell
docker run --rm --name panorama-elt \
  -v /etc/panorama:/config:ro \
  -v /var/lib/panorama/work:/work \
  -w /work \
  aulasneo/panorama-elt:latest \
  --settings /config/panorama_openedx_settings.yaml extract-and-load --all --force
```

### One-time setup and maintenance commands

Run one-time commands from the shell with the same image and mounted settings file:

```shell
docker run --rm -it \
  -v /etc/panorama:/config:ro \
  -v /var/lib/panorama/work:/work \
  -w /work \
  aulasneo/panorama-elt:latest \
  --settings /config/panorama_openedx_settings.yaml create-datalake-tables --all
```

```shell
docker run --rm -it \
  -v /etc/panorama:/config:ro \
  -v /var/lib/panorama/work:/work \
  -w /work \
  aulasneo/panorama-elt:latest \
  --settings /config/panorama_openedx_settings.yaml create-datalake-views --all
```

For commands that update the settings file, mount `/config` as read-write:

```shell
docker run --rm -it \
  -v /etc/panorama:/config:rw \
  -v /var/lib/panorama/work:/work \
  -w /work \
  aulasneo/panorama-elt:latest \
  --settings /config/panorama_openedx_settings.yaml set-tables-fields --all
```

## License

This software is licenced under Apache 2.0 license. Please see LICENSE for more details.

## Contributing

Contributions are welcome! Please submit your PR and we will check it.
For questions, please send an email to <mailto:andres@aulasneo.com>.
# Tutor 22 / Open edX Verawood validation

This upgrade keeps the package version and existing CSV columns and S3 consumer
paths unchanged. The target platform is `release/verawood.1`; Tutor 22.0.2 uses
MySQL 8.4.11 and MongoDB 7.0.39. The extractor runs separately on Python 3.12 and
does not share the LMS dependency constraints. Do not reuse the old Python 3.8
`venv`.

The refreshed runtime lock resolves Boto3/Botocore 1.43.89, PyMongo 4.18.0,
PyMySQL 1.2.0 and cryptography 50.0.1. These are independently tested extractor
dependencies, not the Verawood LMS compatibility baseline. `PyMySQL[rsa]`
explicitly supplies the cryptography dependency for MySQL SHA-2 authentication.
Direct imports retain `botocore`; `jmespath`, `s3transfer`, `six`, `urllib3` and
`python-dateutil` remain in the lock as dependencies of the AWS SDK rather than
unnecessary direct requirements.

[PyMongo's supported servers](https://pypi.org/project/pymongo/) include MongoDB
7.0. [PyMySQL](https://github.com/PyMySQL/PyMySQL) supports MySQL 8.x and documents
the `rsa` extra for `sha256_password`/`caching_sha2_password`. This establishes
driver protocol support; it does not substitute for testing the migrated schema,
authentication configuration or a real replica set. Mongo topology discovery is
enabled by default; `mongodb_direct_connection: true` is an explicit option for
single-endpoint development tunnels. Connection diagnostics issue a real ping
and read, rather than merely obtaining a collection handle.

[cryptography 50.0.0](https://cryptography.io/en/stable/changelog/#v50-0-0)
fixes the PKCS7 decryption oracle; 50.0.1 refreshes bundled OpenSSL to 4.0.2.
The extractor does not decrypt PKCS7 messages or use the affected ChaCha20 APIs;
its cryptography use is the MySQL driver's RSA authentication. The security floor
is retained despite that limited exposure. A runtime lock audit on 2026-09-08
reported no known vulnerabilities.

Scheduled runs now return failure for incomplete table exports, failed Athena
submissions and failed/cancelled/timed-out partition updates. Independent tables
and datasources finish before aggregated failures are raised. Athena waits for
at most 20 status polls with one second between pending polls; SDK requests have
10 second connect/30 second read timeouts and at most three attempts. Missing
Athena database/workgroup configuration fails required partition updates.
Already uploaded objects are not rolled back when another table fails; a retry
replaces the same consumer filenames. This is not an atomic datalake snapshot.

MySQL exports fetch at most 1,000 rows at a time through an unbuffered cursor;
Excel worksheets stream in read-only mode. Distinct MySQL partition keys and
course structure graphs still reside in memory. Temporary exports are isolated
and removed on success or failure, and database clients close after extraction.
Missing published course structures, roots, children or problem definitions fail
the course snapshot before upload. Explicit zero problem weights are preserved.
An installation with no published courses retains the previous no-upload behavior;
stale course objects and deleted/empty incremental partitions require a separate
retention policy, not an implicit deletion during this upgrade.

SQL constant/partition data are bound parameters (including apostrophes, zero,
false, empty strings and NULL). Configured identifiers reject SQL expressions.
NULL partition keys use `__HIVE_DEFAULT_PARTITION__`; numeric keys use their
string representation. Existing string partition URL escaping, S3 table overrides,
filenames, CSV quote/backslash behavior and escaped CR/LF remain unchanged.
Do not remove the existing CSV backslash transformation until Athena consumer
comparisons authorize a format migration.

Local validation in a fresh environment:

```sh
python3.12 -m venv /tmp/panorama-extractor-check
/tmp/panorama-extractor-check/bin/pip install -r requirements-test.txt pip-audit
/tmp/panorama-extractor-check/bin/pip install --no-deps .
/tmp/panorama-extractor-check/bin/pip check
/tmp/panorama-extractor-check/bin/pytest
/tmp/panorama-extractor-check/bin/flake8 panorama_elt tests
/tmp/panorama-extractor-check/bin/pylint panorama_elt
/tmp/panorama-extractor-check/bin/pip-audit -r requirements.txt
```

The Docker image installs the reviewed lock before the package and runs
`panorama-elt`; `python panorama.py` remains supported for existing cron recipes.
SQL view assets remain at `/app/openedx_views` in the standalone image.
The sdist includes the Dockerfile, runtime lock, compatibility script, SQL views
and example configurations so that building from the source artifact retains
these assets. Local validation on 2026-09-08 passed 138 tests with 93.86% coverage,
flake8, pylint (9.96/10), wheel/sdist build, installed entry-point checks and
`pip check`. Tests include a real XLSX read and PyMySQL RSA encryption/decryption
with the upgraded cryptography library; database and AWS calls remain mocked.
Before staging acceptance, compare every configured table/column (especially
optional enterprise tables) to the migrated MySQL database; test real MySQL SHA-2
credentials, replica-set discovery, unpublished/published courses, failed Athena
queries and S3 retry behavior. Compare full/incremental counts, Unicode, multiline
CSV and exact partition paths with existing consumers. These external service
checks cannot be established by the local mocked suite.
