"""
Connects to a mongodb database with Open edX modulestore and creates a table with the structure of all active
versions of each course. The table will be saved as a csv file and uploaded to S3.

"""
import csv
import os

import bson
import pymysql
from pymongo import MongoClient
import pymongo.errors

from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake
from panorama_elt.panorama_logger.setup_logger import log

filename = 'course_structures.csv'


class CourseStructuresDatasource:

    def __init__(
            self,
            datalake: PanoramaDatalake,
            datasource_settings: dict,
    ):

        self.datalake = datalake
        mongodb_username = datasource_settings.get('mongodb_username')
        mongodb_password = datasource_settings.get('mongodb_password')
        mongodb_host = datasource_settings.get('mongodb_host', '127.0.0.1')
        self.mongodb_database = datasource_settings.get('mongodb_database', 'edxapp')

        # Create a connection using MongoClient. 
        log.debug("Connecting to mongo. Host: {} username: {} password: {}, db: {}".format(
            mongodb_host, mongodb_username, mongodb_password, self.mongodb_database))

        try:
            self.client = MongoClient(
                    host=mongodb_host, 
                    username=mongodb_username, 
                    password=mongodb_password, 
                    authSource=self.mongodb_database, 
                    readPreference='secondaryPreferred', 
                    directConnection=True
                    )
            self.mongodb = self.client[self.mongodb_database]
        except pymongo.errors.ConfigurationError as e:
            log.error(e)
            exit(1)

        # With split mongo, the active versions are stored in a mysql table
        if datasource_settings.get('mysql_host'):
            log.info("MySQL host defined. Using MySQL to get active versions")
            self.use_split_mongo_active_versions = True
            mysql_username = datasource_settings.get('mysql_username', 'root')
            mysql_password = datasource_settings.get('mysql_password')
            mysql_port = datasource_settings.get('mysql_port', 3306)
            mysql_host = datasource_settings.get('mysql_host', '127.0.0.1')
            mysql_database = datasource_settings.get('mysql_database', 'edxapp')

            try:
                conn = pymysql.connect(
                    host=mysql_host,
                    port=mysql_port,
                    user=mysql_username,
                    passwd=mysql_password,
                    db=mysql_database
                )
                self.cur = conn.cursor()

            except pymysql.err.OperationalError as e:
                log.error(e)
                exit(1)
        else:
            log.info("MySQL host not defined. Using MongoDB to get active versions")
            self.use_split_mongo_active_versions = False


    def test_connections(self) -> dict:
        """
        Performs connections test
        :return: dict with test results
        """

        try:
            modulestore = self.mongodb.get_collection('modulestore')
            if modulestore is not None:
                results = {'MongoDB': 'OK'}
            else:
                results = {'MongoDB': 'Modulestore collection not found in db {}'.format(self.mongodb_database)}
        except pymongo.errors.ServerSelectionTimeoutError as e:
            results = {'MongoDB': e.args[0]}
        except pymongo.errors.ConfigurationError as e:
            results = {'MongoDB': e}
        except pymongo.errors.OperationFailure as e:
            results = {'MongoDB': e}
        return results

    def get_tables(self) -> list:
        """
        Returns the list of tables available in the database
        :return: list of sheet names
        """
        return ['course_structures']

    def get_fields(self, table: str, force_query: bool = False) -> list:
        """
        Returns a list of fields of the table in the database using an existing mysql cursor

        :param table: table name
        :param force_query: (optional) if set to True, will query the db even if there is a definition set
        :return: list[str] of fields
        """

        fields = [
            'module_location',
            'course_id',
            'organization',
            'course_code',
            'course_edition',
            'parent',
            'block_type',
            'block_id',
            'display_name',
            'course_name',
            'chapter',
            'sequential',
            'vertical',
            'library',
            'component'
        ]

        fields_and_types = [{"name": f, "type": "varchar"} for f in fields]

        return fields_and_types

    def get_structures(self, id_list: dict) -> list:
        """
        Returns a list of records in the structures collections whose id are keys of id_list
        :param id_list: dict. The keys of the dict are used to filter the structures by id. The keys must be of type
            'bson.objectid.ObjectId', as returned by pymongo's find
        :return: list of structures
        """
        log.debug("Getting {} blocks".format(len(id_list)))
        cursor = self.mongodb.modulestore.structures.find({'_id': {'$in': list(id_list.keys())}})

        structs = []
        for record in cursor:
            structs.append(record)

        return structs

    def get_active_versions_mongodb(self):
        """
        Returns a dict of courses in the active_versions collection in the form:
            { published_branch_id: { 'org': org, 'course': course, 'run': run }},...
            where published_branch_id is of type 'bson.objectid.ObjectId' as returned by pymongo's find()

            Old version that queries MongoDB

        :return: dict of courses.
        """

        log.debug("Getting active versions")

        # Filter records without published-branch. This avoids loading e.g. libraries.
        cursor = self.mongodb.modulestore.active_versions.find({'versions.published-branch': {'$exists': True}})

        active_versions = dict()

        try:
            for record in cursor:
                published_branch = record.get('versions').get('published-branch')

                if published_branch:
                    active_versions[published_branch] = {
                        'org': record['org'],
                        'course': record['course'],
                        'run': record['run']
                    }
                else:
                    log.error("No published_branch information found in record {}".format(record))
        except pymongo.errors.OperationFailure as e:
            log.error("Error accessing MongoDB: {}".format(e))
            return None

        log.info("{} active versions found".format(len(active_versions)))
        return active_versions

    def get_active_versions(self):
        """
        Returns a dict of courses in the active_versions collection in the form:
            { published_branch_id: { 'org': org, 'course': course, 'run': run }},...
            where published_branch_id is of type 'bson.objectid.ObjectId' as returned by pymongo's find()

                https://github.com/openedx/edx-platform/commit/6c856680994583953906155290f1c9e37530b733
                Now active versions come from MySQL table split_modulestore_django_splitmodulestorecourseindex, which
                has the following fields:
                id
                objectid
                course_id
                org
                draft_version
                published_version
                library_version
                wiki_slug
                base_store (currently "mongodb")
                edited_on (date)
                last_update (date)
                edited_by_id (int): user id

                objectid, draft_version and published_version are the string id of the mongodb object.

        :return: dict of courses.
        """

        field_list = [
            "published_version",
            "course_id",
        ]

        log.debug("Getting active versions from mysql")
        query = 'select {fields} from {table}'.format(
            fields=",".join(field_list),
            table='split_modulestore_django_splitmodulestorecourseindex;',
        )

        log.debug("Querying mysql rows: {}".format(query))

        self.cur.execute(query)
        rows = self.cur.fetchall()

        active_versions = dict()

        for record in rows[1:]:
            published_branch = bson.objectid.ObjectId(record[0])
            course_id = record[1]

            active_versions[published_branch] = {
                'org': course_id[10:].split('+')[0],
                'course': course_id.split('+')[1],
                'run': course_id.split('+')[2]
            }

        log.info("{} active versions found".format(len(active_versions)))
        return active_versions

    def get_blocks(self, course_structures: list, active_versions: dict) -> dict:
        """
        Extracts the blocks of a course structure, in the form:
        { block_id: {
            'course_block_id': id of course in the structures' dict.
                Use as index in the active_versions to get course info,
            'block_type': block_type,
            'display_name': display_name,
            'children': children
            },
            ...
        }
        :param active_versions: course information from get_active_versions
        :param course_structures: list of course structure obtained from the get_structures function
        :return: dict of blocks
        """

        log.debug("Getting blocks for {} active versions".format(len(active_versions)))

        blocks = dict()

        # Course structures is a list with one item per course, with the structure of the current active version.
        # There should be one and only one item in active_versions for each one in course_structures
        for structure in course_structures:
            course_block_id = structure.get('_id')
            active_version = active_versions.get(course_block_id)

            # The active_version dict only has information of the course id. This info is not in the structure element
            organization = active_version.get('org')
            course_code = active_version.get('course')
            course_edition = active_version.get('run')

            course_id = 'course-v1:{}+{}+{}'.format(organization, course_code, course_edition)

            # The course structure dict has an element 'blocks' with the internal structure of the course
            for block in structure.get('blocks'):
                block_id = block.get('block_id')
                block_type = block.get('block_type')

                # One and only one item in the blocks list is the root of the course structure tree
                if block_type == 'course':
                    module_location = course_id
                else:
                    module_location = 'block-v1:{}+{}+{}+type@{}+block@{}'.format(
                        organization, course_code, course_edition, block_type, block_id
                    )

                # The display name of the block and the list of children is inside a dict called 'fields'
                fields = block.get('fields')
                display_name = fields.get('display_name')
                children = fields.get('children')

                # We build a dict for each component of the course with all the information
                # that will be exported as a table

                log.debug("Creating block {} with name {} and {} children".format(
                    module_location, display_name, len(children)))
                blocks[module_location] = dict(
                    organization=organization,
                    course_code=course_code,
                    course_edition=course_edition,
                    course_id=course_id,
                    block_type=block_type,
                    block_id=block_id,
                    display_name=display_name,
                    children=children
                )

            # After checking all the blocks, there should be one for the course root
            if course_id not in blocks:
                log.error("No course block found in course {}".format(course_block_id))
            else:
                # Starting with the root block of the course, we fill the tree with the parent branch information
                self.fill_parents(blocks=blocks, block_id=course_id)

        log.info("{} blocks found".format(len(blocks)))

        return blocks

    def fill_parents(self, blocks: dict, block_id: str, parent_block_id: str = None) -> None:
        """
        Fills with the parents a block and calls itself recursively on each child
        :param parent_block_id:
        :param block_id:
        :param blocks:
        :return:
        """

        log.debug("Filling parents of {}".format(block_id))

        block = blocks.get(block_id)
        if not block:
            log.error("No block id {} found".format(block_id))
            return

        # Course blocks don't have a parent. All the rest do.
        if parent_block_id:
            parent_block = blocks.get(parent_block_id)
            parent_block_type = parent_block.get('block_type')

            block['parent'] = parent_block_id
            block[parent_block_type] = parent_block.get('display_name')

            log.debug("Parent id: {}".format(parent_block_id))
            # Fills with the upper structures
            for b_type in ['course', 'chapter', 'sequential', 'vertical', 'library_content']:
                if b_type in parent_block:
                    log.debug("Parent {}: {}".format(b_type, parent_block.get(b_type)))
                    block[b_type] = parent_block.get(b_type)

        children = block.get('children')

        if children:
            log.debug("{} children found".format(len(children)))
            for child in children:
                child_module_location = 'block-v1:{}+{}+{}+type@{}+block@{}'.format(
                    block.get('organization'),
                    block.get('course_code'),
                    block.get('course_edition'),
                    child[0],  # child block type
                    child[1]  # child block id
                )
                self.fill_parents(blocks=blocks, block_id=child_module_location, parent_block_id=block_id)
        else:
            log.debug("No children found")

            # If it is a component block, set the display name as component name
            if block.get('block_type') not in ['course', 'chapter', 'sequential', 'vertical', 'library_content']:
                block['component_name'] = block.get('display_name')

    def extract_and_load(self, selected_tables: str = None, force: bool = False):

        if selected_tables and 'course_structures' not in selected_tables:
            return

        # Get the active versions of each course
        # If there is a mysql host configured in the settings, then use split mongo to get the active versions (Nutmeg+)
        # Otherwise, use the old mongodb

        if self.use_split_mongo_active_versions:
            log.info("Using split mongo to get active versions")
            active_versions = self.get_active_versions()
        else:
            log.info("Using mongodb to get active versions")
            active_versions = self.get_active_versions_mongodb()
        if not active_versions:
            log.warning("No active versions found")
            return
        log.debug("Found {} active versions".format(len(active_versions)))

        # Get the structures of all the active versions
        structures = self.get_structures(active_versions)
        log.debug("Found {} structures".format(len(structures)))

        if len(active_versions) != len(structures):
            # There should be as many active versions as course structures
            log.warning(
                "Found {} active versions but {} course structures".format(len(active_versions), len(structures)))

        # Build a dict with one item per block, including courses, chapters, sequentials, verticals and components
        blocks = self.get_blocks(course_structures=structures, active_versions=active_versions)
        log.debug("{} blocks found".format(len(blocks)))

        # Save the blocks as a csv table
        log.debug("Writing to CSV")

        fields = self.get_fields(table="course_structures")

        with open(filename, 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([f.get('name') for f in fields])

            for module_location, block_data in blocks.items():
                row = [
                    module_location,
                    block_data.get('course_id'),
                    block_data.get('organization'),
                    block_data.get('course_code'),
                    block_data.get('course_edition'),
                    block_data.get('parent'),
                    block_data.get('block_type'),
                    block_data.get('block_id'),
                    block_data.get('display_name'),
                    block_data.get('course'),
                    block_data.get('chapter'),
                    block_data.get('sequential'),
                    block_data.get('vertical'),
                    block_data.get('library_content'),
                    block_data.get('component_name'),
                ]
                csv_writer.writerow(row)

        self.datalake.upload_table_from_file(filename=filename, table='course_structures', update_partitions=True)

        os.remove(filename)

        log.debug("Process completed")
