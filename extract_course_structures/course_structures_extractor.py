"""
Connects to a mongodb database with Open edX modulestore and creates a table with the structure of all active
versions of each course. The table will be saved as a csv file and uploaded to S3.

"""
import csv
import urllib.parse

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError, ParamValidationError
from pymongo import MongoClient

from panorama_logger.setup_logger import log

filename = 'course_structures.csv'


class CourseStructuresExtractor:

    def __init__(
            self,
            aws_access_key,
            aws_secret_access_key,
            panorama_s3_bucket,
            mongodb_username,
            mongodb_password,
            mongodb_host,
            mongodb_database,
            base_partitions,
    ):
        self.base_partitions = base_partitions
        self.panorama_s3_bucket = panorama_s3_bucket

        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key
        )

        self.s3_client = session.client('s3')

        if mongodb_username:
            connection_string = "mongodb://{}:{}@{}/{}".format(mongodb_username, mongodb_password, mongodb_host,
                                                               mongodb_database)
        else:
            connection_string = "mongodb://{}/{}".format(mongodb_host, mongodb_database)

        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        log.debug("Connecting to mongo using connection string '{}'".format(connection_string))
        client = MongoClient(connection_string)
        self.mongodb = client[mongodb_database]

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

    def get_active_versions(self) -> dict:
        """
        Returns a dict of courses in the active_versions collection in the form:
            { published_branch_id: { 'org': org, 'course': course, 'run': run }},...
            where published_branch_id is of type 'bson.objectid.ObjectId' as returned by pymongo's find()

        :return: dict of courses.
        """

        log.debug("Getting active versions")

        # Filter records without published-branch. This avoids loading e.g. libraries.
        cursor = self.mongodb.modulestore.active_versions.find({'versions.published-branch': {'$exists': True}})

        active_versions = dict()
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

    def extract_course_structures(self):

        # Get the active versions of each course
        active_versions = self.get_active_versions()
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

        with open(filename, 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(fields)

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

        # Upload to s3
        if self.panorama_s3_bucket:
            log.debug("Uploading to s3 bucket {}".format(self.panorama_s3_bucket))

            # Base prefix of the file in the S3 buckets. The first folder is the table name.
            # Next, the list of base partitions definitions for all tables in Hive format
            # The complete prefix will be the base prefix plus any specific partitions defined for the table
            base_prefix_list = ['course_structures']
            for key, value in self.base_partitions.items():
                base_prefix_list.append("{}={}".format(key, urllib.parse.quote(value)))
            base_prefix = "/".join(base_prefix_list)
            log.debug("Base prefix: {}".format(base_prefix))

            try:
                key = "/".join([base_prefix, 'course_structures.csv'])
                self.s3_client.upload_file(filename, self.panorama_s3_bucket, key)
                log.info("Uploaded to {}".format(key))

            except (ClientError, S3UploadFailedError, ParamValidationError) as e:
                log.error("Error uploading to S3: {}".format(e))

        else:
            log.warning("No panorama bucket specified, skipping s3 upload")

        log.debug("Process completed")
