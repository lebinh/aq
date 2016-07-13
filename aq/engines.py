import itertools
import os.path
import pprint
import sqlite3
import time
from collections import defaultdict
from multiprocessing.dummy import Pool

import boto3
from boto3.resources.collection import CollectionManager

from aq import logger, util, sqlite_util
from aq.errors import QueryError

LOGGER = logger.get_logger()


class BotoSqliteEngine(object):
    def __init__(self, options=None):
        self.options = options if options else {}
        self.debug = options.get('--debug', False)

        self.table_cache_ttl = int(options.get('--table-cache-ttl', 300))
        self.last_refresh_time = defaultdict(int)

        self.boto3_session = boto3.Session()
        # dash (-) is not allowed in database name so we use underscore (_) instead in region name
        # throughout this module region name will *always* use underscore
        self.default_region = self.boto3_session.region_name.replace('-', '_')
        self.db = self.init_db()
        # attach the default region too
        self.attach_region(self.default_region)

    def init_db(self):
        util.ensure_data_dir_exists()
        db_path = '~/.aq/{}.db'.format(self.default_region)
        absolute_path = os.path.expanduser(db_path)
        return sqlite_util.connect(absolute_path)

    def execute(self, query, metadata):
        LOGGER.info('Executing query: %s', query)
        self.load_tables(query, metadata)
        try:
            cursor = self.db.execute(query)
        except sqlite3.OperationalError as e:
            raise QueryError(str(e))
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        return columns, rows

    def load_tables(self, query, meta):
        """
        Load necessary resources tables into db to execute given query.
        """
        for table in meta.tables:
            self.load_table(table)

    def load_table(self, table):
        """
        Load resources as specified by given table into our db.
        """
        region = table.database if table.database else self.default_region
        resource_name, collection_name = table.table.split('_', 1)
        # we use underscore "_" instead of dash "-" for region name but boto3 need dash
        boto_region_name = region.replace('_', '-') if region else None
        resource = boto3.resource(resource_name, region_name=boto_region_name)
        if not hasattr(resource, collection_name):
            raise QueryError(
                'Unknown collection <{}> of resource <{}>'.format(collection_name, resource_name))

        self.attach_region(region)
        self.refresh_table(region, table.table, resource, getattr(resource, collection_name))

    def attach_region(self, region):
        if not self.is_attached_region(region):
            LOGGER.info('Attaching new database for region: %s', region)
            region_db_file_path = '~/.aq/{}.db'.format(region)
            absolute_path = os.path.expanduser(region_db_file_path)
            self.db.execute('ATTACH DATABASE ? AS ?', (absolute_path, region))

    def is_attached_region(self, region):
        databases = self.db.execute('PRAGMA database_list')
        db_names = (db[1] for db in databases)
        return region in db_names

    def refresh_table(self, schema_name, table_name, resource, collection):
        if not self.is_fresh_enough(schema_name, table_name):
            LOGGER.info('Refreshing table: %s.%s', schema_name, table_name)
            columns = get_columns_list(resource, collection)
            LOGGER.info('Columns list: %s', columns)
            with self.db:
                sqlite_util.create_table(self.db, schema_name, table_name, columns)
                items = collection.all()
                # special treatment for tags field
                items = [convert_tags_to_dict(item) for item in items]
                sqlite_util.insert_all(self.db, schema_name, table_name, columns, items)
                self.last_refresh_time[(schema_name, table_name)] = time.time()

    def is_fresh_enough(self, schema_name, table_name):
        last_refresh = self.last_refresh_time[(schema_name, table_name)]
        age = time.time() - last_refresh
        return age < self.table_cache_ttl

    @property
    def available_schemas(self):
        # we want to return all regions if possible so ec2 is a good enough guess
        regions = self.boto3_session.get_available_regions(service_name='ec2')
        return [r.replace('-', '_') for r in regions]

    @property
    def available_tables(self):
        resources = self.boto3_session.get_available_resources()
        tables = Pool(processes=len(resources)).map(self._get_table_names_for_resource, resources)
        return itertools.chain.from_iterable(tables)

    def _get_table_names_for_resource(self, resource_name):
        resource = self.boto3_session.resource(resource_name)
        for attr in dir(resource):
            if isinstance(getattr(resource, attr), CollectionManager):
                yield '{}_{}'.format(resource_name, attr)


class ObjectProxy(object):
    def __init__(self, source, **replaced_fields):
        self.source = source
        self.replaced_fields = replaced_fields

    def __getattr__(self, item):
        if item in self.replaced_fields:
            return self.replaced_fields[item]
        return getattr(self.source, item)


def convert_tags_to_dict(item):
    """
    Convert AWS inconvenient tags model of a list of {"Key": <key>, "Value": <value>} pairs
    to a dict of {<key>: <value>} for easier querying.

    This returns a proxied object over given item to return a different tags format as the tags
    attribute is read-only and we cannot modify it directly.
    """
    if hasattr(item, 'tags'):
        tags = item.tags
        if isinstance(tags, list):
            tags_dict = {}
            for kv_dict in tags:
                if isinstance(kv_dict, dict) and 'Key' in kv_dict and 'Value' in kv_dict:
                    tags_dict[kv_dict['Key']] = kv_dict['Value']
            return ObjectProxy(item, tags=tags_dict)
    return item


def get_resource_model_attributes(resource, collection):
    service_model = resource.meta.client.meta.service_model
    resource_model = get_resource_model(collection)
    shape_name = resource_model.shape
    shape = service_model.shape_for(shape_name)
    return resource_model.get_attributes(shape)


def get_columns_list(resource, collection):
    resource_model = get_resource_model(collection)
    LOGGER.debug('Resource model: %s', resource_model)

    identifiers = sorted(i.name for i in resource_model.identifiers)
    LOGGER.debug('Model identifiers: %s', identifiers)

    attributes = get_resource_model_attributes(resource, collection)
    LOGGER.debug('Model attributes: %s', pprint.pformat(attributes))

    return list(itertools.chain(identifiers, attributes))


def get_resource_model(collection):
    return collection._model.resource.model
