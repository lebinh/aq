import json
import sqlite3
from datetime import datetime

from six import string_types


def connect(path):
    sqlite3.register_adapter(dict, jsonify)
    sqlite3.register_adapter(list, jsonify)
    db = sqlite3.connect(path)
    db.create_function('json_get', 2, json_get)
    return db


def jsonify(obj):
    return json.dumps(obj, default=json_serialize)


def json_serialize(obj):
    """
    Simple generic JSON serializer for common objects.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()

    if hasattr(obj, 'id'):
        return jsonify(obj.id)

    if hasattr(obj, 'name'):
        return jsonify(obj.name)

    raise TypeError('{} is not JSON serializable'.format(obj))


def json_get(serialized_object, field):
    """
    This emulates the HSTORE `->` get value operation.
    It get value from JSON serialized column by given key and return `null` if not present.
    Key can be either an integer for array index access or a string for object field access.

    :return: JSON serialized value of key in object
    """
    # return null if serialized_object is null or "serialized null"
    if serialized_object is None:
        return None
    obj = json.loads(serialized_object)
    if obj is None:
        return None

    if isinstance(field, int):
        # array index access
        res = obj[field] if 0 <= field < len(obj) else None
    else:
        # object field access
        res = obj.get(field)

    if not isinstance(res, (int, float, string_types)):
        res = json.dumps(res)

    return res


def create_table(db, schema_name, table_name, columns):
    """
    Create a table, schema_name.table_name, in given database with given list of column names.
    """
    table = '{}.{}'.format(schema_name, table_name) if schema_name else table_name
    db.execute('DROP TABLE IF EXISTS {}'.format(table))
    columns_list = ', '.join(columns)
    db.execute('CREATE TABLE {} ({})'.format(table, columns_list))


def insert_all(db, schema_name, table_name, columns, items):
    """
    Insert all item in given items list into the specified table, schema_name.table_name.
    """
    table = '{}.{}'.format(schema_name, table_name) if schema_name else table_name
    columns_list = ', '.join(columns)
    values_list = ', '.join(['?'] * len(columns))
    query = 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
        table=table, columns=columns_list, values=values_list)
    for item in items:
        values = [getattr(item, col) for col in columns]
        db.execute(query, values)
