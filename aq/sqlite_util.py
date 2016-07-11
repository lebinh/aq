import json
import sqlite3
from datetime import datetime


def jsonify(obj):
    return json.dumps(obj, default=json_serialize)


sqlite3.register_adapter(dict, jsonify)
sqlite3.register_adapter(list, jsonify)


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
