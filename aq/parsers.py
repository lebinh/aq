import collections
from collections import namedtuple

from six import string_types

from aq.errors import QueryParsingError
from aq.select_parser import select_stmt, ParseException

TableId = namedtuple('TableId', ('database', 'table', 'alias'))
QueryMetadata = namedtuple('QueryMetadata', ('tables',))


class SelectParser(object):
    def __init__(self, options):
        self.options = options

    @staticmethod
    def parse_query(query):
        try:
            parse_result = select_stmt.parseString(query, parseAll=True)
        except ParseException as e:
            raise QueryParsingError(e)

        tables = [parse_table_id(tid) for tid in parse_result.table_ids]
        parsed_query = concat(parse_result)
        return parsed_query, QueryMetadata(tables=tables)


def parse_table_id(table_id):
    database = table_id.database[0] if table_id.database else None
    table = table_id.table[0] if table_id.table else None
    alias = table_id.alias[0] if table_id.alias else None
    return TableId(database, table, alias)


def flatten(nested_list):
    for item in nested_list:
        if isinstance(item, collections.Iterable) and not isinstance(item, string_types):
            for nested_item in flatten(item):
                yield nested_item
        else:
            yield item


def concat(tokens):
    return ' '.join(flatten(tokens))
