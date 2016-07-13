"""aq - Query AWS resources with SQL

Usage:
    aq [options]
    aq [options] <query>

Sample queries:
    aq "select tags->'Name' from ec2_instances"
    aq "select count(*) from us_west_1.ec2_instances"

Options:
    --table-cache-ttl=<seconds>  number of seconds to cache the tables
                                 before we update them from AWS again [default: 300]
    -v, --verbose  enable verbose logging
    --debug  enable debug mode
"""
from __future__ import print_function

import traceback
from collections import namedtuple

from docopt import docopt

from aq.engines import BotoSqliteEngine
from aq.formatters import TableFormatter
from aq.logger import initialize_logger
from aq.parsers import SelectParser
from aq.prompt import AqPrompt

__version__ = '0.1.0'

QueryResult = namedtuple('QueryResult', ('parsed_query', 'query_metadata', 'columns', 'rows'))


def get_engine(options):
    return BotoSqliteEngine(options)


def get_parser(options):
    return SelectParser(options)


def get_formatter(options):
    return TableFormatter(options)


def get_prompt(parser, engine, options):
    return AqPrompt(parser, engine, options)


def main():
    args = docopt(__doc__)
    initialize_logger(verbose=args['--verbose'], debug=args['--debug'])

    parser = get_parser(args)
    engine = get_engine(args)
    formatter = get_formatter(args)

    if args['<query>']:
        query = args['<query>']
        res = execute_query(engine, formatter, parser, query)
        print(formatter.format(res.columns, res.rows))
    else:
        repl = get_prompt(parser, engine, args)
        while True:
            try:
                query = repl.prompt()
                res = execute_query(engine, formatter, parser, query)
                print(formatter.format(res.columns, res.rows))
                repl.update_with_result(res.query_metadata)
            except EOFError:
                break
            except:
                traceback.print_exc()


def execute_query(engine, formatter, parser, query):
    parsed_query, metadata = parser.parse_query(query)
    columns, rows = engine.execute(parsed_query, metadata)
    return QueryResult(parsed_query=parsed_query, query_metadata=metadata,
                       columns=columns, rows=rows)
