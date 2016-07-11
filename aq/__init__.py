"""AWS Query
Usage:
    aq [options]
    aq [options] <query>

Sample queries:
    aq 'select name from ec2_instances'

Options:
    --debug  enable debug mode
    -v, --verbose  enable verbose logging
"""
from __future__ import print_function

import traceback

from docopt import docopt

from aq.engines import BotoSqliteEngine
from aq.formatters import TableFormatter
from aq.logger import initialize_logger
from aq.parsers import SelectParser
from aq.prompt import AqPrompt


def get_engine(options):
    return BotoSqliteEngine(options)


def get_parser(options):
    return SelectParser(options)


def get_formatter(options):
    return TableFormatter(options)


def get_prompt(options):
    return AqPrompt(options)


def main():
    args = docopt(__doc__)
    initialize_logger(verbose=args['--verbose'], debug=args['--debug'])

    parser = get_parser(args)
    engine = get_engine(args)
    formatter = get_formatter(args)

    if args['<query>']:
        query = args['<query>']
        execute_query(engine, formatter, parser, query)
    else:
        repl = get_prompt(args)
        while True:
            try:
                query = repl.prompt()
                execute_query(engine, formatter, parser, query)
            except EOFError:
                break
            except KeyboardInterrupt:
                break
            except:
                traceback.print_exc()


def execute_query(engine, formatter, parser, query):
    parsed_query, metadata = parser.parse_query(query)
    columns, rows = engine.execute(parsed_query, metadata)
    print(formatter.format(columns, rows))
