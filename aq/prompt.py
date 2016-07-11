from __future__ import unicode_literals

import os

from prompt_toolkit import prompt, AbortAction
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.contrib.completers import WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.layout.lexers import PygmentsLexer
from prompt_toolkit.validation import Validator, ValidationError
from pygments.lexers.sql import SqlLexer

from aq import util
from aq.errors import QueryParsingError


class AqPrompt(object):
    def __init__(self, parser, engine, options=None):
        self.parser = parser
        self.engine = engine
        self.options = options if options is not None else {}
        util.ensure_data_dir_exists()
        history_file = os.path.expanduser('~/.aq/history')
        self.history = FileHistory(history_file)
        self.lexer = PygmentsLexer(SqlLexer)
        self.completer = AqCompleter(schemas=engine.available_schemas,
                                     tables=engine.available_tables)
        self.auto_suggest = AutoSuggestFromHistory()
        self.validator = QueryValidator(self.parser)

    def prompt(self):
        return prompt('> ',
                      lexer=self.lexer,
                      completer=self.completer,
                      history=self.history,
                      auto_suggest=self.auto_suggest,
                      validator=self.validator,
                      on_abort=AbortAction.RETRY)

    def update_with_result(self, query_metadata):
        # TODO
        pass


class AqCompleter(WordCompleter):
    keywords = '''UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING,
        NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT,
        FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET CAST, ISNULL, NOTNULL,
        NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB,
        REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP
        '''.replace(',', '').split()

    functions = '''avg, count, max, min, sum, json_get'''.replace(',', '').split()

    def __init__(self, schemas=None, tables=None):
        all_completions = set(self.keywords + self.functions)
        schemas = schemas if schemas else []
        all_completions.update(schemas)
        tables = tables if tables else []
        all_completions.update(tables)
        super(AqCompleter, self).__init__(all_completions, ignore_case=True)


class QueryValidator(Validator):
    def __init__(self, parser):
        self.parser = parser

    def validate(self, document):
        try:
            self.parser.parse_query(document.text)
        except QueryParsingError as e:
            raise ValidationError(message='Invalid SQL query. {}'.format(e),
                                  cursor_position=document.cursor_position)
