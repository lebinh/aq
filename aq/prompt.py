from __future__ import unicode_literals

import os

from prompt_toolkit import prompt, AbortAction
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completion, Completer
from prompt_toolkit.history import FileHistory
from prompt_toolkit.layout.lexers import PygmentsLexer
from prompt_toolkit.validation import Validator, ValidationError
from pygments.lexers.sql import SqlLexer

from aq import util
from aq.errors import QueryParsingError
from aq.logger import get_logger

LOGGER = get_logger()


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


class AqCompleter(Completer):
    keywords = '''UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING,
        NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT,
        FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET CAST, ISNULL, NOTNULL,
        NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB,
        REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP
        '''.replace(',', '').split()

    functions = '''avg, count, max, min, sum, json_get'''.replace(',', '').split()

    def __init__(self, schemas=None, tables=None):
        self.schemas = list(schemas) if schemas else []
        self.tables = list(tables) if tables else []

    def get_completions(self, document, complete_event):
        start_of_current_word = document.find_start_of_previous_word(1)
        current_word = document.text_before_cursor[start_of_current_word:].strip().lower()

        start_of_previous_2_words = document.find_start_of_previous_word(2)
        previous_word = document.text_before_cursor[
                        start_of_previous_2_words:start_of_current_word].strip().lower()

        if document.text_before_cursor[-1:].isspace():
            previous_word = current_word
            current_word = ''

        if not previous_word:
            candidates = ['SELECT']
        elif current_word == ',' or previous_word in [',', 'from', 'join']:
            candidates = self.tables + self.schemas
        else:
            candidates = self.keywords + self.functions + self.tables + self.schemas

        for candidate in candidates:
            if candidate.lower().startswith(current_word):
                yield Completion(candidate, -len(current_word))


class QueryValidator(Validator):
    def __init__(self, parser):
        self.parser = parser

    def validate(self, document):
        try:
            self.parser.parse_query(document.text)
        except QueryParsingError as e:
            raise ValidationError(message='Invalid SQL query. {}'.format(e),
                                  cursor_position=document.cursor_position)
