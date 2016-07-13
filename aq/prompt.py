from __future__ import unicode_literals

import itertools
import os

from prompt_toolkit import AbortAction, CommandLineInterface
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completion, Completer
from prompt_toolkit.history import FileHistory
from prompt_toolkit.layout.lexers import PygmentsLexer
from prompt_toolkit.shortcuts import create_prompt_application, create_eventloop
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
        application = create_prompt_application(
            message='> ',
            lexer=PygmentsLexer(SqlLexer),
            history=FileHistory(os.path.expanduser('~/.aq/history')),
            completer=AqCompleter(schemas=engine.available_schemas, tables=engine.available_tables),
            auto_suggest=AutoSuggestFromHistory(),
            validator=QueryValidator(parser),
            on_abort=AbortAction.RETRY,
        )
        loop = create_eventloop()
        self.cli = CommandLineInterface(application=application, eventloop=loop)
        self.patch_context = self.cli.patch_stdout_context()

    def prompt(self):
        with self.patch_context:
            return self.cli.run(reset_current_buffer=True).text

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

    starters = ['SELECT']

    def __init__(self, schemas=None, tables=None):
        self.schemas = schemas if schemas else []
        self.tables = tables if tables else []
        self.tables_and_schemas = itertools.chain(self.schemas, self.tables)
        self.all_completions = itertools.chain(self.keywords, self.functions,
                                               self.tables_and_schemas)

    def get_completions(self, document, complete_event):
        start_of_current_word = document.find_start_of_previous_word(1)
        current_word = document.text_before_cursor[start_of_current_word:].strip().lower()

        start_of_previous_2_words = document.find_start_of_previous_word(2)
        previous_word = document.text_before_cursor[
                        start_of_previous_2_words:start_of_current_word].strip().lower()

        if document.text_before_cursor[-1:].isspace():
            previous_word = current_word
            current_word = ''

        candidates = self.get_completion_candidates(current_word, previous_word, document)
        for candidate in candidates:
            if candidate.lower().startswith(current_word):
                yield Completion(candidate, -len(current_word))

    def get_completion_candidates(self, current_word, previous_word, document):
        if not previous_word:
            return self.starters

        if current_word == ',' or previous_word in [',', 'from', 'join']:
            # we delay the materialize of table list until here for faster startup time
            if not isinstance(self.tables_and_schemas, list):
                self.tables_and_schemas = list(self.tables_and_schemas)
            return self.tables_and_schemas

        if not isinstance(self.all_completions, list):
            # ensure we materialized the table list first
            if not isinstance(self.tables_and_schemas, list):
                self.tables_and_schemas = list(self.tables_and_schemas)
            self.all_completions = list(self.all_completions)
        return self.all_completions


class QueryValidator(Validator):
    def __init__(self, parser):
        self.parser = parser

    def validate(self, document):
        try:
            self.parser.parse_query(document.text)
        except QueryParsingError as e:
            raise ValidationError(message='Invalid SQL query. {}'.format(e),
                                  cursor_position=document.cursor_position)
