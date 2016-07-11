from __future__ import unicode_literals

import os

from prompt_toolkit import prompt
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.history import FileHistory
from prompt_toolkit.layout.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer

from aq import util


class AqPrompt(object):
    def __init__(self, options=None):
        self.options = options if options is not None else {}
        util.ensure_data_dir_exists()
        history_file = os.path.expanduser('~/.aq/history')
        self.history = FileHistory(history_file)
        self.lexer = PygmentsLexer(SqlLexer)
        self.completer = AqCompleter()
        self.auto_suggest = AutoSuggestFromHistory()

    def prompt(self):
        return prompt('> ',
                      lexer=self.lexer,
                      completer=self.completer,
                      history=self.history,
                      auto_suggest=self.auto_suggest)


class AqCompleter(Completer):
    def get_completions(self, document, complete_event):
        yield Completion('SELECT', start_position=0)
