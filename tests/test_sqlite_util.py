from unittest import TestCase

from aq.sqlite_util import sqlite3, create_table, insert_all


class TestSqliteUtil(TestCase):
    def test_dict_adapter(self):
        with sqlite3.connect(':memory:') as conn:
            conn.execute('CREATE TABLE foo (foo)')
            conn.execute('INSERT INTO foo (foo) VALUES (?)', ({'bar': 'blah'},))
            values = conn.execute('SELECT * FROM foo').fetchone()
            assert values[0] == '{"bar": "blah"}'

    def test_create_table(self):
        with sqlite3.connect(':memory:') as conn:
            create_table(conn, None, 'foo', ('col1', 'col2'))
            tables = conn.execute("PRAGMA table_info(\'foo\')").fetchall()
            assert len(tables) == 2
            assert tables[0][1] == 'col1'
            assert tables[1][1] == 'col2'

    def test_insert_all(self):
        class Foo(object):
            def __init__(self, c1, c2):
                self.c1 = c1
                self.c2 = c2

        columns = ('c1', 'c2')
        values = (Foo(1, 2), Foo(3, 4))
        with sqlite3.connect(':memory:') as conn:
            create_table(conn, None, 'foo', columns)
            insert_all(conn, None, 'foo', columns, values)
            rows = conn.execute('SELECT * FROM foo').fetchall()
            assert (1, 2) in rows
            assert (3, 4) in rows
