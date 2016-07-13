from unittest import TestCase

from aq.sqlite_util import connect, create_table, insert_all


class TestSqliteUtil(TestCase):
    def test_dict_adapter(self):
        with connect(':memory:') as conn:
            conn.execute('CREATE TABLE foo (foo)')
            conn.execute('INSERT INTO foo (foo) VALUES (?)', ({'bar': 'blah'},))
            values = conn.execute('SELECT * FROM foo').fetchone()
            self.assertEqual(len(values), 1)
            self.assertEqual(values[0], '{"bar": "blah"}')

    def test_create_table(self):
        with connect(':memory:') as conn:
            create_table(conn, None, 'foo', ('col1', 'col2'))
            tables = conn.execute("PRAGMA table_info(\'foo\')").fetchall()
            self.assertEqual(len(tables), 2)
            self.assertEqual(tables[0][1], 'col1')
            self.assertEqual(tables[1][1], 'col2')

    def test_insert_all(self):
        class Foo(object):
            def __init__(self, c1, c2):
                self.c1 = c1
                self.c2 = c2

        columns = ('c1', 'c2')
        values = (Foo(1, 2), Foo(3, 4))
        with connect(':memory:') as conn:
            create_table(conn, None, 'foo', columns)
            insert_all(conn, None, 'foo', columns, values)
            rows = conn.execute('SELECT * FROM foo').fetchall()
            self.assertTrue((1, 2) in rows, '(1, 2) in rows')
            self.assertTrue((3, 4) in rows, '(3, 4) in rows')

    def test_json_get_field(self):
        with connect(':memory:') as conn:
            json_obj = '{"foo": "bar"}'
            query = "select json_get('{0}', 'foo')".format(json_obj)
            self.assertEqual(conn.execute(query).fetchone()[0], 'bar')

    def test_json_get_index(self):
        with connect(':memory:') as conn:
            json_obj = '[1, 2, 3]'
            query = "select json_get('{0}', 1)".format(json_obj)
            self.assertEqual(conn.execute(query).fetchone()[0], 2)

    def test_json_get_field_nested(self):
        with connect(':memory:') as conn:
            json_obj = '{"foo": {"bar": "blah"}}'
            query = "select json_get('{0}', 'foo')".format(json_obj)
            self.assertEqual(conn.execute(query).fetchone()[0], '{"bar": "blah"}')
            query = "select json_get(json_get('{0}', 'foo'), 'bar')".format(json_obj)
            self.assertEqual(conn.execute(query).fetchone()[0], 'blah')

    def test_json_get_field_of_null(self):
        with connect(':memory:') as conn:
            query = "select json_get(NULL, 'foo')"
            self.assertEqual(conn.execute(query).fetchone()[0], None)

    def test_json_get_field_of_serialized_null(self):
        with connect(':memory:') as conn:
            json_obj = 'null'
            query = "select json_get('{0}', 'foo')".format(json_obj)
            self.assertEqual(conn.execute(query).fetchone()[0], None)
