from unittest import TestCase

from aq.errors import QueryParsingError
from aq.parsers import SelectParser, TableId


class TestSelectParser(TestCase):
    parser = SelectParser({})

    def test_parse_query_simplest(self):
        query, meta = self.parser.parse_query('select * from foo')
        self.assertEqual(query, 'SELECT * FROM foo')
        table = TableId(None, 'foo', None)
        self.assertListEqual(meta.tables, [table])

    def test_parse_query_db(self):
        query, meta = self.parser.parse_query('select * from foo.bar')
        self.assertEqual(query, 'SELECT * FROM foo . bar')
        table = TableId('foo', 'bar', None)
        self.assertListEqual(meta.tables, [table])

    def test_parse_query_table_alias(self):
        query, meta = self.parser.parse_query('select * from foo.bar a')
        self.assertEqual(query, 'SELECT * FROM foo . bar a')
        table = TableId('foo', 'bar', 'a')
        self.assertListEqual(meta.tables, [table])

    def test_parse_query_table_as_alias(self):
        query, meta = self.parser.parse_query('select * from foo.bar as a')
        self.assertEqual(query, 'SELECT * FROM foo . bar AS a')
        table = TableId('foo', 'bar', 'a')
        self.assertListEqual(meta.tables, [table])

    def test_parse_query_join_simple(self):
        query, meta = self.parser.parse_query('select * from foo, bar')
        self.assertEqual(query, 'SELECT * FROM foo , bar')
        table1 = TableId(None, 'foo', None)
        table2 = TableId(None, 'bar', None)
        self.assertListEqual(meta.tables, [table1, table2])

    def test_parse_query_join_expr(self):
        query, meta = self.parser.parse_query('select * from foo join bar on foo.a = bar.b')
        self.assertEqual(query, 'SELECT * FROM foo JOIN bar ON foo.a = bar.b')
        table1 = TableId(None, 'foo', None)
        table2 = TableId(None, 'bar', None)
        self.assertListEqual(meta.tables, [table1, table2])

    def test_parse_query_join_table_with_using(self):
        query, meta = self.parser.parse_query('select * from foo join foo.bar using (name)')
        self.assertEqual(query, 'SELECT * FROM foo JOIN foo . bar USING ( name )')
        table1 = TableId(None, 'foo', None)
        table2 = TableId('foo', 'bar', None)
        self.assertListEqual(meta.tables, [table1, table2])

    def test_parse_query_sub_select(self):
        query, meta = self.parser.parse_query('select * from (select * from foo)')
        self.assertEqual(query, 'SELECT * FROM ( SELECT * FROM foo )')
        table = TableId(None, 'foo', None)
        self.assertListEqual(meta.tables, [table])

    def test_parse_query_sub_select_and_join(self):
        query, meta = self.parser.parse_query('select * from (select * from foo.bar) left join blah')
        self.assertEqual(query, 'SELECT * FROM ( SELECT * FROM foo . bar ) LEFT JOIN blah')
        table1 = TableId('foo', 'bar', None)
        table2 = TableId(None, 'blah', None)
        self.assertListEqual(meta.tables, [table1, table2])

    def test_parse_query_invalid(self):
        try:
            self.parser.parse_query('foo')
        except QueryParsingError:
            pass
        else:
            self.fail()

    def test_parse_query_expand_json_get(self):
        query, _ = self.parser.parse_query("select foo->1")
        self.assertEqual(query, 'SELECT json_get(foo, 1)')

        query, _ = self.parser.parse_query("select foo.bar -> 'blah'")
        self.assertEqual(query, "SELECT json_get(foo.bar, 'blah')")

        query, _ = self.parser.parse_query("select foo->bar->blah")
        self.assertEqual(query, 'SELECT json_get(json_get(foo, bar), blah)')

    def test_parse_query_expand_not_json_get(self):
        query, _ = self.parser.parse_query("select * from foo where x = 'bar -> 1'")
        self.assertEqual(query, "SELECT * FROM foo WHERE x = 'bar -> 1'")

        query, _ = self.parser.parse_query("select * from foo where x -> 'bar -> 1'")
        self.assertEqual(query, "SELECT * FROM foo WHERE json_get(x, 'bar -> 1')")
