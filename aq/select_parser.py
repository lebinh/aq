# Adapted from select_parser.py by Paul McGuire
# http://pyparsing.wikispaces.com/file/view/select_parser.py/158651233/select_parser.py
#
# a simple SELECT statement parser, taken from SQLite's SELECT statement
# definition at http://www.sqlite.org/lang_select.html
#
from pyparsing import *

ParserElement.enablePackrat()


def no_suppress_delimited_list(expression, delimiter=','):
    return expression + ZeroOrMore(delimiter + expression)


def concat(tokens):
    return ''.join(tokens)


def build_json_get_expr(terms):
    if len(terms) < 2:
        raise ValueError('Not enough terms')
    if len(terms) == 2:
        return 'json_get({}, {})'.format(terms[0], terms[1])
    return 'json_get({}, {})'.format(build_json_get_expr(terms[:-1]), terms[-1])


def replace_json_get(tokens):
    terms = [t for t in tokens[0] if t != '->']
    return build_json_get_expr(terms)


# keywords
(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER,
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET) = map(CaselessKeyword, """UNION, ALL, AND, INTERSECT,
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED,
 NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET
 """.replace(",", "").split())

(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE,
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE,
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",", "").split())

keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER,
                      CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE,
                      GROUP, BY,
                      HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN,
                      ELSE, END, CASE, WHEN, THEN, EXISTS,
                      COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
                      CURRENT_TIMESTAMP))

identifier = ~keyword + Word(alphas, alphanums + "_")
collation_name = identifier.copy()
column_name = identifier.copy()
column_alias = identifier.copy()
table_name = identifier.copy()
table_alias = identifier.copy()
index_name = identifier.copy()
function_name = identifier.copy()
parameter_name = identifier.copy()
database_name = identifier.copy()

# expression
LPAR, RPAR, COMMA = map(Word, "(),")
select_stmt = Forward()
expr = Forward()

integer = Regex(r"[+-]?\d+")
numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
string_literal = QuotedString("'", unquoteResults=False)
blob_literal = Combine(oneOf("x X") + "'" + Word(hexnums) + "'")
literal_value = (numeric_literal | string_literal | blob_literal |
                 NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP)
bind_parameter = (
    Word("?", nums) |
    Combine(oneOf(": @ $") + parameter_name)
)
type_name = oneOf("TEXT REAL INTEGER BLOB NULL")

expr_term = (
    CAST + LPAR + expr + AS + type_name + RPAR |
    EXISTS + LPAR + select_stmt + RPAR |
    function_name + LPAR + Optional(no_suppress_delimited_list(expr) | "*") + RPAR |
    literal_value |
    bind_parameter |
    (database_name + "." + table_name + "." + identifier) |
    (table_name + "." + identifier) |
    identifier
).setParseAction(concat)

UNARY, BINARY, TERNARY = 1, 2, 3

expr << operatorPrecedence(expr_term,
                           [
                               ('->', BINARY, opAssoc.LEFT, replace_json_get),
                               (oneOf('- + ~') | NOT, UNARY, opAssoc.LEFT),
                               (ISNULL | NOTNULL | (NOT + NULL), UNARY, opAssoc.LEFT),
                               (IS + NOT, BINARY, opAssoc.LEFT),
                               ('||', BINARY, opAssoc.LEFT),
                               (oneOf('* / %'), BINARY, opAssoc.LEFT),
                               (oneOf('+ -'), BINARY, opAssoc.LEFT),
                               (oneOf('<< >> & |'), BINARY, opAssoc.LEFT),
                               (oneOf('< <= > >='), BINARY, opAssoc.LEFT),
                               (
                                   oneOf('= == != <>') | IS | IN | LIKE | GLOB | MATCH | REGEXP,
                                   BINARY,
                                   opAssoc.LEFT),
                               ((BETWEEN, AND), TERNARY, opAssoc.LEFT),
                           ])

compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)

ordering_term = expr + Optional(COLLATE + collation_name) + Optional(ASC | DESC)

join_constraint = Optional(
    ON + expr | USING + LPAR + Group(no_suppress_delimited_list(column_name)) + RPAR)

join_op = COMMA | (Optional(NATURAL) + Optional(INNER | CROSS | LEFT + OUTER | LEFT | OUTER) + JOIN)

table_reference = (
    (database_name("database") + "." + table_name("table") | table_name("table")) +
    Optional(Optional(AS) + table_alias("alias"))
).setResultsName("table_ids", listAllMatches=True)

join_source = Forward()
single_source = (
    table_reference +
    Optional(INDEXED + BY + index_name | NOT + INDEXED) |
    (LPAR + select_stmt + RPAR + Optional(Optional(AS) + table_alias)) |
    (LPAR + join_source + RPAR))

join_source << single_source + ZeroOrMore(join_op + single_source + join_constraint)

result_column = table_name + "." + "*" | (expr + Optional(Optional(AS) + column_alias)) | "*"
select_core = (
    SELECT + Optional(DISTINCT | ALL) + Group(no_suppress_delimited_list(result_column)) +
    Optional(FROM + join_source) +
    Optional(WHERE + expr) +
    Optional(GROUP + BY + Group(no_suppress_delimited_list(ordering_term)) +
             Optional(HAVING + expr)))

select_stmt << (select_core + ZeroOrMore(compound_operator + select_core) +
                Optional(ORDER + BY + Group(no_suppress_delimited_list(ordering_term))) +
                Optional(
                    LIMIT + (integer | integer + OFFSET + integer | integer + COMMA + integer)))
