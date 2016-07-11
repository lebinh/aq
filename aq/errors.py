class AQError(Exception):
    pass


class QueryError(AQError):
    pass


class QueryParsingError(AQError):
    pass
