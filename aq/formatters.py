from tabulate import tabulate


class TableFormatter(object):
    def __init__(self, options=None):
        self.options = options if options else {}

    @staticmethod
    def format(columns, rows):
        return tabulate(rows, headers=columns, tablefmt='psql', missingval='NULL')
