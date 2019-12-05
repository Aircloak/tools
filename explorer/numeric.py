import logging

from .buckets import Buckets
from .connection import AircloakConnection

from . import queries


class Explorer:
    def __init__(self, *, dbname):
        self.stats = {}
        self.ac = AircloakConnection(dbname=dbname)

    def explore_numeric_col(self, *, table: str, column: str):
        stats = self.ac.fetch(
            queries.top_level_stats(table=table, column=column))

        distincts = self.ac.fetch(queries.top_level_distinct(
            table=table, column=column))

        stats = stats['rows'][0]
        suppressed_count = count_suppressed(distincts['rows'], column)

        suppressed_ratio = suppressed_count / stats['count']

        if suppressed_ratio > 0.05:
            # too many supressed values, lets drill down
            value_range = stats['max'] - stats['min']

            bucket_size = Buckets().estimate_bucket_size(
                value_range, stats['count'])

            self.stats[(table, column)] = self.ac.fetch(
                queries.bucketed_stats(table=table, column=column, bucket_size=bucket_size))

            # TODO: check quality of returned buckets and, if necessary launch more queries with adjusted bucket size.

    def histogram(self, *, table, column):
        stats = self.stats[(table, column)]['rows']
        return [row['bucket'] for row in stats[1:]], [row['count'] for row in stats[1:]]

    def to_dataframe(self, *, table, column):
        stats = self.stats[(table, column)]
        return {
            'data': stats['rows'],
            'columns': stats['labels'],
            'index': None,
        }

    def __del__(self):
        self.ac.close()


def count_suppressed(rows, col, count_col='count'):
    return next(r[count_col] for r in rows if r[col] == None)


if __name__ == "__main__":
    e = Explorer(dbname='gda_banking')
    e.explore_numeric_col(table='loans', column='amount')
    x, y = e.histogram(table='loans', column='amount')
    print(x, y)
