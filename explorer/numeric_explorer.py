from itertools import count
from collections import namedtuple
import logging

from . import queries
from . import bucket_util


class NumericColumnExplorer:
    def __init__(self, *, aircloak_connection, table, column):
        self.table = table
        self.column = column
        self.aircloak = aircloak_connection

        self._count = 0
        self._explored_buckets = set([])
        self._suppressed_counts = {}
        self._bucket_data = {}
        self._column_labels = []

        self._initialise()

    def _initialise(self):
        stats = self.aircloak.fetch(
            queries.top_level_stats(table=self.table, column=self.column))

        distincts = self.aircloak.fetch(
            queries.top_level_distinct(table=self.table, column=self.column))

        stats = stats['rows'][0]
        suppressed_count = count_suppressed(distincts['rows'], self.column)

        self._suppressed_counts[0] = suppressed_count
        self._count = stats['count']

        # If there are hardly any suppressed values, bucketing is futile.
        if self.suppressed_ratio(0) < 0.02:
            return

        data_range = stats['max'] - stats['min']

        # Choose buckets starting ~1/10th the full range,
        # (three bucket sizes is roughly equivalent to a factor of 10)
        # Drop the first two bucket sizes smaller than the range and retain every other
        # bucket size smaller than this. Take the first three of these for the initial
        # query.
        buckets = list(bucket_util.buckets_smaller_than(data_range))[2::2][:3]

        self.explore(set(buckets))

    def explore(self, buckets: set):
        to_explore = buckets - self._explored_buckets

        query_result = self.aircloak.fetch(queries.multi_bucket_stats(
            table=self.table, column=self.column, buckets=to_explore))

        logging.debug("Received query results, processing...")
        self._process_query_result(to_explore, query_result)
        logging.debug("... finished processing query results.")

        self._explored_buckets |= buckets

    def _process_query_result(self, bucket_sizes, query_result):
        '''Add the bucket size and bucket lower bound columns to each row
        '''
        if len(self._column_labels) == 0:
            self._column_labels = query_result['labels'][len(bucket_sizes):]

        suppressed = []
        for row in query_result['rows']:
            for (bs_label, bs) in [('bucket_' + str(bs), bs) for bs in bucket_sizes]:
                if row[bs_label] is not None:
                    # Note: row[bs_label] is the lower bound of the bucket, bs is the bucket size
                    self._bucket_data[(bs, row[bs_label])
                                      ] = row[len(bucket_sizes):]
                    break
            else:
                # HACK ALERT: No bucket_XXX column was filled. This can mean one of two things:
                # Either 1. The data was suppressed ("star rows')
                #     OR 2. All three columns are NULL (ie. no data to be bucketed)
                # The query has been filtered using 'WHERE {column} IS NOT NULL' so assume
                # that any NULL rows are in fact suppressed columns
                suppressed.append(row['count'])

        # Bigger buckets mean fewer suppressed rows, so we can assume that the smallest
        # number of suppressed rows match the largest bucket sizes.
        self._suppressed_counts.update(
            zip(sorted(bucket_sizes), sorted(suppressed, reverse=True)))

        pass

    def suppressed_ratio(self, bucket_size):
        return self._suppressed_counts[bucket_size] / self._count

    def extract_to_dataframe(self):
        # reshape the data and return args for pandas dataframe contructor
        indices = ['bucket_size', 'lower_bound']
        return {
            'data': [[*k, *v] for k, v in self._bucket_data.items()],
            'columns': [*indices, *self._column_labels],
            # 'index': indices,
        }


def count_suppressed(rows, col_name_or_index, count_col='count'):
    return next(r[count_col] for r in rows if r[col_name_or_index] == None)


if __name__ == "__main__":
    from .connection import AircloakConnection
    import logging
    logging.basicConfig(level=logging.DEBUG)

    e = NumericColumnExplorer(aircloak_connection=AircloakConnection(dbname='GiveMeSomeCredit'),
                              table='loans', column='MonthlyIncome')

    e.extract_to_dataframe()
