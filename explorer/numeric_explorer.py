from itertools import count
from collections import namedtuple, defaultdict
import logging

from . import queries
from . import bucket_util
from . import bucket_tree as bt


class NumericColumnExplorer:
    def __init__(self, *, aircloak_connection, table, column):
        self.table = table
        self.column = column
        self.aircloak = aircloak_connection

        column_type = self.aircloak.column_info(table, column).type
        assert column_type in [
            'integer', 'real'], f'NumericColumnExplorer can only deal with numeric columns but {self.column} is of type {column_type}'

        self._top_level_stats = self.aircloak.fetch(
            queries.top_level_stats(table=self.table, column=self.column))['rows'][0]

        self._distincts = self.aircloak.fetch(
            queries.top_level_distinct(table=self.table, column=self.column))

        self._suppressed_count = next(
            r['count'] for r in self._distincts['rows'] if r[self.column] == None)

        self._suppressed_ratio = self._suppressed_count / \
            self._top_level_stats['count']

        self._data_range = self._top_level_stats['max'] - \
            self._top_level_stats['min']

        self._bucket_tree = bt.BucketTree(
            self._data_range, self._distincts, self._top_level_stats['count'], self._suppressed_count)

        self._column_labels = []

    def explore(self, depth=3):
        to_explore = self._bucket_tree.next_levels(depth)

        query_result = self.aircloak.fetch(queries.multi_bucket_stats(
            table=self.table, column=self.column, buckets=to_explore))

        logging.debug("Received query results, processing...")

        self._process_query_result(to_explore, query_result)

        logging.debug("... finished processing query results.")

    def _process_query_result(self, bucket_sizes, query_result):
        '''Add the bucket size and bucket lower bound columns to each row
        '''
        if len(self._column_labels) == 0:
            self._column_labels = query_result['labels'][len(bucket_sizes):]

        suppressed = []
        bucket_data = defaultdict(list)
        for row in query_result['rows']:
            for (bs_label, bs) in [('bucket_' + str(bs), bs) for bs in bucket_sizes]:
                if row[bs_label] is not None:
                    # Note: row[bs_label] is the lower bound of the bucket, bs is the bucket size
                    bucket_data[bs].append(
                        bt.Bucket(bs, row[bs_label], row[len(bucket_sizes):]))
                    break
            else:
                # HACK ALERT: No bucket_XXX column was filled. This can mean one of two things:
                # Either 1. The data was suppressed ("star rows')
                #     OR 2. All three columns are NULL (ie. no data to be bucketed)
                # The query has been filtered using 'WHERE {column} IS NOT NULL' so assume
                # that any NULL rows are in fact STAR (suppressed) columns
                suppressed.append(row['count'])

        # Bigger buckets mean fewer suppressed rows, so we can assume that the smallest
        # number of suppressed rows match the largest bucket sizes.
        for bs, sup in zip(sorted(bucket_sizes, reverse=True), sorted(suppressed)):
            self._bucket_tree.insert_query_result(
                bs, bucket_data[bs], suppressed=sup)

    def extract_to_dataframe(self, bucket_sizes=[]):
        # reshape the data and return args for pandas dataframe contructor

        return {
            'data': self._bucket_tree.get_buckets(bucket_sizes),
            'columns': ['bucket_size', 'lower_bound', *self._column_labels],
        }


if __name__ == "__main__":
    from .connection import AircloakConnection
    import logging
    logging.basicConfig(level=logging.DEBUG)

    e = NumericColumnExplorer(aircloak_connection=AircloakConnection(dbname='GiveMeSomeCredit'),
                              table='loans', column='MonthlyIncome')

    e.explore()

    e.extract_to_dataframe()
