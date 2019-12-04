import asyncio
import asyncpg
import logging

from buckets import Buckets
from connection import AircloakConnection

import queries

# assume for now that we want at least 20 values per bucket (valid?)
# also that the smallest useful bucket size is at 1/100 of the total range
MAX_BUCKETS = 100
MIN_BUCKET_SIZE = 20


async def explore_numeric_col(table: str, column: str, max_buckets=MAX_BUCKETS, min_bucket_size=MIN_BUCKET_SIZE):
    ac = AircloakConnection()
    await ac.connect()

    stats = await ac.run_query(queries.top_level_stats, table=table, column=column)
    distincts = await ac.run_query(queries.top_level_distinct, table=table, column=column)

    stats = stats[0]
    count_total = stats['count']
    suppresed_count = count_suppressed(distincts, 0)

    suppressed_ratio = suppresed_count / count_total

    if suppressed_ratio > 0.05:
        # too many supressed values, lets drill down
        value_range = stats['max'] - stats['min']

        # Estimate lower and upper bounds for the bucket size
        bs_lower_bound = value_range / max_buckets
        bs_upper_bound = value_range / (count_total / min_bucket_size)

        bucket_size = Buckets().estimate_bucket_size(bs_lower_bound, bs_upper_bound)

        bucketed_stats = await ac.run_query(queries.bucketed_stats, table=table, column=column, bucket_size=bucket_size)

        # TODO: check quality of returned buckets and, if necessary launch more queries.

    await ac.close()

    return bucketed_stats


def count_suppressed(rows, col, count_col='count'):
    return next(r[count_col] for r in rows if r[col] == None)


def run_exp(exp):
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    return loop.run_until_complete(exp)


# if __name__ == "__main__":
#     async def main():
#         logging.basicConfig(level=logging.DEBUG)
#         ac = AircloakConnection()
#         await ac.connect()
#         values = await ac.run_query('bucketed', table='loans', column='amount', bucket_size=10000)
#         logging.debug(values)

#     loop = asyncio.get_event_loop()
#     loop.set_debug()
#     loop.run_until_complete(main())
