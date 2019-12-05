import logging
from psycopg2 import sql


def top_level_distinct(*, table: str, column: str):
    return sql.SQL('''
        SELECT
            {column}
        ,   count(*)
        FROM {table}
        GROUP BY 1
        ORDER BY 2 DESC
    ''').format(table=sql.Identifier(table), column=sql.Identifier(column))


def top_level_stats(*, table: str, column: str):
    return sql.SQL('''
        SELECT
            min({column})
        ,   max({column})
        ,   avg({column})
        ,   count(*)
        ,   count_noise(*)
        FROM {table}
    ''').format(table=sql.Identifier(table), column=sql.Identifier(column))


def bucketed_stats(*, table: str, column: str, bucket_size: int):
    return sql.SQL('''
        SELECT
            bucket({column} by {bucket_size})
        ,   {bucket_size} as bucket_size
        ,   min({column})
        ,   max({column})
        ,   avg({column})
        ,   count(*)
        ,   count_noise(*)
        FROM {table}
        GROUP BY 1
    ''').format(table=sql.Identifier(table), column=sql.Identifier(column), bucket_size=sql.Literal(bucket_size))
