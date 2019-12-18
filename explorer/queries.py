import logging
from psycopg2 import sql


def column_info(*, table: str):
    return sql.SQL('''
        SHOW COLUMNS FROM {table}
    ''').format(table=sql.Identifier(table))


def table_info():
    return sql.SQL('''
        SHOW TABLES
    ''')


def top_level_distinct(*, table: str, column: str):
    return sql.SQL('''
        SELECT
            {column}
        ,   count(*)
        FROM {table}
        WHERE {column} IS NOT NULL
        GROUP BY 1
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


def multi_bucket_stats(*, table: str, column: str, buckets: list):
    buckets_sql = sql.Composed(
        sql.SQL('bucket({column} by {bucket_size}) as {label}').format(
            bucket_size=sql.Literal(bucket_size),
            label=sql.Identifier(f'bucket_{bucket_size}'),
            column=sql.Identifier(column))
        for bucket_size in buckets).join('\n, ')

    return sql.SQL('''
    SELECT
        {buckets}
    ,   count(*)
    ,   count_noise(*)
    ,   min({column})
    ,   max({column})
    ,   avg({column})
    FROM {table}
    WHERE {column} IS NOT NULL
    GROUP BY GROUPING SETS ({sets})
    ''').format(
        buckets=buckets_sql,
        column=sql.Identifier(column),
        table=sql.Identifier(table),
        sets=sql.SQL(', ').join(sql.Literal(i+1) for i in range(len(buckets)))
    )
