
def top_level_distinct(table: str, column: str):
    return f'''
        SELECT
            {column}
        ,   count(*)
        FROM {table}
        GROUP BY 1
        ORDER BY count DESC
    '''


def top_level_stats(table: str, column: str):
    return f'''
        SELECT
            min({column})
        ,   max({column})
        ,   avg({column})
        ,   stddev({column})
        ,   count(*)
        ,   count_noise(*)
        FROM {table}
    '''


def bucketed_stats(table: str, column: str, bucket_size: int):
    return f'''
        SELECT
            bucket({column} by {bucket_size})
        ,   {bucket_size} as bucket_size
        ,   min({column})
        ,   max({column})
        ,   avg({column})
        ,   stddev({column})
        ,   count(*)
        ,   count_noise(*)
        FROM {table}
        GROUP BY 1
    '''
