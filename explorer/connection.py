import psycopg2
from psycopg2.extras import DictCursor, LoggingConnection
import logging
from collections import namedtuple

from . import queries as q

ColumnInfo = namedtuple('ColumnInfo', 'type isolator id')
TableInfo = namedtuple('TableInfo', 'type')


class AircloakConnection():
    def __init__(self, *, dbname):
        self.user = 'daniel-613C7ADF4535BB56DBCD'
        self.port = 9432
        self.host = 'attack.aircloak.com'
        self.dbname = dbname

        logging.debug(
            f'Connecting to Aircloak: user={self.user}, host={self.host}, port={self.port}, dbname={self.dbname}')

        self.conn = psycopg2.connect(
            user=self.user, host=self.host, port=self.port, dbname=self.dbname,
            connection_factory=LoggingConnection)

        self.conn.initialize(logging.getLogger(self.__class__.__name__))

        self._table_info = index_and_wrap(TableInfo, self.fetch(
            q.table_info(), cursor_factory=None)['rows'])
        self._column_info = {}

    def column_info(self, table, column):
        if table not in self._column_info:
            self._column_info[table] = index_and_wrap(ColumnInfo, self.fetch(q.column_info(
                table=table), cursor_factory=None)['rows'])
        return self._column_info[table][column]

    def table_info(self, table):
        return self._table_info[table]

    def close(self):
        self.conn.close()

    def fetch(self, query, cursor_factory=DictCursor):
        logging.debug(f'Sending query: {query.as_string(self.conn)}')
        with self.conn.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(query)
            result = {
                'rows': cur.fetchall(),
                'labels': [col.name for col in cur.description]
            }

        return result


def index_and_wrap(Wrapper, rows):
    return dict([(row[0], Wrapper(*row[1:])) for row in rows])
