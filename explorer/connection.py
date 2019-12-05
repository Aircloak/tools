import psycopg2
from psycopg2.extras import DictCursor
import logging


class AircloakConnection():
    def __init__(self, *, dbname):
        self.user = 'daniel-613C7ADF4535BB56DBCD'
        self.port = 9432
        self.host = 'attack.aircloak.com'
        self.dbname = dbname

        logging.debug(
            f'Connecting to Aircloak: user={self.user}, host={self.host}, port={self.port}, dbname={self.dbname}')

        self.conn = psycopg2.connect(
            user=self.user, host=self.host, port=self.port, dbname=self.dbname, cursor_factory=DictCursor)

    def close(self):
        self.conn.close()

    def fetch(self, query):
        logging.debug(f'Sending query: {query.as_string(self.conn)}')
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = {
                'rows': cur.fetchall(),
                'labels': [col.name for col in cur.description]
            }

        return result
