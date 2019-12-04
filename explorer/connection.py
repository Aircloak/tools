import asyncio
import asyncpg
import logging


class AircloakConnection():
    def __init__(self, **kwargs):
        self.user = 'daniel-613C7ADF4535BB56DBCD'
        self.port = 9432
        self.host = 'attack.aircloak.com'
        self.database = 'gda_banking'

    async def connect(self):
        self.conn = await asyncpg.connect(user=self.user, host=self.host, port=self.port, database=self.database)

    async def close(self):
        await self.conn.close()

    async def run_query(self, query_fn, **query_args):
        query_str = query_fn(**query_args)
        logging.debug(f'Querying: {query_str}')
        result = await self.conn.fetch(query_str)
        return result
