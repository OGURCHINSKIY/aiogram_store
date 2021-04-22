from typing import Union, Dict, Optional, AnyStr
from aiogram.dispatcher.storage import BaseStorage
import json

try:
    import asyncpg
    from asyncpg.protocol.protocol import Record
    from asyncpg.pool import Pool
except ModuleNotFoundError as e:
    import warnings
    warnings.warn("Install asyncpg with `pip install asyncpg`")
    raise e


class PostgresStorage(BaseStorage):
    def __init__(self, host='localhost', port=5432, db_name='aiogram_fsm', table_name='aiogram_state', uri=None,
                 username='postgres', password='', **kwargs):
        if uri:
            self._uri = uri
        else:
            self._uri = f"postgres://{username}:{password}@{host}:{port}/{db_name}"
        self._table: str = table_name
        self._kwargs: Dict = kwargs
        self._db: Optional[Pool] = None

    async def get_db(self) -> Pool:
        if isinstance(self._db, Pool):
            return self._db
        self._db = await asyncpg.create_pool(dsn=self._uri, **self._kwargs)
        async with self._db.acquire() as con:
            async with con.transaction():
                await con.execute(
                    f"""CREATE TABLE IF NOT EXISTS {self._table} (
                            chat_id INT,
                            user_id INT,
                            data TEXT,
                            state TEXT,
                            bucket TEXT,
                        PRIMARY KEY (chat_id, user_id)
                    );"""
                )
        return self._db

    async def wait_closed(self):
        return True

    async def close(self):
        if self._db:
            await self._db.close()

    async def set_state(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                        state: Optional[AnyStr] = None):
        chat, user = self.check_address(chat=chat, user=user)
        db = await self.get_db()
        async with db.acquire() as con:
            async with con.transaction():
                await con.execute(
                    f"INSERT INTO {self._table}(chat_id,user_id,state) "
                    f"VALUES($1,$2,$3) ON CONFLICT (chat_id,user_id) DO UPDATE SET state = EXCLUDED.state",
                    chat, user, state
                )

    async def get_state(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                        default: Optional[str] = None) -> Optional[str]:
        chat, user = self.check_address(chat=chat, user=user)
        db = await self.get_db()
        async with db.acquire() as con:
            async with con.transaction():
                result = await con.fetchrow(
                    f"SELECT state FROM {self._table} WHERE chat_id = $1 AND user_id = $2;",
                    chat, user
                )
                if result:
                    return result.get("state")
                return default

    async def set_data(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                       data: Dict = None):
        chat, user = self.check_address(chat=chat, user=user)
        db = await self.get_db()

        async with db.acquire() as con:
            async with con.transaction():
                await con.execute(
                    f"INSERT INTO {self._table}(chat_id,user_id,data) "
                    f"VALUES($1,$2,$3) ON CONFLICT (chat_id,user_id) DO UPDATE SET data = EXCLUDED.data",
                    chat, user, "{}" if data is None else json.dumps(data)
                )

    async def get_data(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                       default: Optional[dict] = None) -> Dict:
        chat, user = self.check_address(chat=chat, user=user)
        db = await self.get_db()
        async with db.acquire() as con:
            async with con.transaction():
                result = await con.fetchrow(
                    f"SELECT data FROM {self._table} "
                    f"WHERE chat_id = $1 and user_id = $2;",
                    chat, user
                )
                if result:
                    return {} if result.get("data") is None else json.loads(result.get("data"))
                return default or {}

    async def update_data(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                          data: Dict = None, **kwargs):
        if data is None:
            data = {}
        temp_data = await self.get_data(chat=chat, user=user, default={})
        temp_data.update(data, **kwargs)
        await self.set_data(chat=chat, user=user, data=temp_data)

    def has_bucket(self):
        return True

    async def get_bucket(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                         default: Optional[dict] = None) -> Dict:
        chat, user = self.check_address(chat=chat, user=user)
        db = await self.get_db()
        async with db.acquire() as con:
            async with con.transaction():
                result = await con.fetchrow(
                    f"SELECT bucket FROM {self._table} "
                    f"WHERE chat_id=$1 and user_id=$2",
                    chat, user
                )
                if result:
                    return {} if result.get("bucket") is None else json.loads(result.get("bucket"))
                return default or {}

    async def set_bucket(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                         bucket: Dict = None):
        chat, user = self.check_address(chat=chat, user=user)
        db = await self.get_db()

        async with db.acquire() as con:
            async with con.transaction():
                await con.execute(
                    f"INSERT INTO {self._table}(chat_id,user_id,bucket) "
                    f"VALUES($1,$2,$3) ON CONFLICT (chat_id,user_id) DO UPDATE SET bucket = EXCLUDED.bucket",
                    chat, user, "{}" if bucket is None else json.dumps(bucket)
                )

    async def update_bucket(self, *, chat: Union[str, int, None] = None,
                            user: Union[str, int, None] = None,
                            bucket: Dict = None, **kwargs):
        if bucket is None:
            bucket = {}
        temp_bucket = await self.get_bucket(chat=chat, user=user)
        temp_bucket.update(bucket, **kwargs)
        await self.set_bucket(chat=chat, user=user, bucket=temp_bucket)

    async def reset_all(self, full=True):
        """
        Reset states in DB

        :param full: clean DB or clean only states
        :return:
        """
        db = await self.get_db()

        async with db.acquire() as con:
            async with con.transaction():
                await con.execute(
                    f"UPDATE {self._table} SET state = ''"
                )
                if full:
                    await con.execute(
                        f"UPDATE {self._table} SET data = '{{}}'"
                    )
                    await con.execute(
                        f"UPDATE {self._table} SET bucket = '{{}}'"
                    )