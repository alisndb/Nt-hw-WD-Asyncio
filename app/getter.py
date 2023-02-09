import asyncio

from aiohttp import ClientSession
from more_itertools import chunked

from models import Base, People


class SWAPIGetter:
    def __init__(self, chunk_size, engine, session):
        self.chunk_size = chunk_size
        self.engine = engine
        self.Session = session

        self.url = 'https://swapi.dev/api/'
        self.columns = ['homeworld', 'films', 'species', 'starships', 'vehicles']
        self.to_del = ['url', 'created', 'edited']

    async def _chunked_async(self, async_iter, size):
        buffer = []

        while True:
            try:
                item = await async_iter.__anext__()
            except StopAsyncIteration:
                if buffer:
                    yield buffer
                break

            buffer.append(item)

            if len(buffer) == size:
                yield buffer
                buffer = []

    async def _get_from_url(self, url, detail):
        async with ClientSession() as session:
            response = await session.get(f'{url}')
            data = await response.json()
            obj = data.get(detail, None)

            return obj

    async def _get_details(self, data, column):
        detail = 'title' if column == 'films' else 'name'

        if column == 'homeworld':
            objects = await self._get_from_url(data[column], detail)
        else:
            coroutines = [self._get_from_url(url, detail) for url in data[column]]
            objects = await asyncio.gather(*coroutines)

        return objects

    async def _get_person(self, person_id, session):
        async with session.get(f'{self.url}people/{person_id}') as response:
            data = await response.json()

            if not data.get('detail', False):
                for column in self.to_del:
                    del data[column]

                data['id'] = person_id

                coroutines = [self._get_details(data, self.columns[i]) for i, _ in enumerate(self.columns)]
                objects = await asyncio.gather(*coroutines)

                for i, column in enumerate(self.columns):
                    if column == 'homeworld':
                        data[column] = objects[i]
                    else:
                        data[column] = ', '.join([obj for obj in objects[i] if obj is not None])

                return data

    async def _get_people(self, from_id, to_id):
        async with ClientSession() as session:
            for chunk in chunked(range(from_id, to_id), self.chunk_size):
                coroutines = [self._get_person(person_id, session) for person_id in chunk]
                results = await asyncio.gather(*coroutines)

                for item in results:
                    yield item

    async def _insert_people(self, people_chunk):
        async with self.Session() as session:
            session.add_all([People(**item) for item in people_chunk if item])
            await session.commit()

    async def _run(self, from_id, to_id):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.commit()

        async for chunk in self._chunked_async(self._get_people(from_id, to_id), self.chunk_size):
            asyncio.create_task(self._insert_people(chunk))

        tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}

        for task in tasks:
            await task

    def get_and_download_to_db(self, from_id, to_id):
        asyncio.run(self._run(from_id, to_id))
        print(f'Персонажи с id {from_id} по {to_id} (невкл.) успешно загружены в БД!')
