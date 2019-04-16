import asyncio
import asyncpg
import json
import aiohttp
import os
import time


posgresql_user = 'kola'
posgresql_db = 'mtest' #должна быть создана база в Postresql
posgresql_dump = 'dump'
DATABASE_URI = f'postgresql://kola:test@localhost:5432/{posgresql_db}'
psgr_import_dump = f'cat {posgresql_dump} | psql -h localhost -U {posgresql_user} {posgresql_db}'
ElasticSearch_URI = 'http://localhost:9200/post/_doc/'
SEARCH = ('messageid', 'author_from', 'author_to', 'email', 'raw_data') #Поисковые  колонки
IMPORT_DUMP = False #



class Client():

	def __init__(self, count_generator, connect_uri, session=None):
		self.count_index = count_generator
		self.connect_uri = connect_uri
		if session is None:
			self.session = aiohttp.ClientSession()
		else:
			self.session = session


	async def send(self, data, sem):
		async with sem:
			url = self.connect_uri + '{}/?pretty'.format(
									await self.count_index.__anext__())
			await self._fetch(data, url)


	async def _fetch(self,send, url):
		async with self.session.post(url, json=json.loads(send)) as resp:
			print('-----', resp)
			if resp.content_type == 'application/json':
				data =  await resp.json()
				print(data)

	async def close(self):
		await self.session.close()

	async def __aenter__(self):
		return self

	async def __aexit__(self, exception_type, exception_value, traceback):
		await self.close()


async def data_table(table,count_index):
	tasks =[]
	sem = asyncio.Semaphore(value=1000)
	async with Client(count_index, ElasticSearch_URI) as cli:
		for a in table:
			if SEARCH[0] in a and SEARCH[1] in a and SEARCH[2] in a and SEARCH[3] in a:
				aser = dict(a)
				rel = {ser : aser.pop(ser) for ser in SEARCH[:3]}
				rel.update({SEARCH[4] : aser})
				data_json = json.dumps(rel, sort_keys=True, indent=4, default=str)
				task = loop.create_task(cli.send(data_json, sem))
				tasks.append(task)
		await asyncio.gather(*tasks)


async def count():
	a = 0
	while True:
		a+=1
		yield a


async def main():
	t1 = time.time()
	if IMPORT_DUMP:
		os.system(psgr_import_dump)
	db = await asyncpg.create_pool(dsn=DATABASE_URI)
	async with db.acquire() as conn:
		query = """SELECT table_name FROM information_schema.tables
				WHERE table_schema NOT IN ('information_schema','pg_catalog');"""
		all_tables = await conn.fetch(query)
		count_index = count()
		for tabl in all_tables:
			tabl_name = tabl['table_name']
			query = f"""select * from {tabl_name} ;"""
			all_ = await conn.fetch(query)
			await data_table(all_, count_index)
	print('time -', time.time() - t1)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	task = [
			loop.create_task(main())
		]
	wait_tasks = asyncio.wait(task)
	loop.run_until_complete(wait_tasks)
	loop.close()