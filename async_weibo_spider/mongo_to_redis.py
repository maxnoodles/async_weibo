from motor.motor_asyncio import AsyncIOMotorClient
import aioredis
import asyncio


async def mongo_to_redis():
    client = AsyncIOMotorClient()
    col = client['WeiBo']['weibo_json_temp']
    r = await aioredis.create_redis_pool(('localhost', 6379), password='noodles')

    async with await client.start_session() as s:
        async for doc in col.find(session=s):
            await r.lpush('weibo_json_temp', doc['text'])

    a = await r.llen('weibo_json_temp')
    print(a)
    r.close()
    await r.wait_closed()


def run():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mongo_to_redis())
    loop.close()

run()