import aiohttp
import asyncio
import json


class AsyncSocket():
    def __init__ (self):
        # self.conn = aiohttp.TCPConnector(
        #     limit_per_host=100,
        #     limit=0,
        #     ttl_dns_cache=300,
        # )
        self.lock = asyncio.Semaphore(100)
        self.session = aiohttp.ClientSession()
    
    async def gather(self, callee, requests, responses):
        num_requests = len(requests)
    
        async def get(id, session, **kwargs):
            async with self.lock:
                responses[id] = await callee(session, **kwargs)

        await asyncio.gather(*(get(id, self.session, **kwargs) for id, kwargs in enumerate(requests)))

    def run(self, callee, requests):
        responses = {}
        loop=asyncio.get_event_loop()
        loop.run_until_complete(
            self.gather(callee, requests, responses)
        )
        return [responses[i] for i in range(len(requests))]
    
    def close(self):
        self.session.connector.close()