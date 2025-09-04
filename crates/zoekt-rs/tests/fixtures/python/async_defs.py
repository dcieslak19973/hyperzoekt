async def fetch_data():
    # simulate async function
    return 42

async def process():
    x = await fetch_data()
    return x * 2

class AsyncWorker:
    async def run(self):
        return await fetch_data()
