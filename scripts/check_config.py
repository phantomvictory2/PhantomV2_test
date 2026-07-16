import asyncio
import asyncpg
import os
from dotenv import load_dotenv

async def run():
    load_dotenv()
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    rows = await conn.fetch('SELECT * FROM system_config')
    for r in rows:
        print(dict(r))
    await conn.close()

if __name__ == "__main__":
    asyncio.run(run())
