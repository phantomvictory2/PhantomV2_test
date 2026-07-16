import asyncio
import asyncpg
import os
import time
from dotenv import load_dotenv

async def run():
    load_dotenv()
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    rows = await conn.fetch("SELECT entry_price, opened_at, pnl FROM positions WHERE strategy_type = 'LAST_SHADOW_TRADE_LITE_V4'")
    for r in rows:
        d = dict(r)
        d['opened_at'] = str(d['opened_at'])
        print(d)
    await conn.close()

if __name__ == "__main__":
    asyncio.run(run())
