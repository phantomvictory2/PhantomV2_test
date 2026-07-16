import asyncio
import asyncpg
import os
from dotenv import load_dotenv

async def run():
    load_dotenv()
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    await conn.execute("UPDATE system_config SET value = '1000.0' WHERE key = 'bankroll'")
    print("Bankroll updated to 1000.0 in system_config")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(run())
