import asyncio
import os
import sys
import asyncpg
from dotenv import load_dotenv

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main():
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    print(f"Connecting to: {url}")
    try:
        conn = await asyncpg.connect(url)
        print("Successfully connected!")
        val = await conn.fetchval("SELECT 1")
        print(f"Test query returned: {val}")
        await conn.close()
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
