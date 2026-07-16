import asyncio
import os
import json
from dotenv import load_dotenv
from analytics_engine import AnalyticsEngine

async def run():
    load_dotenv()
    a = AnalyticsEngine()
    res = await a.run_analysis(output_file=None, return_data=True)
    if res:
        print("Success! Rankings:")
        for r in res.get("ranking", []):
            print(r)
    else:
        print("No Data")

if __name__ == "__main__":
    asyncio.run(run())
