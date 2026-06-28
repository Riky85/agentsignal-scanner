#!/usr/bin/env python3
"""Reset base44_id su tutti i record Railway — da eseguire ONCE."""
import asyncio, asyncpg, os

async def main():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    async with pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM companies WHERE base44_id IS NOT NULL")
        print(f"Record con base44_id: {n:,}")
        await c.execute("UPDATE companies SET base44_id=NULL, last_push_date=NULL")
        n2 = await c.fetchval("SELECT COUNT(*) FROM companies WHERE base44_id IS NULL")
        print(f"Reset completato — {n2:,} record azzerati")
    await pool.close()

asyncio.run(main())
