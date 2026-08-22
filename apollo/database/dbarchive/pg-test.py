"""
    just a test file for postgres async connection pool and stuff
"""

import os
import asyncio
from dotenv import load_dotenv
from psycopg_pool import AsyncConnectionPool

load_dotenv()

# ------------------------------------------------------------------

async def main():
    conninfo = f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}"
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    async with AsyncConnectionPool(conninfo=conninfo, min_size=1, max_size=10) as pool: # min_size = minimum conncurrent connections, max_size = maximum conncurrent connections, tune this based on postgres config
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT tablename
                    FROM pg_catalog.pg_tables
                    WHERE schemaname = 'public';
                """)
                tables = await cur.fetchall()
                print("all public tables: ", tables)
                await pool.close()
                

if __name__ == "__main__":
    asyncio.run(main())


    