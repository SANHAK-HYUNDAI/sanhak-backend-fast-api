import aiomysql as mysql
import asyncio

loop = asyncio.get_event_loop()

conn = None


async def create_connection():
    if not conn:
        return await mysql.connect(
            host="sanhak-mysql-server",
            port=3306,
            user="sanhak",
            password="sanhak",
            db="hyundaitransys",
            charset="utf8",
            loop=loop
        )
    else:
        return conn
