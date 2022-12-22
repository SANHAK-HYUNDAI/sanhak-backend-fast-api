import warnings

import pandas as pd

from ca.data import ca_column_convert
from db.connect import create_connection

warnings.filterwarnings(action='ignore')


async def ca_findall():
    conn = await create_connection()
    ca_select_findall_sql = 'select ca_id, content from cafe_article'
    async with conn.cursor() as cursor:
        await cursor.execute(ca_select_findall_sql)
        res = await cursor.fetchall()
        await conn.commit()
    return res


async def save_ca(ca_df):
    ca_bulk_insert_sql = 'insert into cafe_article (board_name, cafe_name, content, title, url, writer, created_at) values (%s, %s, %s, %s, %s, %s, %s)'
    values = []

    for idx, row in ca_df.iterrows():
        # 시간 관련 컬럼 수정
        values.append([row['board_name'], row['cafe_name'], row['content'], row['title'], row['url'], row['writer'],
                       pd.to_datetime(row['created_at'])])

    conn = await create_connection()
    async with conn.cursor() as cursor:
        await cursor.executemany(ca_bulk_insert_sql, values)
        await conn.commit()


async def save_ca_field_keyword(morpheme_list):
    conn = await create_connection()
    update_ca_keyword_sql = "update cafe_article set keywords=%s where ca_id=%s"

    values = []
    for ca_id, val in morpheme_list.items():
        values.append(['|'.join(val), ca_id])

    async with conn.cursor() as cursor:
        await cursor.executemany(update_ca_keyword_sql, values)
        await conn.commit()


async def save_ca_keyword(keyword_frequency):
    conn = await create_connection()
    insert_keyword_frequency_sql = "insert into ca_keyword (word, count) values (%s, %s)"
    values = [(key, val) for key, val in keyword_frequency.items()]
    async with conn.cursor() as cursor:
        await cursor.execute("truncate table ca_keyword")
        await cursor.executemany(insert_keyword_frequency_sql, values)
        await conn.commit()


def convert_ca_column(excel_file):
    excel_file.columns = [ca_column_convert[col] for col in excel_file.columns]
    return excel_file
