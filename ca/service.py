import re
import warnings

import pandas as pd

from ca.data import ca_column_convert

warnings.filterwarnings(action='ignore')


async def ca_findall(cursor):
    ca_select_findall_sql = 'select ca_id, content from cafe_article'
    await cursor.execute(ca_select_findall_sql)
    res = await cursor.fetchall()
    return res


async def save_ca(ca_df, cursor):
    ca_bulk_insert_sql = 'insert into cafe_article (board_name, cafe_name, content, title, url, writer, created_at) values (%s, %s, %s, %s, %s, %s, %s)'
    values = []
    for idx, row in ca_df.iterrows():
        # 시간 관련 컬럼 수정
        values.append([row['board_name'], row['cafe_name'], row['content'], row['title'], row['url'], row['writer'],
                       pd.to_datetime(row['created_at'])])
    await cursor.executemany(ca_bulk_insert_sql, values)


async def save_ca_field_keyword(morpheme_list, cursor):
    update_ca_keyword_sql = "update cafe_article set keywords=%s where ca_id=%s"
    values = []
    for ca_id, val in morpheme_list.items():
        values.append(['|'.join(val), ca_id])
    await cursor.executemany(update_ca_keyword_sql, values)


async def save_ca_keyword(keyword_frequency, cursor):
    insert_keyword_frequency_sql = "insert into ca_keyword (word, frequency) values (%s, %s)"
    values = [(key, val) for key, val in keyword_frequency.items()]
    await cursor.execute("truncate table ca_keyword")
    await cursor.executemany(insert_keyword_frequency_sql, values)


def convert_ca_column(excel_file):
    excel_file.columns = [ca_column_convert[col] for col in excel_file.columns]
    return excel_file


async def ca_preprocessing(ca_data):
    # 전처리 시작
    ca_data = [(idx, content) for idx, content in ca_data if content and isinstance(content, str)]
    # CA:한글만 출력이 되도록 필터링
    ca_data = [(idx, re.sub("[^가-힣]+", "", content)) for idx, content in ca_data]
    # CA:개행 문자 지우기
    ca_data = [(idx, re.sub('\n', '', content)) for idx, content in ca_data]
    # CA:긴 공백은 하나의 공백으로 바꾸기
    ca_data = [(idx, re.sub(' +', ' ', content)) for idx, content in ca_data]
    return ca_data


async def save_ca_big_category(big_cate: dict, cursor):
    # ca의 경우 전체 ca 데이터를 불러와서 유사도 분석을 진행하기 때문에 이전의 count 값을 고려할 필요없이 그대로 update하면됨
    update_big_category_sql = "update ca_big_category set count = %s where cate_name = %s"
    big_values = [(int(v), k) for k, v in big_cate.items()]
    await cursor.executemany(update_big_category_sql, big_values)


async def save_ca_sub_category(sub_cate: dict, cursor):
    # ca의 경우 전체 ca 데이터를 불러와서 유사도 분석을 진행하기 때문에 이전의 count 값을 고려할 필요없이 그대로 update하면됨
    update_sub_category_sql = "update ca_sub_category set count = %s where cate_name = %s"
    sub_values = [(int(v), k) for k, v in sub_cate.items()]
    await cursor.executemany(update_sub_category_sql, sub_values)
