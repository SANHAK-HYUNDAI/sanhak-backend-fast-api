import re
import warnings

from loguru import logger

from big_sub_category import labelling, big_category
from db.connect import create_connection
from ro.data import ro_order_list

warnings.filterwarnings(action='ignore')


def confirm_kor_or_eng(input_s):
    k_count = 0
    e_count = 0
    for c in input_s:
        if ord('가') <= ord(c) <= ord('힣'):
            k_count += 1
        elif ord('a') <= ord(c.lower()) <= ord('z'):
            e_count += 1
    return "한국어" if k_count > e_count else "영어"


async def ro_pretreatment(RO_df):
    return RO_df


async def add_big_category(df):
    df.insert(5, 'big_phenom', [big_category[labelling[val['sub_phenom']]] for idx, val in df.iterrows()])
    return df


async def ro_findall(cursor):
    ro_select_findall_sql = 'select ro_id, special_note from repair_order'
    await cursor.execute(ro_select_findall_sql)
    res = await cursor.fetchall()
    return res


def convert_ro_column(excel_file):
    excel_file.columns = ro_order_list
    return excel_file


async def save_ro_big_category(big_cate: dict, cursor):
    update_big_category_sql = "update ro_big_category set count = count + %s where cate_name = %s"
    big_values = [(int(v), k) for k, v in big_cate.items()]
    await cursor.executemany(update_big_category_sql, big_values)
    # await conn.commit()


async def save_ro_sub_category(sub_cate: dict, cursor):
    update_sub_category_sql = "update ro_sub_category set count = count + %s where cate_name = %s"
    sub_values = [(int(v), k) for k, v in sub_cate.items()]
    await cursor.executemany(update_sub_category_sql, sub_values)
    # await conn.commit()


async def save_ro(data, cursor):
    values = []
    # bulk insert 를 위한 sql 작성
    bulk_insert_sql = """
    insert into repair_order (vehicle_type, part_number, cause_part, cause_part_name_kor, cause_part_name_eng, big_phenom, sub_phenom, special_note, location, cause_part_cluster, problematic, cause) 
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    """

    # big_phenom 을 맵핑 동작 반복문
    for idx, row in data.iterrows():
        tmp = row.to_list()
        values.append(tmp)

    await cursor.executemany(bulk_insert_sql, values)
    # await conn.commit()


async def ro_preprocessing(ro_data):
    ro_data = [(idx, content) for idx, content in ro_data if content and isinstance(content, str)]
    # RO:한글만 출력이 되도록 필터링
    ro_data = [(idx, re.sub("[^가-힣 ]+", "", special_note)) for idx, special_note in ro_data]
    # RO:개행 문자 삭제
    ro_data = [(idx, re.sub('\n', '', special_note)) for idx, special_note in ro_data]
    # RO:긴 공백을 하나의 공백으로 바꾸기
    ro_data = [(idx, re.sub(' +', ' ', special_note)) for idx, special_note in ro_data]
    return ro_data


async def save_ro_frequency(keyword_frequency, cursor):
    truncate_ro_keyword_sql = 'truncate table ro_keyword'
    insert_ro_keyword_frequency_sql = 'insert into ro_keyword (word, count) values (%s, %s)'

    values = [(k, v) for k, v in keyword_frequency.items()]

    await cursor.execute(truncate_ro_keyword_sql)
    await cursor.executemany(insert_ro_keyword_frequency_sql, values)
    # await conn.commit()
