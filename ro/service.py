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


async def ro_findall():
    conn = await create_connection()
    ro_select_findall_sql = 'select ro_id, special_note from repair_order'
    async with conn.cursor() as cursor:
        await cursor.execute(ro_select_findall_sql)
        res = await cursor.fetchall()
        await conn.commit()
    return res


def convert_ro_column(excel_file):
    excel_file.columns = ro_order_list
    return excel_file


async def save_ro_category(big_cate: dict, sub_cate: dict):
    con = await create_connection()
    cursor = await con.cursor()
    try:
        update_big_category_sql = "update ro_big_category set count = count + %s where cate_name = %s"
        update_sub_category_sql = "update ro_sub_category set count = count + %s where cate_name = %s"

        big_values = [(int(v), k) for k, v in big_cate.items()]
        sub_values = [(int(v), k) for k, v in sub_cate.items()]

        await cursor.executemany(update_big_category_sql, big_values)
        await cursor.executemany(update_sub_category_sql, sub_values)
        await con.commit()
    except Exception:
        logger.warning('update big, sub category sql error')
    finally:
        await cursor.close()


async def save_ro(data):
    con = await create_connection()
    cursor = await con.cursor()
    values = []
    try:
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
        await con.commit()
    except Exception:
        logger.warning('update big, sub category sql error')
    finally:
        await cursor.close()
