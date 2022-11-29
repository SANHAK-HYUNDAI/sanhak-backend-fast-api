import collections

from fastapi import FastAPI, UploadFile, Form
import pandas as pd
from ro_upload import ro_upload
from db.connect import conn
from big_sub_category import labelling, big_category

insert_size_limit = 10000

app = FastAPI()


@app.get("")
async def root():
    return {"hello": "world"}


@app.post("/upload/ro")
async def upload_ro(file: UploadFile = Form(...)):
    # file validation
    if file.filename[:-3] == "xls" or file.filename[:-4] == "xlsx":
        raise Exception("엑셀 파일이 아닙니다.")

    read_file = await file.read()
    excel_file = pd.read_excel(read_file)

    # column 명을 변경
    excel_file = convert_ro_column(excel_file)

    # ro 전처리
    df = await ro_upload(excel_file)

    # ro 저장
    df = save_ro(df)

    sub_list, big_list = df["sub_phenom"].to_list(), df["big_phenom"].to_list()
    sub_tmp, big_tmp = calculate_frequency(sub_list), calculate_frequency(big_list)

    save_category(big_tmp, sub_tmp)

    return {"message": "ro save success"}


def calculate_frequency(elems):
    return dict(collections.Counter(elems))


def convert_ro_column(excel_file):
    excel_file.columns = ["vehicle_type", "part_number", "cause_part", "cause_part_name_kor", "cause_part_name_eng",
                          "phenomenon", "special_note", "location", "cause_part_cluster", "problematic", "cause"]
    return excel_file


def save_ro(data):
    try:
        cursor = conn.cursor()

        # bulk insert 를 위한 sql 작성
        bulk_insert_sql = """
        insert into repair_order (vehicle_type, part_number, cause_part, cause_part_name_kor, cause_part_name_eng, big_phenom, sub_phenom, special_note, location, cause_part_cluster, problematic, cause) 
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """

        # big_phenom 을 맵핑 동작 반복문
        values = []
        for idx, row in data.iterrows():
            tmp = row.to_list()
            tmp.insert(5, big_category[labelling[tmp[5]]])
            values.append(tmp)

        cursor.executemany(bulk_insert_sql, values)
        conn.commit()
    except Exception:
        print('update big, sub category sql error')
    finally:
        conn.close()

    # 카테고리를 위한 새로운 df를 생성
    res = pd.DataFrame(values, columns=["vehicle_type", "part_number", "cause_part", "cause_part_name_kor",
                                        "cause_part_name_eng", "big_phenom", "sub_phenom", "special_note", "location",
                                        "cause_part_cluster", "problematic", "cause"])
    return res


def save_category(big_cate: dict, sub_cate: dict):
    try:
        curser = conn.cursor()
        update_big_category_sql = "update ro_big_category set count = count + %s where cate_name = %s"

        big_values = [(int(v), k) for k, v in big_cate.items()]
        sub_values = [(int(v), k) for k, v in sub_cate.items()]

        curser.executemany(update_big_category_sql, big_values)
        curser.executemany(update_big_category_sql, sub_values)
        conn.commit()
    except Exception:
        print('update big, sub category sql error')
    finally:
        conn.close()
