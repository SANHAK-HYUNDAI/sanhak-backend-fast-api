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
    tmp = await ro_upload(excel_file)

    # ro 저장
    save_ro(tmp)
    return {"message": "ro save success"}


def convert_ro_column(excel_file):
    excel_file.columns = ["vehicle_type", "part_number", "cause_part", "cause_part_name_kor", "cause_part_name_eng",
                          "phenomenon", "special_note", "location", "cause_part_cluster", "problematic", "cause"]
    return excel_file


def save_ro(data):
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
    cursor.close()
