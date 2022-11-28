from fastapi import FastAPI, UploadFile, Form, HTTPException
import pandas as pd
from db.connect import conn
import db.sql as sql
import ro.data as ro
import ca.data as ca

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

    try:
        await save_ro(excel_file)
    except Exception:
        raise HTTPException(status_code=403, detail="error insert, please check xlsx file data format")
    return {"message": "Hello World"}


async def save_ro(excel_data):
    # ./ro/data.py에 저장된 data
    ro_rename_dict = ro.ro_rename_dict
    ro_order_list = ro.ro_order_list

    cursor = conn.cursor()
    bulk_insert_ro_sql = sql.bulk_insert_ro_sql
    excel_data.rename(columns=ro_rename_dict, inplace=True)
    data_str = ""

    for j in range(insert_size_limit):
        data_str += "("
        sort_list = []
        for i in ro_order_list[:-2]:
            sort_list.append(str(excel_data.iloc[j][i]))
        data_str += ",".join(sort_list)
        data_str += "),"

    # 맨 뒤에 , 없애기
    bulk_insert_ro_sql += data_str[:len(data_str) - 1]
    cursor.execute(bulk_insert_ro_sql)
    conn.commit()


@app.post("/upload/ca")
async def upload_ca(file: UploadFile = Form(...)):
    # file validation
    if file.filename[:-3] == "xls" or file.filename[:-4] == "xlsx":
        raise Exception("엑셀 파일이 아닙니다.")

    read_file = await file.read()
    excel_file = pd.read_excel(read_file)

    try:
        await save_ca(excel_file)
    except Exception:
        raise HTTPException(status_code=403, detail="error insert, please check xlsx file data format")
    return {"message": "Hello World"}


async def save_ca(excel_data):
    # ./ca/data.py에 저장된 data
    ca_rename_dict = ca.ca_rename_dict
    ca_order_list = ca.ca_order_list

    cursor = conn.cursor()
    bulk_insert_ca_sql = sql.bulk_insert_ca_sql
    excel_data.rename(columns=ca_rename_dict, inplace=True)
    data_str = ""

    for j in range(insert_size_limit):
        data_str += "("
        sort_list = []
        for i in ca_order_list[:-2]:
            sort_list.append(str(excel_data.iloc[j][i]))
        data_str += ",".join(sort_list)
        data_str += "),"

    # 맨 뒤에 , 없애기
    bulk_insert_ca_sql += data_str[:len(data_str) - 1]
    cursor.execute(bulk_insert_ca_sql)
    conn.commit()


# 'keyword': frequency_value 형태 입력
# pk가 존재할 경우 update, 없으면 insert 하는 sql
# 쿼리를 이어붙이기 어려워 bulk 포기, 단건으로 작성
def upload_ca_top_keyword_frequency(**kwargs):
    cursor = conn.cursor()
    insert_ca_keyword_frequency_sql = sql.insert_ca_keyword_frequency_sql

    for key, val in kwargs.items():
        cursor.execute(insert_ca_keyword_frequency_sql, (key, val, key, val))
    conn.commit()


# CA_ID=['키워드1','키워드2','키워드3']의 형태 입력
def upload_ca_keywords(**kwargs):
    cursor = conn.cursor()
    insert_ca_keywords = sql.insert_ca_keywords

    for key, val in kwargs.items():
        for ca_keyword in val:
            cursor.excute(insert_ca_keywords, ca_keyword, key)

    conn.commit()


