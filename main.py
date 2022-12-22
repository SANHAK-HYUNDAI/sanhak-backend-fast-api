import asyncio
import collections
import re

import pandas as pd
from fastapi import FastAPI, UploadFile, Form, HTTPException
from loguru import logger

from big_sub_category import labelling, big_category
from ca.core import calculate_similar_multi_processing, calculate_similar_cosine, morphological_analysis
from ca.service import save_ca_field_keyword, save_ca_keyword, ca_findall, save_ca, convert_ca_column
from db.connect import create_connection
from ro.service import add_big_category, ro_findall, save_ro_category, save_ro, convert_ro_column

insert_size_limit = 10000

app = FastAPI()


@app.get("")
async def root():
    return {"hello": "world"}


@app.get("/category/init")
async def init():
    con = await create_connection()
    cursor = await con.cursor()
    insert_sub_cate_zero_sql = "insert into ro_sub_category (cate_name, big_cate_name, count) values (%s, %s, %s)"
    insert_big_cate_zero_sql = "insert into ro_big_category (cate_name, count) values (%s,  %s)"
    try:
        sub_values = []
        big_values = [[cate, 0] for cate in big_category.values()]

        for sub_cate, val in labelling.items():
            big_cate = big_category[val]
            sub_values.append([sub_cate, big_cate, 0])

        # sub category 초기화
        await asyncio.wait([
            cursor.executemany(insert_sub_cate_zero_sql, sub_values),
            cursor.executemany(insert_big_cate_zero_sql, big_values)
        ])
        con.commit()
    except:
        logger.warning("sub category를 초기화하는 과정에서 문제가 발생하였습니다.")
    finally:
        await cursor.close()
    return {"init": "success"}


# 파일이 엑셀 파일이 맞는지 검증하는 로직
def is_excel(file_name: str):
    return file_name[:-3] == "xls" or file_name[:-4] == 'xlsx'


@app.post("/upload/ro")
async def upload_ro(file: UploadFile = Form(...)):
    logger.info("upload ro test_file start")

    # 엑셀 파일이 맞는지 검증하는 단계
    if is_excel(file.filename):
        raise HTTPException(status_code=400)

    # 파일을 읽고 그 컬럼 이름을 변경하는 단계
    try:
        read_file = await file.read()
        excel_file = pd.read_excel(read_file)
        df = convert_ro_column(excel_file)
    except:
        raise HTTPException(status_code=400)

    # 비어있는 내용에 대해서 빈 문자열 대체
    df.fillna('', inplace=True)

    # big category를 추가하는 로직
    df = await add_big_category(df)

    sub_list, big_list = df["sub_phenom"].to_list(), df["big_phenom"].to_list()
    # calculate frequency
    sub_cate, big_cate = calculate_frequency(sub_list), calculate_frequency(big_list)

    # save category and ro
    await asyncio.wait([
        save_ro_category(big_cate, sub_cate),
        save_ro(df)
    ])
    logger.info("upload ro test_file end")
    return {"message": "ro save success"}


async def save_similar(similar_list):
    conn = await create_connection()
    insert_similarity_sql = "insert into similarity (ca_id, ro_id) values (%s, %s)"
    values = [[str(ca_id), str(ro_id)] for ca_id, val in similar_list.items() for ro_id, similar in val]

    async with conn.cursor() as cursor:
        await cursor.executemany(insert_similarity_sql, values)
        await conn.commit()


@app.post('/upload/ca')
async def upload_ca(file: UploadFile = Form(...)):
    # test_file validation
    file_extension = ["xls", "xlsx"]
    if file and (file.filename[:-3] in file_extension or file.filename[:-4] in file_extension):
        raise HTTPException(status_code=400)

    # test_file read and convert column
    try:
        read_file = await file.read()
        excel_file = pd.read_excel(read_file)
        ca_df = convert_ca_column(excel_file)
    except:
        raise HTTPException(status_code=400, detail="입력된 파일의 형식이 다릅니다.")

    # CA 파일에 대한 전처리
    # CA: nan 값에 대해서 None 으로 변환
    ca_df.fillna('', inplace=True)

    # ca 정보에 대해서 저장
    await save_ca(ca_df)

    # ca_id와 함께 조회
    ca_data = await ca_findall()
    ro_data = await ro_findall()

    # 전처리 시작
    ca_data = [(idx, content) for idx, content in ca_data if content and isinstance(content, str)]
    # CA:한글만 출력이 되도록 필터링
    ca_data = [(idx, re.sub("[^가-힣]+", "", content)) for idx, content in ca_data]
    # CA:개행 문자 지우기
    ca_data = [(idx, re.sub('\n', '', content)) for idx, content in ca_data]
    # CA:긴 공백은 하나의 공백으로 바꾸기
    ca_data = [(idx, re.sub(' +', ' ', content)) for idx, content in ca_data]

    ro_data = [(idx, content) for idx, content in ro_data if content and isinstance(content, str)]
    # RO:한글만 출력이 되도록 필터링
    ro_data = [(idx, re.sub("[^가-힣 ]+", "", special_note)) for idx, special_note in ro_data]
    # RO:개행 문자 삭제
    ro_data = [(idx, re.sub('\n', '', special_note)) for idx, special_note in ro_data]
    # RO:긴 공백을 하나의 공백으로 바꾸기
    ro_data = [(idx, re.sub(' +', ' ', special_note)) for idx, special_note in ro_data]

    # 유사도 분석 -> 현재 os의 cpu 개수 만큼 병렬 처리 시작
    similar_list = calculate_similar_multi_processing(calculate_similar_cosine, ca_data, ro_data, 4)

    # 형태소 분석
    morpheme_list = morphological_analysis(ca_data)

    # 키워드 빈도수 측정
    keyword_list = []
    for val in morpheme_list.values():
        keyword_list.extend(val)
    keyword_frequency = collections.Counter(keyword_list)

    # 유사도, 형태소 결과 저장
    await save_similar(similar_list)
    await save_ca_field_keyword(morpheme_list)
    await save_ca_keyword(keyword_frequency)

    return {"message": "CA test_file upload success"}


def calculate_frequency(elems):
    return dict(collections.Counter(elems))
