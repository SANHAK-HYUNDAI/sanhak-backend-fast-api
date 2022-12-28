import collections
import collections
import itertools
import multiprocessing

import pandas as pd
from fastapi import FastAPI, UploadFile, Form, HTTPException
from loguru import logger

from ca.core import calculate_similar_multi_processing, calculate_similar_cosine, morphological_analysis
from ca.service import save_ca_field_keyword, save_ca_keyword, ca_findall, save_ca, convert_ca_column, ca_preprocessing, \
    save_ca_big_category, save_ca_sub_category
from db.connect import create_connection
from ro.service import add_big_category, ro_findall, save_ro, convert_ro_column, ro_preprocessing, save_ro_sub_category, \
    save_ro_big_category, save_ro_frequency
from similarity.service import save_similar

insert_size_limit = 10000

app = FastAPI()
pool = multiprocessing.Pool(4)


@app.get("")
async def root():
    return {"hello": "world"}


# 파일이 엑셀 파일이 맞는지 검증하는 로직
def is_excel(file_name: str):
    return file_name[:-3] == "xls" or file_name[:-4] == 'xlsx'


@app.post("/upload/ro")
async def upload_ro(file: UploadFile = Form(...)):
    logger.info("upload ro test_file start")

    logger.info("입력된 파일에 대한 Validation 체크")
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

    logger.info("nan 값에 대한 예외 상황 체크 및 빈 문자열로 대체")
    # 비어있는 내용에 대해서 빈 문자열 대체
    df.fillna('', inplace=True)

    logger.info("big category 컬럼 추가")
    # big category를 추가하는 로직
    df = await add_big_category(df)

    # 특이사항 컬럼에 대한 키워드 분석
    special_note_list = [(idx, content) for idx, content in enumerate(df["special_note"].to_list())]
    special_note_list = await ro_preprocessing(special_note_list)

    keyword_list = morphological_analysis(special_note_list)
    keyword_list = list(itertools.chain(*keyword_list.values()))

    keyword_frequency = calculate_frequency(keyword_list)

    logger.info("big category 컬럼 빈도수 측정")
    sub_list, big_list = df["sub_phenom"].to_list(), df["big_phenom"].to_list()
    # calculate frequency
    sub_cate, big_cate = calculate_frequency(sub_list), calculate_frequency(big_list)

    logger.info("RO, RO category, frequency 정보를 DBdp 저장")
    # save category and ro

    # 하나의 큰 커넥션으로 묶음
    async with await create_connection() as conn:
        # 하나의 큰 트랜잭션으로 묶음
        async with conn.cursor() as cursor:
            await save_ro_frequency(keyword_frequency, cursor),
            await save_ro_sub_category(sub_cate, cursor),
            await save_ro_big_category(big_cate, cursor),
            await save_ro(df, cursor)
            await conn.commit()
    logger.info("upload ro test_file end")
    return {"message": "ro save success"}


@app.post('/upload/ca')
async def upload_ca(file: UploadFile = Form(...)):
    logger.info("CA 업로드 시작")
    async with await create_connection() as conn:
        async with conn.cursor() as cursor:

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

            logger.info("CA 빈 문자열에 대한 예외처리 시작")
            # CA 파일에 대한 전처리
            # CA: nan 값에 대해서 None 으로 변환
            ca_df.fillna('', inplace=True)

            logger.info("CA에 대한 전체 내용 저장")
            # ca 정보에 대해서 저장
            await save_ca(ca_df, cursor)

            # ca_id와 함께 조회
            ca_data = await ca_findall(cursor)
            ro_data = await ro_findall(cursor)

            logger.info(len(ca_data))

            logger.info("유사도 분석을 위한 전처리 시작")
            ca_data = await ca_preprocessing(ca_data)
            ro_data = await ro_preprocessing(ro_data)

            logger.info("유사도 분석 병렬 processing 시작")
            # 유사도 분석 -> 현재 os의 cpu 개수 만큼 병렬 처리 시작
            similar_list = calculate_similar_multi_processing(func=calculate_similar_cosine, content_list=ca_data,
                                                              special_note_list=ro_data, pool=pool, process_count=4)

            logger.info("중요 키워드에 대한 분석 시작")
            # 형태소 분석
            morpheme_list = morphological_analysis(ca_data)

            logger.info("키워드에 대한 빈도수 분석 시작")
            # 2차원 리스트를 1차원 리스트로 변경
            keyword_list = list(itertools.chain(*morpheme_list.values()))
            # 중요 키워드에 대한 빈도수 측정
            keyword_frequency = calculate_frequency(keyword_list)

            logger.info("유사도, 키워드에 대한 저장 및 키워드 빈도수 DB 저장")
            # 유사도, 형태소 결과를 비동기로 저장
            # 유사도 저장
            await save_similar(similar_list, cursor),
            # 키워드 저장
            await save_ca_field_keyword(morpheme_list, cursor),
            # 키워드 빈도수 저장
            await save_ca_keyword(keyword_frequency, cursor)
            # ca와 관련된 상위 카테고리와 하위 카테고리 빈도수를 측정해서 저장

            logger.info("CA와 맵핑된 RO 정보의 대 카테고리와, 중 카테고리를 불러와서 빈도수 측정해야함.")
            phenom_list = await find_total_phenom_frequency(cursor)
            print(phenom_list)

            sub_phenom_frequency = collections.Counter([sub for big, sub in phenom_list])
            big_phenom_frequency = collections.Counter([big for big, sub in phenom_list])

            print(big_phenom_frequency)
            print(sub_phenom_frequency)

            logger.info("CA 카테고리 빈도수 저장 완료")
            await save_ca_big_category(big_phenom_frequency, cursor)
            await save_ca_sub_category(sub_phenom_frequency, cursor)

            logger.info("CA 키워드 부분에 저장 시작")
            await conn.commit()

    logger.info("CA 업로드 종료")
    return {"message": "CA test_file upload success"}


def calculate_frequency(elems) -> dict:
    return dict(collections.Counter(elems))



