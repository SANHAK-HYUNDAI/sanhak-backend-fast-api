import multiprocessing
import os
import pprint
import re
import time

import difflib

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from konlpy.tag import Mecab

from ca.data import ca_rename_dict
from ro.data import ro_order_list
from loguru import logger

"""
### calculate_similar 함수 parameter 설명 ###
ca_content_list = [(ca_id1, content1),(ca_id2, content2),(ca_id3, content3),(ca_id4, content4)]
ro_special_note_list = [(ro_id1, special_note1),(ro_id2, special_note2),(ro_id3, special_note3),(ro_id4, special_note4)]
n = 임의의 한 숫자 해당 숫자는 병렬 처리를 할 때에 함수 번호를 체크하기 위해서 구현한 파라미터로 아무 값이나 입력해도 상관없음 

### 사용 예제 ###
ca_content_list = [(1, "안녕하세요"),(2, "안녕"),(3, "반가워"),(4, "잘부탁해")]
ro_special_note_list = [(5, "안녕할까?"),(6, "이게 얼마만이야!"),(7, "잘지내지?"),(8, "반가워")]

result = calculate_similar_sequential_matcher(ca_content_list, ro_special_note_list, 11)
pprint.pprint(result)


### 실행 결과 ###
{1: [(5, 0.4), (6, 0.0), (7, 0.0), (8, 0.0)],
 2: [(5, 0.5714285714285714), (6, 0.0), (7, 0.0), (8, 0.0)],
 3: [(8, 1.0), (5, 0.0), (6, 0.0), (7, 0.0)],
 4: [(7, 0.2222222222222222), (5, 0.0), (6, 0.0), (8, 0.0)]}
 
### 알고리즘 설명 ### 
해당 함수는 유사도 분석을 하기 위한 core 모듈에 해당합니다. 
현재는 Sequential Matching Algorithm을 사용하여서 유사도 분석을 진행하고 있습니다.
"""


# 유사도 분석 O(N^2)
def calculate_similar_sequential_matcher(content_list, special_note_list, n):
    logger.info("start:" + n)
    result = dict()
    se = difflib.SequenceMatcher()
    for ca_id, content in content_list:
        similar_list = []
        for ro_id, special_note in special_note_list:
            se.set_seqs(content, special_note)
            similar = se.ratio()
            similar_list.append((ro_id, similar))

        # 전체 유사도에서 가장 유사도가 큰 정보만 걸러서 저장하기
        result[ca_id] = sorted(similar_list, key=lambda x: x[1], reverse=True)[:8]
    logger.info("end:" + n)
    return result


"""
### calculate_similar 함수 parameter 설명 ###
ca_content_list = [(ca_id1, content1),(ca_id2, content2),(ca_id3, content3),(ca_id4, content4)]
ro_special_note_list = [(ro_id1, special_note1),(ro_id2, special_note2),(ro_id3, special_note3),(ro_id4, special_note4)]
n = 임의의 한 숫자 해당 숫자는 병렬 처리를 할 때에 함수 번호를 체크하기 위해서 구현한 파라미터로 아무 값이나 입력해도 상관없음 

### 사용 예제 ###
ca_content_list = [(1, "안녕하세요"),(2, "안녕"),(3, "반가워"),(4, "잘부탁해")]
ro_special_note_list = [(5, "안녕할까?"),(6, "이게 얼마만이야!"),(7, "잘지내지?"),(8, "반가워")]

result = calculate_similar_cosine(ca_content_list, ro_special_note_list, 11)
pprint.pprint(result)


### 실행 결과 ###
{1: [(5, 0.0), (6, 0.0), (7, 0.0), (8, 0.0)],
 2: [(5, 0.0), (6, 0.0), (7, 0.0), (8, 0.0)],
 3: [(8, 1.0), (5, 0.0), (6, 0.0), (7, 0.0)],
 4: [(5, 0.0), (6, 0.0), (7, 0.0), (8, 0.0)]}

### 알고리즘 설명 ### 
해당 함수는 유사도 분석을 하기 위한 core 모듈에 해당합니다. 
현재는 Sequential Matching Algorithm을 사용하여서 유사도 분석을 진행하고 있습니다.
"""


def calculate_similar_cosine(content_list, special_note_list, n):
    print("start", n)
    result = dict()
    tfidf = TfidfVectorizer(ngram_range=(1, 5), min_df=3, max_df=0.9)

    ro_id_list = [ro_id for ro_id, special_note in special_note_list]
    special_notes = [special_note for ro_id, special_note in special_note_list]

    for ca_id, content in content_list:
        data = [*special_notes, content]  # 학습시킬 데이터
        tfidf_matrix = tfidf.fit_transform(data)
        cosine_sim = cosine_similarity(tfidf_matrix[-1], tfidf_matrix)
        recommendation_need = cosine_sim[-1]

        recommend_index = np.argsort(recommendation_need)[::-1][3:11]
        result[ca_id] = [(ro_id_list[idx], recommendation_need[idx]) for idx in recommend_index]
    print("end", n)
    return result


"""
### calculate_similar_multi_processing 함수 parameter 설명 ###
ca_content_list = [(ca_id1, content1),(ca_id2, content2),(ca_id3, content3),(ca_id4, content4)]
ro_special_note_list = [(ro_id1, special_note1),(ro_id2, special_note2),(ro_id3, special_note3),(ro_id4, special_note4)]
n = 임의의 한 숫자 해당 숫자는 병렬 처리를 할 때에 함수 번호를 체크하기 위해서 구현한 파라미터로 아무 값이나 입력해도 상관없음 

### 사용 예제 ###
ca_content_list = [(1, "안녕하세요"),(2, "안녕"),(3, "반가워"),(4, "잘부탁해")]
ro_special_note_list = [(5, "안녕할까?"),(6, "이게 얼마만이야!"),(7, "잘지내지?"),(8, "반가워")]

result = calculate_similar_multi_processing(calculate_similar, ca_content_list, ro_special_note_list, 4)
pprint.pprint(result)

### 실행 결과 ###
{1: [(5, 0.4), (6, 0.0), (7, 0.0), (8, 0.0)],
 2: [(5, 0.5714285714285714), (6, 0.0), (7, 0.0), (8, 0.0)],
 3: [(8, 1.0), (5, 0.0), (6, 0.0), (7, 0.0)],
 4: [(7, 0.2222222222222222), (5, 0.0), (6, 0.0), (8, 0.0)]}
 
### 알고리즘 설명 ###
기존에 calculate_similar를 단독으로 사용하기에는 소요 시간이 너무 큽니다. 전체 데이터 양이 방대하기 때문에 해당 함수를 병렬처리하여서 동작시킬 필요성을 느꼈습니다.
따라서 해당 함수는 유사도 분석 알고리즘을 쉽게 병렬 처리할 수 있도록 지원해주는 모듈입니다.
"""


def calculate_similar_multi_processing(func, content_list, special_note_list, pool, process_count=1):
    # 사전 처리
    if process_count <= 0:
        return
    if not len(content_list) or not len(special_note_list):
        return

    # 병렬 처리를 위해서 전체 길이를 이용해서 파트 부분 연산
    total = len(content_list)
    part = total // process_count

    # 병렬 처리에 사용될 여러 인자를 생성하는 로직
    arg = [(content_list[part * i:part * (i + 1)], special_note_list[:], i) for i in range(process_count - 1)]
    arg.append((content_list[part * (process_count - 1):], special_note_list[:], process_count - 1))

    # 프로세스 풀에서 병렬 연산 진행
    res = pool.starmap(func, arg)
    result = dict()
    for data in res:
        result.update(data)
    return result


def main():
    # read test_file
    ca_df = pd.read_excel("./test_file/ca.xlsx")
    ro_df = pd.read_excel("./test_file/ro.xlsx")

    # convert ca columns
    ca_order_list = [name if name not in ca_rename_dict else ca_rename_dict[name] for name in ca_df.columns]

    ro_df.columns = ro_order_list
    ca_df.columns = ca_order_list

    # 해당 값은 이미 주어진다고 가정 해당 부분에서 광고를 필터링하는 로직이 들어가야함
    ca_id_list = [i for i in range(len(ca_df))]
    ca_content_list = [content for content in ca_df["content"].to_list()
                       if content and content and isinstance(content, str)]
    ro_id_list = [i for i in range(len(ro_df))]
    ro_special_note_list = [special_note for special_note in ro_df["special_note"].to_list()
                            if special_note and isinstance(special_note, str)]

    # 전처리 시작
    # CA:한글만 출력이 되도록 필터링
    ca_content_list = [re.sub("[^가-힣]+", "", content) for content in ca_content_list]
    # CA:개행 문자 지우기
    ca_content_list = [re.sub('\n', '', content) for content in ca_content_list]
    # CA:긴 공백은 하나의 공백으로 바꾸기
    ca_content_list = [re.sub(' +', ' ', content) for content in ca_content_list]

    # RO:한글만 출력이 되도록 필터링
    ro_special_note_list = [re.sub("[^가-힣 ]+", "", special_note) for special_note in ro_special_note_list]
    # RO:개행 문자 삭제
    ro_special_note_list = [re.sub('\n', '', special_note) for special_note in ro_special_note_list]
    # RO:긴 공백을 하나의 공백으로 바꾸기
    ro_special_note_list = [re.sub(' +', ' ', special_note) for special_note in ro_special_note_list]

    start = time.time()
    # 유사도 분석 함수 파라미터에 맞게 입력값 생성
    ca_param = [(i, j) for i, j in zip(ca_id_list, ca_content_list)]
    ro_param = [(i, j) for i, j in zip(ro_id_list, ro_special_note_list)]

    # 유사도 분석 시작 -> 해당 로직 개선 필요(시간이 너무 많이 걸림 row 당 2.7~3초)
    res = calculate_similar_multi_processing(calculate_similar_cosine, ca_param, ro_param, os.cpu_count())
    # res = calculate_similar(ca_param[:10], ro_param, 1)
    end = time.time()
    # pprint.pprint(res)
    print('소요 시간: ', end - start)


def test():
    ca_content_list = [(1, "안녕하세요"), (2, "안녕"), (3, "반가워"), (4, "잘부탁해")]
    ro_special_note_list = [(5, "안녕할까?"), (6, "이게 얼마만이야!"), (7, "잘지내지?"), (8, "반가워")]

    result = calculate_similar_cosine(ca_content_list, ro_special_note_list, 11)
    pprint.pprint(result)


def test2():
    ca_content_list = [(1, "안녕하세요"), (2, "안녕"), (3, "반가워"), (4, "잘부탁해")]
    ro_special_note_list = [(5, "안녕할까?"), (6, "이게 얼마만이야!"), (7, "잘지내지?"), (8, "반가워")]

    result = calculate_similar_multi_processing(calculate_similar_cosine, ca_content_list, ro_special_note_list, 4)
    pprint.pprint(result)


### 형태소 분석 로직 구현 ###
def morphological_analysis(data_list):
    mecab = Mecab()
    result = dict()
    for idx, data in data_list:
        # 명사만 키워드로 추출
        keywords = mecab.nouns(data)
        # 한글자 단어는 제거 -> 한글자는 키워드로 사용하기 에 부적한한 경우가 많다고 생각
        keywords = [keyword for keyword in keywords if len(keyword) > 1][:5]
        result[idx] = keywords
    return result


def morphological_analysis_test():
    ca_df = pd.read_excel("./test_file/ca.xlsx")
    ro_df = pd.read_excel("./test_file/ro.xlsx")

    # convert ca columns
    ca_order_list = [name if name not in ca_rename_dict else ca_rename_dict[name] for name in ca_df.columns]

    ro_df.columns = ro_order_list
    ca_df.columns = ca_order_list

    # 해당 값은 이미 주어진다고 가정 해당 부분에서 광고를 필터링하는 로직이 들어가야함
    ca_id_list = [i for i in range(len(ca_df))]
    ca_content_list = [content for content in ca_df["content"].to_list()
                       if content and content and isinstance(content, str)]
    ro_id_list = [i for i in range(len(ro_df))]
    ro_special_note_list = [special_note for special_note in ro_df["special_note"].to_list()
                            if special_note and isinstance(special_note, str)]

    # 전처리 시작
    # CA:한글만 출력이 되도록 필터링
    ca_content_list = [re.sub("[^가-힣]+", "", content) for content in ca_content_list]
    # CA:개행 문자 지우기
    ca_content_list = [re.sub('\n', '', content) for content in ca_content_list]
    # CA:긴 공백은 하나의 공백으로 바꾸기
    ca_content_list = [re.sub(' +', ' ', content) for content in ca_content_list]

    # RO:한글만 출력이 되도록 필터링
    ro_special_note_list = [re.sub("[^가-힣 ]+", "", special_note) for special_note in ro_special_note_list]
    # RO:개행 문자 삭제
    ro_special_note_list = [re.sub('\n', '', special_note) for special_note in ro_special_note_list]
    # RO:긴 공백을 하나의 공백으로 바꾸기
    ro_special_note_list = [re.sub(' +', ' ', special_note) for special_note in ro_special_note_list]

    start = time.time()
    # 유사도 분석 함수 파라미터에 맞게 입력값 생성
    ca_param = [(i, j) for i, j in zip(ca_id_list, ca_content_list)]
    ro_param = [(i, j) for i, j in zip(ro_id_list, ro_special_note_list)]

    res = morphological_analysis(ca_param)
    pprint.pprint(res)


if __name__ == '__main__':
    main()
    # morphological_analysis_test()
