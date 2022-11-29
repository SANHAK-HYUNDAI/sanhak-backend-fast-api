import pandas as pd
from bs4 import BeautifulSoup
from hanspell import spell_checker
import nltk
import re
import warnings

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


async def ro_upload(RO_df):
    # 사용자 불용어 사전
    stop_words = "1. 2. 3. 4. 1.1 1.2 2.1 2.2 3.1 3.2 4.1 4.2 점검 점검 및 원인 점검 사항 현상 조치 조치내용 점검내용 요망사항 현상: 점검: 내용 요망 사항 점검내용및원인"
    stop_words = set(stop_words.split(' '))

    RO_df_number = RO_df.shape[0]
    print("number : ", RO_df_number)

    if RO_df_number >= 8497:
        for i in range(8496, RO_df_number):
            try:
                # [T][A]와 같은 단어 제거
                if RO_df['special_note'][i][0] == '[':
                    RO_df['special_note'][i] = RO_df['special_note'][i][3:]

                # 한국어만 정제
                if confirm_kor_or_eng(RO_df['special_note'][i]) != '한국어':
                    RO_df = RO_df.drop(index=i, axis=0)

                # html 태그 제거
                result = BeautifulSoup(RO_df['special_note'][i], 'html.parser').text
                RO_df['special_note'][i] = result

                # 특수기호 제거
                result = re.sub(r'[^ ㄱ-ㅣ가-힣]', ' ', RO_df['special_note'][i])
                RO_df['special_note'][i] = result

                # py-hansell 맞춤법 검사
                spelled_sent = spell_checker.check(RO_df['special_note'][i])
                hanspell_sent = spelled_sent.checked
                RO_df['special_note'][i] = hanspell_sent

                # 사용자 정의 불용어 제거
                clean_words = []
                for word in nltk.tokenize.word_tokenize(RO_df['special_note'][i]):
                    if word not in stop_words:  # 불용어 제거
                        clean_words.append(word)
                        result = ' '.join(clean_words)
                        RO_df['special_note'][i] = result

            except:
                # print("except : ", i)
                pass

    RO_df.dropna(inplace=True)
    return RO_df
