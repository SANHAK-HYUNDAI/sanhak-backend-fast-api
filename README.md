<p align="center">
<img src="https://user-images.githubusercontent.com/73744183/232030952-cb08ddc9-818c-437b-8dc8-aec63d9042f0.png">
</p>

<p align="center">
<img src="https://img.shields.io/badge/python-3.9-blue">
<img src="https://img.shields.io/badge/FastAPI-0.88.0-blue">
<img src="https://img.shields.io/badge/version-v1.0.0-blue">
<img src="https://img.shields.io/badge/license-MIT-brightgreen.svg"/>
</p>

<h1 align="center">Sanhak Backend FastAPI</h1>


### Purpose

- 대용량 파일 업로드를 위한 비동기 API 구현
- 자연어 처리를 통해 네이터 카페 data와 repair data를 유사도 높은 DATA와 서로 맵핑
- 유사도 연결한 데이터를 대시보드에 출력하기 위해서 DB에 DATA 적재
- CQRS pattern에서 command 역할인 고비용의 작업(유사도 분석, 대량의  data 저장) 처리
![sanhak-st](https://user-images.githubusercontent.com/73744183/232180297-65a5e893-5314-4e58-916e-57aa531cc468.svg)


### Performance
- 콜백, 람다와 같은 함수형 프로그래밍 기법을 사용하여 확장성과 유지보수성을 고려
    - '무엇'보다는 '어떻게' 데이터를 처리할 것인지에 집중하는 기능 설계
- 대용량 데이터의 빠른 유사도 분석을 위해서 멀티 프로세스 모듈 개발
    - 확장성, 재사용성 높은 모듈 개발을 위해서 핵심 로직 및 환경변수(유사도 분석 로직, cpu core 수)를 변경할 수 있도록 구현
    - 멀티 프로세스 동작을 지원하여 하드웨어 환경에 따라서 추가적인 성능 향상을 기대 가능
        - process pool size : 4 --> <strong>2.5배</strong> 이상의 시간 단축 확인
- bulk insert(update)를 이용한 대량의 정보 저장 효율 향상
    - 단일 insert 대비 <strong>2.6배</strong>의 성능 향상

### Data Flow

![ca_data_flow_chart drawio](https://user-images.githubusercontent.com/73744183/210063395-24f3761f-fcdb-4b49-9ae5-0148175d32ad.svg)


### How to use

1. 파이썬 모듈 일괄 설치

```
pip install -r requirements.txt
```

2. konlpy mecab 설치

[설치하기 - KoNLPy 0.4.3 documentation](https://konlpy-ko.readthedocs.io/ko/v0.4.3/install/)

3. 로컬에서 fast api 실행

```
uvicorn --reload main:app
```

```
uvicorn --reload --host 127.0.0.1 --port 8080 main:app
```

### Database Setting(ERD)

<img width="966" alt="sanhak-erd-re" src="https://user-images.githubusercontent.com/73744183/211250981-252203d7-09e9-4be6-9d04-f0f7d51b1f57.png">

#### Database Init DDL File Download

Download : [init.zip](https://github.com/SANHAK-HYUNDAI/sanhak-backend-fast-api/files/10370876/init.zip)

### Skill Set

> #### Backend
> - fastapi
> - uvicorn
> - scikit-learn
> - loguru
> - pandas
> - nltk
> - konlpy
> - mecab
> - aiomysql
> - openpyxl
>
> #### Database
> - mysql


