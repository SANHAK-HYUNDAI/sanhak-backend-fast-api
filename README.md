# SANHAK Backend Fast-API(Python)

### Purpose
 
 - 대용량 파일 업로드를 위한 비동기 API 구현
 - 자연어 처리를 통해 네이터 카페 DATA와 Repair DATA를 유사도 높은 DATA와 서로 맵핑
 - 유사도 연결한 데이터를 대시보드에 출력하기 위해서 DB에 DATA 적재

### Performance

 - 대용량 데이터의 빠른 유사도 분석을 위해서 멀티 프로세스 모듈 개발
   - 확장성, 재사용성 높은 모듈 개발을 위해서 핵심 로직 및 환경변수(유사도 분석 로직, cpu core 수)를 변경할 수 있도록 구현
   - 멀티 프로세스 동작을 지원하여 하드웨어 환경에 따라서 추가적인 성능 향상을 기대 가능
     - process pool size : 4 --> <strong>2.5배</strong> 이상의 시간 단축 확인
 - bulk insert(update)를 이용한 대량의 정보 저장 효율 향상
   - 단일 insert 대비 <strong>2.6배</strong>의 성능 향상

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

