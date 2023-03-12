#FROM ubuntu:22.04
#RUN apt update | apt upgrade
#RUN apt-get install -y g++ curl openjdk-8-jdk python3-dev python3-pip bash automake curl
#RUN python3 -m pip install --upgrade pip

FROM base-image
# 디렉토리 이동 후 프로젝트 복사
WORKDIR app
COPY . .
RUN apt-get install -y git
RUN pip install -r requirements.txt
RUN pip install konlpy
RUN bash ./script/mecab.sh

# 시작
EXPOSE 8777
ENTRYPOINT uvicorn --reload --port 8777 --host 0.0.0.0 main:app