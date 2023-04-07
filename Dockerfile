FROM rhlehfndvkd7557/sanhak-base-image:2
#WORKDIR app
COPY . .
RUN bash ./script/mecab.sh
ENTRYPOINT uvicorn --reload --port 8777 --host 0.0.0.0 main:app