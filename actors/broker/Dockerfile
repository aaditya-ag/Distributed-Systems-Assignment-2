# syntax=docker/dockerfile:1

FROM python:3.10.6-slim-bullseye

WORKDIR /broker_app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=5000"]