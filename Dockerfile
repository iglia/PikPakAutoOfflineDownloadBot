# syntax=docker/dockerfile:1 
FROM python:3.11-slim-buster 
WORKDIR /code 
COPY ./requirements.txt /code/requirements.txt 
RUN pip3 install --no-cache-dir -r requirements.txt 
COPY . /code
RUN python3 --version
CMD [ "python3", "pikpakTgBot.py"]

