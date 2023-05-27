FROM python:3.10-slim-buster

RUN pip install --no-cache-dir pandas fastapi uvicorn

WORKDIR /src
COPY main.py .
ENTRYPOINT [ "uvicorn" ]