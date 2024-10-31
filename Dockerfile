FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED 1

COPY requirements*.txt /

RUN pip install --upgrade --no-cache-dir pip && \
    pip install --no-cache-dir -r /requirements.txt -r requirements-dev.txt

COPY *.py /apps/app/
WORKDIR /apps/app
RUN pws ; ls  -lha

WORKDIR /apps/app
CMD ["python", "s3sync.py"]
