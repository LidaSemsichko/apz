FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn sqlalchemy psycopg2-binary hazelcast-python-client pydantic

COPY main.py /app/main.py

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]