FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion/ .
COPY data/ ./data/
ENV BATCH_INPUT_FOLDER=/app/data/batch
CMD ["python", "batch_ingest.py"]