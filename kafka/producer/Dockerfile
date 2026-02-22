FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY live_score_producer.py .

CMD ["python", "live_score_producer.py"]