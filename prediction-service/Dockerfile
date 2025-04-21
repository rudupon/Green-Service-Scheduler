FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p models

COPY requirements.txt .
COPY prediction_service.py .
COPY models/ models/

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x prediction_service.py

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["python", "prediction_service.py"]