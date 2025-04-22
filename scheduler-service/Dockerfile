FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY scheduler_service.py .

ENV PYTHONBUFFERED=1

EXPOSE 80

CMD ["python", "scheduler_service.py"]