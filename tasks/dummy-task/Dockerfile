FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir numpy psutil

COPY dummy_task.py .

CMD ["python", "dummy_task.py"]