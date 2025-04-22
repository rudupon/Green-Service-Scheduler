FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY node_controller.py .
COPY service_clients.py .

RUN chmod +x node_controller.py

ENV PYTHONBUFFERED=1

CMD ["python", "node_controller.py"]