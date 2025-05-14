FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/data

COPY data_service.py .
COPY solar_data_cleaned_1-25_april_home.csv /app/data/

EXPOSE 5000

CMD ["python", "data_service.py"]