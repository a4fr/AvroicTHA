FROM python:3.13

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY kafka-consumer.py .

# Set the entry point to run the kafka-consumer script
ENTRYPOINT ["python", "kafka-consumer.py"]
