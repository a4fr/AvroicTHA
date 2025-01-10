FROM bitnami/spark:3.5.0

ENV JAVA_HOME=/opt/bitnami/java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY spark-job.py .

CMD ["python", "spark-job.py"]
