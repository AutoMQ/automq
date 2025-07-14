FROM tabulario/spark-iceberg:latest

RUN pip install --upgrade confluent-kafka[avro] Faker pyspark

RUN rm -rf /home/iceberg/notebooks
COPY notebooks /home/iceberg/notebooks
