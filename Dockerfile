FROM apache/airflow:2.10.3

USER root
RUN apt-get update && \
    apt install -y default-jdk && \
    apt-get install wget && \
    apt-get install etl-dev && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
# RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy environment vars
COPY .env /opt/airflow/.env

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar

# RUN spark-submit --jars aws-java-sdk-bundle-1.12.540.jar,hadoop-aws-3.3.4.jar /opt/dags/trainModel.py
