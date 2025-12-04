# Берём за основу официальный Airflow
FROM apache/airflow:2.7.1

# Переключаемся на root, чтобы установить Java (нужна для Spark)
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

# Настраиваем переменную JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

# Возвращаемся к пользователю airflow и ставим библиотеки
USER airflow
RUN pip install pyspark==3.5.0 boto3 pandas