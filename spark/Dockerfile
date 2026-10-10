# Image officielle Apache Spark 3.5.1 + Python 3 (Ubuntu 22.04, Python 3.10).
# NB : l'image « apache/spark-py » citée dans le sujet n'est plus la référence pour Spark 3.5 ;
# l'équivalent officiel est apache/spark:3.5.1-python3 (voir ADR du README).
FROM apache/spark:3.5.1-python3

ENV TZ=UTC PYTHONUNBUFFERED=1

# Connecteur Kafka embarqué dans l'image (pas de téléchargement Ivy au runtime, donc pas de
# dépendance réseau ni de répertoire ~/.ivy2 à rendre inscriptible pour l'utilisateur non-root).
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# Root uniquement le temps du build (droits des jars, pip, dossiers) : l'exécution se fait en « spark ».
USER root
COPY requirements.txt /tmp/requirements.txt
RUN chmod 644 /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
              /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
              /opt/spark/jars/kafka-clients-3.4.1.jar \
              /opt/spark/jars/commons-pool2-2.11.1.jar \
 && pip3 install --no-cache-dir -r /tmp/requirements.txt \
 && mkdir -p /checkpoints /app \
 && chown spark:spark /checkpoints /app
USER spark

WORKDIR /app
COPY --chown=spark:spark jobs/ /app/jobs/

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "1g", "/app/jobs/streaming.py"]
