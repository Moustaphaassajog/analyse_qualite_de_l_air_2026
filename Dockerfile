# Utiliser l'image officielle Spark avec Python 3
FROM apache/spark:3.4.0-python3

# Passer en root pour pouvoir installer les paquets Python
USER root

# Mettre à jour et installer pip si nécessaire
RUN apt-get update && apt-get install -y python3-pip

# Installer les modules Python nécessaires
RUN pip3 install --no-cache-dir requests kafka-python python-dotenv

#Integration pyspark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Créer le dossier de travail dans le conteneur et mettre comme WORKDIR
WORKDIR /app/spark

# Copier tout le code local 'spark' dans le conteneur
COPY spark/ /app/spark/

# Revenir à l'utilisateur spark pour la sécurité
USER spark

# Point d'entrée : rester dans bash par défaut pour tester
CMD ["bash"]
