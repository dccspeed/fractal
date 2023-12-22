# Use Alpine Linux as the base image
FROM alpine:latest

# Environment
ENV SPARK_HOME=/app/spark
ENV FRACTAL_HOME=/app/fractal

# Set the working directory in the container
COPY . $FRACTAL_HOME
WORKDIR /app

RUN apk --no-cache add openjdk11 wget tar bash git coreutils procps libstdc++
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz && \
    tar xf spark-3.5.0-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.5.0-bin-hadoop3-scala2.13 spark && \
    rm spark-3.5.0-bin-hadoop3-scala2.13.tgz
RUN cd $FRACTAL_HOME && ./gradlew jar

# Used to force spark-submit to download packages and store into image
ENV app=motifs_po
ENV steps=2
ENV inputgraph=/app/fractal/data/citeseer
RUN $FRACTAL_HOME/bin/fractal.sh && rm -rf /root/.gradle && apk del wget tar git

RUN cd $FRACTAL_HOME && ./gradlew jar

# Fractal built-in runner as default command
CMD ["/bin/bash", "-c",  "/app/fractal/bin/fractal.sh"]