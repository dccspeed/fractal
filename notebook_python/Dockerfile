# Use Alpine Linux as the base image
FROM alpine:latest

# Environment
ENV SPARK_HOME=/app/spark
ENV FRACTAL_HOME=/app/fractal

# Set the working directory in the container
COPY . $FRACTAL_HOME
WORKDIR /app

RUN apk --no-cache add --update py3-pip python3 gcc python3-dev musl-dev linux-headers graphviz graphviz-dev
RUN python -m venv /opt/venv
# Enable venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install networkx[default,extra] dill pydot jupyterlab jupyterlab-vim

RUN apk --no-cache add --update openjdk11 wget tar bash git coreutils procps libstdc++
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz && \
    tar xf spark-3.5.0-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.5.0-bin-hadoop3-scala2.13 spark && \
    rm spark-3.5.0-bin-hadoop3-scala2.13.tgz

RUN pip install $SPARK_HOME/python
RUN pip install $FRACTAL_HOME/python

RUN apk del wget tar git gcc python3-dev musl-dev linux-headers

ENTRYPOINT ["jupyter", "lab","--ip=0.0.0.0","--allow-root"]

