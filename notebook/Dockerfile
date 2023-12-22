FROM almondsh/almond:latest

WORKDIR /app

USER root
RUN apt-get -y update && \
    apt-get install --no-install-recommends -y \
      openjdk-8-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV FRACTAL_HOME=/app/fractal
COPY . $FRACTAL_HOME

RUN cd $FRACTAL_HOME && ./gradlew jar && \
    mv $FRACTAL_HOME/fractal-core/build/libs/*.jar /app && \
    mv $FRACTAL_HOME/notebook/*.ipynb /app && \
    mv $FRACTAL_HOME/data /app && \
    rm -rf $FRACTAL_HOME && rm -rf .ivy2

RUN chown -R $NB_UID /app
USER $NB_UID