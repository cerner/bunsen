# Create Docker container that includes the Bunsen library along with PySpark.
# See https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html#apache-spark for
# usage patterns.
FROM jupyter/pyspark-notebook:latest

ARG BUNSEN_VERSION

COPY ./bunsen-assembly-${BUNSEN_VERSION} /usr/local/bunsen-assembly-${BUNSEN_VERSION}

USER root
RUN ln -s /usr/local/bunsen-assembly-${BUNSEN_VERSION} /usr/local/bunsen

ENV PYTHONPATH $PYTHONPATH:/usr/local/bunsen/python
ENV PYSPARK_SUBMIT_ARGS --jars /usr/local/bunsen/jars/bunsen-shaded-$BUNSEN_VERSION.jar pyspark-shell