Building Bunsen
===============
Bunsen is built with Apache Maven 3.3 or higher running in an environment with Java 8 or newer.
Python users will also want to create a virtual environment with at least Python 3.4.

Simple Builds
-------------
Bunsen can be built and packaged by simply running the following command in any environment
where Maven is installed:

>>> mvn clean install -Dskip.python.tests=true

This will produce a bunsen-assembly zip file that can be unzipped in used in any Spark or PySpark
environment like this:

>>> unzip bunsen-assembly-0.x.y-dist.zip
>>> export PYTHONPATH=$PWD/bunsen-assembly-0.x.y/python:$PYTHONPATH
>>> pyspark --jars bunsen-assembly-0.1.0/jars/bunsen-shaded-0.x.y.jar

Running Python Tests
--------------------
Running Bunsen's Python tests requires Apache Spark to be installed on the build machine. This
requires some additional steps.

First, install PySpark, optionally in a virtual environment:

>>> pip install pyspark

Depending on the version of Spark you're using, you may also need to set the SPARK_HOME environment variable.
It can be pointed to a Spark installation that was downloaded from
`Apache Spark site <https://spark.apache.org/downloads.html>`_, or to the /site-packages/pyspark folder under your Python
virtual environment.

For instance, if you have a Spark 2.2.1 installation in /usr/local/spark, you can set this:

>>> export SPARK_HOME=/usr/local/spark

With these prerequisites in place, the full project including the Python tests can be run by simply
calling "mvn clean install" in the project root.