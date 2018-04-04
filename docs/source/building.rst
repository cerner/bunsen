Building Bunsen
===============
Bunsen is built with Apache Maven 3.3 or higher running in an environment with Java 8 or newer.
Python users will also want to create a virtual environment with Python 2.7 or Python 3.4 or newer.

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
Running Bunsen's Python tests requires the Apache Spark and Python dependencies be available. Once
Python itself is installed on the build machine, users may want to create a separate vitualenv for
the project to isolate it from other usage.

Installing Bunsen's python dependencies is done by running the following command in the Bunsen project root directory:

>>> make --directory python

After that, users can simply run the "mvn clean install" command as seen below to build, test,
and install the project in the local Maven repository:

>>> mvn clean install

Users who have an existing Spark installation they want to use can set the SPARK_HOME For instance,
if you have a Spark installation in /usr/local/spark you want to use for the Python tests, you can
set this before building the project:

>>> export SPARK_HOME=/usr/local/spark
