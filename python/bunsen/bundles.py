"""
Support for loading FHIR bundles into Bunsen. This includes the following features:

* Allow users to load bundles from a given location
* Convert bundle entries into Spark Dataframes
* Save all entities with a bundle collection to a distinct table for each (e.g., an observation table, a condition table, and so on.)
* Converts the results of a Bunsen query back into bundles that can then be used elsewhere.

See the methods below for details.
"""
from pyspark.sql import DataFrame
import json

def load_from_directory(sparkSession, path, minPartitions=1):
    """
    Returns a Java RDD of bundles loaded from the given path. Note this
    RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like extract_entry.
    """

    bundles = sparkSession._jvm.com.cerner.bunsen.Bundles
    return bundles.loadFromDirectory(sparkSession._jsparkSession, path, minPartitions)

def from_json(df, column):
    """
    Takes a dataframe with JSON-encoded bundles in the given column and returns
    a Java RDD of Bundle records. Note this
    RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like extract_entry.
    """
    bundles = df._sc._jvm.com.cerner.bunsen.Bundles
    return bundles.fromJson(df._jdf, column)

def from_xml(df, column):
    """
    Takes a dataframe with XML-encoded bundles in the given column and returns
    a Java RDD of Bundle records. Note this
    RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like extract_entry.
    """
    bundles = df._sc._jvm.com.cerner.bunsen.Bundles
    return bundles.fromXml(df._jdf, column)


def extract_entry(sparkSession, javaRDD, resourceName):
    """
    Returns a dataset for the given entry type from the bundles.
    """

    bundles = sparkSession._jvm.com.cerner.bunsen.Bundles
    return DataFrame(
            bundles.extractEntry(sparkSession._jsparkSession, javaRDD, resourceName), 
            sparkSession)


def save_as_database(sparkSession, path, databaseName, *resourceNames, **kwargs):
    """
    Loads the bundles in the path and saves them to a database, where
    each table in the database has the same name of the resource it represents.
    """

    gateway = sparkSession.sparkContext._gateway
    namesArray = gateway.new_array(gateway.jvm.String, len(resourceNames))
    for idx, name in enumerate(resourceNames):
        namesArray[idx] = name

    rdd = load_from_directory(sparkSession,path, kwargs.get('minPartitions', 1))

    if (kwargs.get('cache', True)):
        rdd.cache()

    bundles = sparkSession._jvm.com.cerner.bunsen.Bundles

    bundles.saveAsDatabase(sparkSession._jsparkSession, rdd, databaseName, namesArray)

    if (kwargs.get('cache', True)):
        rdd.unpersist()

def to_bundle(sparkSession, dataset):
    """
    Converts a dataset of FHIR resources to a bundle containing those resources.
    Use with caution against large datasets.
    """

    jvm = sparkSession._jvm

    json_string = jvm.com.cerner.bunsen.python.Functions.toJsonBundle(dataset._jdf)

    return json.loads(json_string)