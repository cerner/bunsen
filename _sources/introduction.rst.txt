.. toctree::
   :name: introduction
   :includehidden:

Getting Started
===============
Bunsen offers first-class integration with Apache Spark for Python, Java, and Scala users. This page
describes the steps to get started with them.

Initial Setup
-------------
Scala or Java users of Spark  can simply add the bunsen-shaded JAR to the Spark job. PySpark
users wanting to use the provided wrapper functions will also need to include the Python files
in the PYTHONPATH, all of which can be found in the assembly:

>>> unzip bunsen-assembly-0.1.0-dist.zip
>>> export PYTHONPATH=$PWD/bunsen-assembly-0.1.0/python:$PYTHONPATH
>>> pyspark --jars bunsen-assembly-0.1.0/jars/bunsen-shaded-0.1.0.jar

Bunsen currently uses FHIR STU3 and Spark 2.1. The assembly itself can be downloaded from
`Maven Central <https://repo.maven.apache.org/maven2/com/cerner/>`_.

(Note: the above link will be updated when the first release lands on Maven Central.)

Simple Queries
--------------
A production deployment would typically bulk load large volumes of data, but the easiest way to experiment
with Bunsen is simply to pull in some existing FHIR bundles.

>>> from bunsen.bundles import load_from_directory, extract_entry
>>>
>>> bundles = load_from_directory(spark, 'path/to/test/bundles')
>>>
>>> # The extract_entry method returns a Spark dataframe containing all observations
>>> # that were in the bundle. We can then perform arbitrary Spark operations on them.
>>> observations = extract_entry(spark, bundles, 'observation').cache()
>>>
>>> observations.select('subject.reference', 'code.text').limit(5).show(truncate=False)
+---------------+-------------------------------------+
|reference      |text                                 |
+---------------+-------------------------------------+
|Patient/1032702|Tobacco smoking status               |
|Patient/9995679|Blood pressure systolic and diastolic|
|Patient/9995679|Systolic blood pressure              |
|Patient/9995679|Diastolic blood pressure             |
|Patient/9995679|Blood pressure systolic and diastolic|
+---------------+-------------------------------------+


See the :py:mod:`~bunsen.bundles` module for details on use.

Spark SQL Integration
---------------------
Once the FHIR data is available in Spark, it can be registered as a table or saved to a Hive
database, and then queried with the full power of SQL.

Bunsen also supports basic use of simple value sets to simplify queries. In the example below
we will register our observations dataframe, and we declare some value sets based on standard
terminologies. We use the set of all values with a transitive is-a relationship
in the given termoniology.

>>> observations.registerTempTable('observations')
>>> from bunsen.valuesets import push_valuesets, isa_loinc, isa_snomed
>>> push_valuesets(spark,
>>>                {'body_weight'          : isa_loinc('29463-7'),
>>>                 'bmi'                  : isa_loinc('39156-5'),
>>>                 'heart_rate'           : isa_loinc('8867-4'),
>>>                 'abnormal_weight_loss' : isa_snomed('267024001'),
>>>                 'stroke'               : isa_snomed('230690007')})

Now we can query our data with standard Spark SQL using the in_valueset user-defined function
to reference the valuesets used above. See the :py:mod:`~bunsen.valuesets` module for details on use.

>>> spark.sql("""
>>> select subject.reference,
>>>        effectiveDateTime,
>>>        valueQuantity.value
>>> from observations
>>> where in_valueset(code, "heart_rate")
>>> limit 5
>>> """).show()
+---------------+-----------------+-------+
|      reference|effectiveDateTime|  value|
+---------------+-----------------+-------+
|Patient/9995679|       2006-12-27|54.0000|
|Patient/9995679|       2007-04-18|60.0000|
|Patient/9995679|       2007-07-18|80.0000|
|Patient/9995679|       2008-01-16|47.0000|
|Patient/9995679|       2008-06-25|47.0000|
+---------------+-----------------+-------+

Bring Your Own Value Sets
-------------------------
The above examples show is-a relationships in standard ontologies, but users can also
bring their own datasets or import them from sources like the Value Set Authority Center.

To do so, import the value set into a list of (code system, code value) tuples,
then use the push :py:func:`~bunsen.valuesets.push_valuesets` function to broadcast
them to the cluster. Here's an example:

>>> hypertension_meds = \
>>>  [('http://snomed.info/sct', '68180051503'),
>>>   ('http://snomed.info/sct', '68180048003')]
>>>
>>> # Push a combination of is-a relationships and our own value sets.
>>> push_valuesets(spark,
>>>                {'hypertension'  : isa_snomed('59621000'),
>>>                 'glucose_level' : isa_loinc('2345-7'),
>>>                 'hypertension_meds' : hypertension_meds})

Once those valuesets are pushed to the cluster, we can use them in our queries
like any other:

>>> spark.sql("""
>>> select subject.reference
>>> from medicationstatements
>>> where in_valueset(medicationCodeableConcept, 'hypertension_meds')
>>> """).show()
+---------------+
|      reference|
+---------------+
|Patient/9995467|
|Patient/9995467|
|Patient/9995467|
|Patient/9995467|
|Patient/9995467|
+---------------+
