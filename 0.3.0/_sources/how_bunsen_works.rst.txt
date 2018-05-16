How Bunsen Works
================
Bunsen encodes FHIR data in Apache Spark by generating `Spark Encoders <https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html>`_
from the FHIR resource definitions. The encoders convert the FHIR data model
into a Spark schema on a field-by-field, type-by-type basis. At runtime, they automatically generate
byte code that serialises FHIR objects as Spark Datasets, which are an efficient binary representation
that can be analysed at petabyte scale. Those datasets can then be saved or manipulated like any other
Spark data.

Because the Apache Spark schema and encoder bytecode is generated directly from the FHIR definitions,
the resulting Spark dataset is a faithful representation of the original data. To illustrate, this,
here is a excerpt of calling Spark's printSchema() method on the observations dataset we created earlier.
Notice that every field is a one-to-one mapping to its FHIR equivalent:

>>> observations.printSchema()
root
 |-- id: string (nullable = true)
 |-- meta: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- versionId: string (nullable = true)
<snip>
 |-- status: string (nullable = true)
 |-- category: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- coding: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |-- version: string (nullable = true)
 |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |-- text: string (nullable = true)
 |-- code: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- coding: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
<snip>
 |-- valueDateTime: string (nullable = true)
 |-- valueQuantity: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- value: decimal(12,4) (nullable = true)
 |    |-- comparator: string (nullable = true)
 |    |-- unit: string (nullable = true)
 |    |-- system: string (nullable = true)
 |    |-- code: string (nullable = true)
 |-- valueRatio: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- numerator: struct (nullable = true)
<snip>

By encoding FHIR in this native Spark format, Bunsen can take advantages of Spark's performance and scalability traits.
For instance, analytic queries over hundreds of millions of results typically complete in single-digit seconds.