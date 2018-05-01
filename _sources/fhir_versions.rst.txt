FHIR Versions
=============
Bunsen supports STU3 and R4 builds of the FHIR specification. Support for both are included as
a convenience in the bunsen-shaded JAR and assembly, so Python users can simply use the
:doc:`stu3_pythonapis` or :doc:`r4_pythonapis` libraries as needed.

Java or Scala developers building their own assemblies similarly can use the bunsen-stu3 or
bunsen-r4 jars.

Importing Older FHIR Data
-------------------------
DSTU2 and older versions of FHIR aren't supported for technical reasons
`discussed here <https://github.com/cerner/bunsen/issues/1#issuecomment-383436372>`_. However,
older versions of FHIR can be converted to newer versions using the
`HAPI FHIR Converter API <http://hapifhir.io/doc_converter.html>`_. This can be run externally,
or embedded in a Spark function that reads the XML or JSON resource directly, converts it to
the STU3 or R4 model as appropriate, and then creates a Spark Dataset of the supported type.