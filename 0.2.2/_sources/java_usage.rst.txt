Java Usage
==========
Bunsen allows any org.hl7.fhir.dstu3.model.* Java object to be encoded as an Apache Spark Dataset,
transformed and queried, and then optionally decoded back to the original Java forms.

Creating Encoders
-----------------
Here is a simple example of converting a local list of FHIR Condition objects into
a Dataset and some simple queries of that dataset:

.. code-block:: java

    FhirEncoders encoders = FhirEncoders.forStu3().getOrCreate();

    List<Condition> conditionList = // A list of org.hl7.fhir.dstu3.model.Condition objects.

    Dataset<Condition> conditions = spark.createDataset(conditionList,
        encoders.of(Condition.class));

    // Query for conditions based on arbitrary Spark SQL expressions
    Dataset<Condition> activeConditions = conditions
        .where("clinicalStatus == 'active' and verificationStatus == 'confirmed'");

    // Count the query results
    long activeConditionCount = activeConditions.count();

    // Convert the results back into a list of org.hl7.fhir.dstu3.model.Condition objects.
    List<Condition> retrievedConditions = activeConditions.collectAsList();


Converting Existing Datasets
----------------------------
In other cases, users may have an existing Spark Dataset of FHIR resources in their JSON
or XML forms. In this case, the `HAPI FHIR <http://hapifhir.io>`_ APIs in Spark map functions
to convert to the data model, and then use Bunsen to encode the produced data mdoel as FHIR
resources.

.. code-block:: java

    // Created as a static field to avoid creation costs on each invocation.
    private static final FhirContext ctx = FhirContext.forDstu3();

    // <snip>

    FhirEncoders encoders = FhirEncoders.forStu3().getOrCreate();

    Dataset<String> conditionJsons = // A Dataset of FHIR conditions in JSON form.

    Dataset<Condition> conditions = conditionJsons.map(
        (MapFunction<String,Condition>) conditionString -> {
          return (Condition) ctx.newJsonParser().parseResource(conditionString);
        },
        encoders.of(Condition.class));

    // Arbitrary queries or further transformations the the conditions Dataset goes here.