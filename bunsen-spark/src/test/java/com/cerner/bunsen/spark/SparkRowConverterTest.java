package com.cerner.bunsen.spark;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.stu3.TestData;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Medication;
import org.hl7.fhir.dstu3.model.MedicationRequest;
import org.hl7.fhir.dstu3.model.Meta;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Provenance;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.exceptions.FHIRException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkRowConverterTest {

  private static SparkSession spark;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    spark.stop();
  }

  static FhirContext fhirContext;

  private static final String BASE_VALUESET = "ValueSet";

  private static final Patient testPatient = TestData.newPatient();

  private static Dataset<Row> testPatientDataset;

  private static Patient testPatientDecoded;

  private static final Observation testObservation = TestData.newObservation();

  private static Dataset<Row> testObservationDataset;

  private static Observation testObservationDecoded;

  private static final Observation testObservationNullStatus = TestData.newObservation()
      .setStatus(Observation.ObservationStatus.NULL);

  private static Dataset<Row> testObservationNullStatusDataset;

  private static Observation testObservationDecodedNullStatus;

  private static final Condition testCondition = TestData.newCondition();

  private static Dataset<Row> testConditionDataset;

  private static Condition testConditionDecoded;

  private static final Medication testMedicationOne = TestData.newMedication("test-medication-1");

  private static final Medication testMedicationTwo = TestData.newMedication("test-medication-2");

  private static final Provenance testProvenance = TestData.newProvenance();

  private static final MedicationRequest testMedicationRequest =
      (MedicationRequest) TestData.newMedicationRequest()
          .addContained(testMedicationOne)
          .addContained(testProvenance)
          .addContained(testMedicationTwo);

  private static Dataset<Row> testMedicationRequestDataset;

  private static MedicationRequest testMedicationRequestDecoded;

  private static final Patient testBunsenTestProfilePatient = TestData
      .newBunsenTestProfilePatient();

  private static Dataset<Row> testBunsenTestProfilePatientDataset;

  private static Patient testBunsenTestProfilePatientDecoded;


  /**
   * Loads resource definitions used for testing.
   */
  @BeforeClass
  public static void loadDefinition() throws IOException {

    fhirContext = FhirContexts.forStu3();

    SparkRowConverter patientConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_PATIENT);

    Row testPatientRow = patientConverter.resourceToRow(testPatient);

    testPatientDataset = spark.createDataFrame(Collections.singletonList(testPatientRow),
        patientConverter.getSchema());

    testPatientDecoded = (Patient) patientConverter.rowToResource(testPatientDataset.head());

    SparkRowConverter observationConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_OBSERVATION);

    Row testObservationRow = observationConverter.resourceToRow(testObservation);

    testObservationDataset = spark.createDataFrame(Collections.singletonList(testObservationRow),
        observationConverter.getSchema());

    testObservationDecoded =
        (Observation) observationConverter.rowToResource(testObservationDataset.head());

    Row testObservationNullStatusRow = observationConverter
        .resourceToRow(testObservationNullStatus);

    testObservationNullStatusDataset = spark.createDataFrame(
        Collections.singletonList(testObservationNullStatusRow), observationConverter.getSchema());

    testObservationDecodedNullStatus = (Observation) observationConverter
        .rowToResource(testObservationNullStatusDataset.head());

    SparkRowConverter conditionConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_CONDITION);

    Row testConditionRow = conditionConverter.resourceToRow(testCondition);

    testConditionDataset = spark.createDataFrame(Collections.singletonList(testConditionRow),
        conditionConverter.getSchema());

    testConditionDecoded =
        (Condition) conditionConverter.rowToResource(testConditionDataset.head());

    SparkRowConverter medicationRequestConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_MEDICATION_REQUEST,
        Arrays.asList(TestData.US_CORE_MEDICATION, TestData.PROVENANCE));

    Row testMedicationRequestRow = medicationRequestConverter.resourceToRow(testMedicationRequest);

    testMedicationRequestDataset = spark.createDataFrame(
        Collections.singletonList(testMedicationRequestRow),
        medicationRequestConverter.getSchema());

    testMedicationRequestDecoded = (MedicationRequest) medicationRequestConverter
        .rowToResource(testMedicationRequestDataset.head());

    SparkRowConverter converterBunsenTestProfilePatient = SparkRowConverter
        .forResource(FhirContexts.forStu3(), TestData.BUNSEN_TEST_PATIENT);

    Row testBunsenTestProfilePatientRow = converterBunsenTestProfilePatient
        .resourceToRow(testBunsenTestProfilePatient);

    testBunsenTestProfilePatientDataset = spark
        .createDataFrame(Collections.singletonList(testBunsenTestProfilePatientRow),
            converterBunsenTestProfilePatient.getSchema());

    testBunsenTestProfilePatientDecoded = (Patient) converterBunsenTestProfilePatient
        .rowToResource(testBunsenTestProfilePatientRow);
  }

  @Test
  public void testInteger() {

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
        testPatientDataset.select("multipleBirth.integer").head().get(0));

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
        ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());
  }

  @Test
  public void testDecimal() {

    BigDecimal originalDecimal = ((Quantity) testObservation.getValue()).getValue();

    // Use compareTo since equals checks scale as well.
    Assert.assertTrue(originalDecimal.compareTo(
        (BigDecimal) testObservationDataset.select(
            "value.quantity.value").head().get(0)) == 0);

    Assert.assertEquals(originalDecimal.compareTo(
        ((Quantity) testObservationDecoded
            .getValue())
            .getValue()), 0);
  }

  @Test
  public void testPrimitiveMultiplicity() {

    Assert.assertTrue(testPatient.getName().get(0).getFamily()
        .equalsIgnoreCase(testPatientDecoded.getName().get(0).getFamily()));
    Assert.assertTrue(testPatient.getName().get(0).getGiven().get(0).getValueAsString()
        .equals(testPatientDecoded.getName().get(0).getGiven().get(0).getValueAsString()));
    Assert.assertTrue(testPatient.getName().get(0).getGiven().get(1).getValueAsString()
        .equals(testPatientDecoded.getName().get(0).getGiven().get(1).getValueAsString()));
  }

  @Test
  public void testChoice() throws FHIRException {

    // Ensure that a decoded choice type matches the original
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(testPatientDecoded.getMultipleBirth()));
  }

  @Test
  public void testBoundCode() {

    Assert.assertEquals(testObservation.getStatus().toCode(),
        testObservationDataset.select("status").head().get(0));

    Assert.assertEquals(testObservation.getStatus(),
        testObservationDecoded.getStatus());
  }

  @Test
  public void testBoundCodeNull() {

    Assert.assertNull(testObservationNullStatusDataset.select("status").head().get(0));

    Assert.assertNull(testObservationDecodedNullStatus.getStatusElement().getValue());
  }

  @Test
  public void testCoding() {

    Coding testCoding = testCondition.getSeverity().getCodingFirstRep();
    Coding decodedCoding = testConditionDecoded.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields so we can easily select and compare individual fields.
    Dataset<Row> severityCodings = testConditionDataset
        .select(functions.explode(testConditionDataset.col("severity.coding"))
            .alias("coding"))
        .select("coding.*") // Pull all fields in the coding to the top level.
        .cache();

    Assert.assertEquals(testCoding.getCode(),
        severityCodings.select("code").head().get(0));
    Assert.assertEquals(testCoding.getCode(),
        decodedCoding.getCode());

    Assert.assertEquals(testCoding.getSystem(),
        severityCodings.select("system").head().get(0));
    Assert.assertEquals(testCoding.getSystem(),
        decodedCoding.getSystem());

    Assert.assertEquals(testCoding.getUserSelected(),
        severityCodings.select("userSelected").head().get(0));
    Assert.assertEquals(testCoding.getUserSelected(),
        decodedCoding.getUserSelected());

    Assert.assertEquals(testCoding.getDisplay(),
        severityCodings.select("display").head().get(0));
    Assert.assertEquals(testCoding.getDisplay(),
        decodedCoding.getDisplay());
  }

  @Test
  public void testSingleReference() {

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDataset.select("subject.reference").head().get(0));

    Assert.assertEquals("12345", testConditionDataset
        .select("subject.patientId").head().get(0));

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testMultiReferenceTypes() {

    // Row containing the general practitioner from our dataset.
    Row practitioner = testPatientDataset
        .select(functions.explode(functions.col("generalpractitioner")))
        .select("col.organizationId", "col.practitionerId")
        .head();

    String organizationId = practitioner.getString(0);
    String practitionerId = practitioner.getString(1);

    // The reference is not of this type, so the field should be null.
    Assert.assertNull(organizationId);

    // The field with the expected prefix should match the original data.
    Assert.assertEquals(testPatient.getGeneralPractitionerFirstRep().getReference(),
        "Practitioner/" + practitionerId);

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testNestedReference() {

    Identifier practitionerIdentifier =
        testPatient.getGeneralPractitionerFirstRep().getIdentifier();

    Row practitionerIdentifierRow = testPatientDataset
        .select(functions.explode(functions.col("generalpractitioner")))
        .select("col.organizationId", "col.practitionerId", "col.identifier.id",
            "col.identifier.assigner.reference")
        .head();

    Assert.assertEquals(practitionerIdentifier.getId(), practitionerIdentifierRow.get(2));
    Assert.assertEquals(practitionerIdentifier.getAssigner().getReference(),
        practitionerIdentifierRow.get(3));
  }

  @Test
  public void testSimpleExtension() {

    String testBirthSex = testPatient
        .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    String decodedBirthSex = testPatientDecoded
        .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertEquals(testBirthSex, decodedBirthSex);

    Assert.assertEquals(testBirthSex,
        testPatientDataset.select("birthSex").head().get(0));
  }

  @Test
  public void testNestedExtension() {

    Extension testEthnicity = testPatient
        .getExtensionsByUrl(TestData.US_CORE_ETHNICITY)
        .get(0);

    Coding testOmbCategory = (Coding) testEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    Coding testDetailed1 = (Coding) testEthnicity
        .getExtensionsByUrl("detailed")
        .get(0)
        .getValue();

    Coding testDetailed2 = (Coding) testEthnicity
        .getExtensionsByUrl("detailed")
        .get(1)
        .getValue();

    String testText = testEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Extension decodedEthnicity = testPatientDecoded
        .getExtensionsByUrl(TestData.US_CORE_ETHNICITY)
        .get(0);

    Coding decodedOmbCategory = (Coding) decodedEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    Coding decodedDetailed1 = (Coding) decodedEthnicity
        .getExtensionsByUrl("detailed")
        .get(0)
        .getValue();

    Coding decodedDetailed2 = (Coding) decodedEthnicity
        .getExtensionsByUrl("detailed")
        .get(1)
        .getValue();

    String decodedText = decodedEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertTrue(testOmbCategory.equalsDeep(decodedOmbCategory));
    Assert.assertTrue(testDetailed1.equalsDeep(decodedDetailed1));
    Assert.assertTrue(testDetailed2.equalsDeep(decodedDetailed2));
    Assert.assertEquals(testText, decodedText);

    Row ombCategoryRow = testPatientDataset.select(
        "ethnicity.ombcategory.system",
        "ethnicity.ombcategory.code",
        "ethnicity.ombcategory.display")
        .head();

    Assert.assertEquals(testOmbCategory.getSystem(), ombCategoryRow.get(0));
    Assert.assertEquals(testOmbCategory.getCode(), ombCategoryRow.get(1));
    Assert.assertEquals(testOmbCategory.getDisplay(), ombCategoryRow.get(2));

    // the ethnicity extension has multiple "detailed" sub-extension values
    Row detailedRow = testPatientDataset.select(
        "ethnicity.detailed.system",
        "ethnicity.detailed.code",
        "ethnicity.detailed.display")
        .head();

    Assert.assertEquals(testDetailed1.getSystem(), detailedRow.getList(0).get(0));
    Assert.assertEquals(testDetailed1.getCode(), detailedRow.getList(1).get(0));
    Assert.assertEquals(testDetailed1.getDisplay(), detailedRow.getList(2).get(0));

    Assert.assertEquals(testDetailed2.getSystem(), detailedRow.getList(0).get(1));
    Assert.assertEquals(testDetailed2.getCode(), detailedRow.getList(1).get(1));
    Assert.assertEquals(testDetailed2.getDisplay(), detailedRow.getList(2).get(1));

    Row textRow = testPatientDataset.select("ethnicity.text").head();

    Assert.assertEquals(testText, textRow.get(0));
  }

  @Test
  public void testContainedResources() throws FHIRException {

    Medication testMedicationOne = (Medication) testMedicationRequest.getContained().get(0);
    String testMedicationOneId = testMedicationOne.getId();
    CodeableConcept testMedicationIngredientItem = testMedicationOne.getIngredientFirstRep()
        .getItemCodeableConcept();

    Medication decodedMedicationOne = (Medication) testMedicationRequestDecoded.getContained()
        .get(0);
    String decodedMedicationOneId = decodedMedicationOne.getId();
    CodeableConcept decodedMedicationOneIngredientItem = decodedMedicationOne
        .getIngredientFirstRep()
        .getItemCodeableConcept();

    Assert.assertEquals(testMedicationOneId, decodedMedicationOneId);
    Assert.assertTrue(decodedMedicationOneIngredientItem.equalsDeep(testMedicationIngredientItem));

    Provenance testProvenance = (Provenance) testMedicationRequest.getContained().get(1);
    String testProvenanceId = testProvenance.getId();

    Provenance decodedProvenance = (Provenance) testMedicationRequestDecoded.getContained().get(1);
    String decodedProvenanceId = decodedProvenance.getId();

    Assert.assertEquals(testProvenanceId, decodedProvenanceId);

    Medication testMedicationTwo = (Medication) testMedicationRequest.getContained().get(2);
    String testMedicationTwoId = testMedicationTwo.getId();

    Medication decodedMedicationTwo = (Medication) testMedicationRequestDecoded.getContained()
        .get(2);
    String decodedMedicationTwoId = decodedMedicationTwo.getId();

    Assert.assertEquals(testMedicationTwoId, decodedMedicationTwoId);
  }

  /**
   * Recursively walks the schema to ensure there are no struct fields that are empty.
   */
  private void checkNoEmptyStructs(StructType schema, String fieldName) {

    Assert.assertNotEquals("Struct field " + fieldName + " is empty",
        0,
        schema.fields().length);

    for (StructField field : schema.fields()) {

      if (field.dataType() instanceof StructType) {

        checkNoEmptyStructs((StructType) field.dataType(), field.name());

      } else if (field.dataType() instanceof ArrayType) {

        ArrayType arrayType = (ArrayType) field.dataType();

        if (arrayType.elementType() instanceof StructType) {

          if (!field.name().equals("contained")) {

            checkNoEmptyStructs((StructType) arrayType.elementType(), field.name());
          }
        }
      }
    }
  }

  @Test
  public void testContentReferenceField() {

    // Fields may be a reference to a type defined elsewhere. Make sure they are populated.
    StructType schema = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_OBSERVATION).getSchema();

    checkNoEmptyStructs(schema, null);
  }

  @Test
  public void testRecursiveStructure() {

    // ValueSet has a recursive structure, ensure it terminates without an empty struct.
    StructType schema = SparkRowConverter.forResource(fhirContext, "ValueSet").getSchema();

    checkNoEmptyStructs(schema, null);
  }

  @Test
  public void testDecodeWithDifferentProfile() {

    // Decoding with the base profile should still produce the expected fields.
    SparkRowConverter patientConverter = SparkRowConverter.forResource(fhirContext,
        "Patient");

    Patient basePatientDecoded = (Patient) patientConverter
        .rowToResource(testPatientDataset.head());

    Assert.assertEquals(testPatient.getId(), basePatientDecoded.getId());
    Assert.assertEquals(testPatient.getGender(), basePatientDecoded.getGender());
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(basePatientDecoded.getMultipleBirth()));
  }

  @Test
  public void testSimpleExtensionWithBooleanField() {

    Boolean expected = (Boolean) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Boolean actual = testBunsenTestProfilePatientDataset.select("booleanfield").head()
        .getBoolean(0);
    Assert.assertEquals(expected, actual);

    Boolean decodedBooleanField = (Boolean) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedBooleanField);
  }

  @Test
  public void testSimpleExtensionWithIntegerField() {

    Integer expected = (Integer) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Integer actual = testBunsenTestProfilePatientDataset.select("integerfield").head().getInt(0);
    Assert.assertEquals(expected, actual);

    Integer decodedIntegerField = (Integer) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedIntegerField);
  }

  @Test
  public void testMultiExtensionWithIntegerArrayField() {

    Integer expected1 = (Integer) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    Integer expected2 = (Integer) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(1).getValueAsPrimitive().getValue();

    Integer actual1 = (Integer) testBunsenTestProfilePatientDataset
        .select("integerArrayField").head().getList(0).get(0);

    Integer actual2 = (Integer) testBunsenTestProfilePatientDataset
        .select("integerArrayField").head().getList(0).get(1);

    Assert.assertEquals(expected1, actual1);
    Assert.assertEquals(expected2, actual2);

    Integer decodedIntegerArrayField1 = (Integer) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    Integer decodedIntegerArrayField2 = (Integer) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(1).getValueAsPrimitive().getValue();

    Assert.assertEquals(expected1, decodedIntegerArrayField1);
    Assert.assertEquals(expected2, decodedIntegerArrayField2);
  }

  @Test
  public void testMultiNestedExtension() {

    final Extension nestedExtension1 = testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(0);

    final Extension nestedExtension2 = testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(1);

    String text1 = nestedExtension1.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    String text2 = nestedExtension1.getExtensionsByUrl("text")
        .get(1).getValueAsPrimitive().getValueAsString();

    String text3 = nestedExtension2.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    CodeableConcept codeableConcept1 = (CodeableConcept) nestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    CodeableConcept codeableConcept2 = (CodeableConcept) nestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(1).getValue();

    CodeableConcept codeableConcept3 = (CodeableConcept) nestedExtension2
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    final Extension decodedNestedExtension1 = testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(0);

    final Extension decodedNestedExtension2 = testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(1);

    String decodedText1 = decodedNestedExtension1.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    String decodedText2 = decodedNestedExtension1.getExtensionsByUrl("text")
        .get(1).getValueAsPrimitive().getValueAsString();

    String decodedText3 = decodedNestedExtension2.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    CodeableConcept decodedCodeableConcept1 = (CodeableConcept) decodedNestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    CodeableConcept decodedCodeableConcept2 = (CodeableConcept) decodedNestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(1).getValue();

    CodeableConcept decodedCodeableConcept3 = (CodeableConcept) decodedNestedExtension2
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    Assert.assertEquals(text1, decodedText1);
    Assert.assertEquals(text2, decodedText2);
    Assert.assertEquals(text3, decodedText3);

    Assert.assertTrue(codeableConcept1.equalsDeep(decodedCodeableConcept1));
    Assert.assertTrue(codeableConcept2.equalsDeep(decodedCodeableConcept2));
    Assert.assertTrue(codeableConcept3.equalsDeep(decodedCodeableConcept3));

    final Dataset<Row> textExtDataset =
        testBunsenTestProfilePatientDataset.select("nestedExt.text");

    final Dataset<Row> textRow = textExtDataset
        .select(functions.explode(functions.col("text")).alias("textRow"));

    final Object text_1 = textRow.collectAsList().get(0).getList(0).get(0);
    final Object text_2 = textRow.collectAsList().get(0).getList(0).get(1);
    final Object text_3 = textRow.collectAsList().get(1).getList(0).get(0);

    Assert.assertEquals(decodedText1, text_1);
    Assert.assertEquals(decodedText2, text_2);
    Assert.assertEquals(decodedText3, text_3);

    final Dataset<Row> codeableConceptExtDataset =
        testBunsenTestProfilePatientDataset.select("nestedExt.codeableConceptExt");

    final Dataset<Row> conceptRow = codeableConceptExtDataset
        .select(functions.explode(functions.col("codeableConceptExt")).alias("conceptRow"));

    final int conceptRow1_size = conceptRow.collectAsList().get(0).getList(0).size();
    final int conceptRow2_size = conceptRow.collectAsList().get(1).getList(0).size();

    Assert.assertEquals(2, conceptRow1_size);
    Assert.assertEquals(1, conceptRow2_size);

    final List<Row> conceptRow1 = conceptRow.collectAsList().get(0).getList(0);
    final Object coding_1 = conceptRow1.get(0).getList(1).get(0);
    final Object coding_2 = conceptRow1.get(1).getList(1).get(0);

    Assert.assertEquals(decodedCodeableConcept1.getCoding().get(0).getCode(),
        ((Row)coding_1).getAs("code"));
    Assert.assertEquals(decodedCodeableConcept1.getCoding().get(0).getDisplay(),
        ((Row)coding_1).getAs("display"));
    Assert.assertEquals(decodedCodeableConcept1.getCoding().get(0).getSystem(),
        ((Row)coding_1).getAs("system"));

    Assert.assertEquals(decodedCodeableConcept2.getCoding().get(0).getCode(),
        ((Row)coding_2).getAs("code"));
    Assert.assertEquals(decodedCodeableConcept2.getCoding().get(0).getDisplay(),
        ((Row)coding_2).getAs("display"));
    Assert.assertEquals(decodedCodeableConcept2.getCoding().get(0).getSystem(),
        ((Row)coding_2).getAs("system"));

    final List<Row> conceptRow2 = conceptRow.collectAsList().get(1).getList(0);
    final Object coding_3 = conceptRow2.get(0).getList(1).get(0);

    Assert.assertEquals(decodedCodeableConcept3.getCoding().get(0).getCode(),
        ((Row)coding_3).getAs("code"));
    Assert.assertEquals(decodedCodeableConcept3.getCoding().get(0).getDisplay(),
        ((Row)coding_3).getAs("display"));
    Assert.assertEquals(decodedCodeableConcept3.getCoding().get(0).getSystem(),
        ((Row)coding_3).getAs("system"));
  }

  @Test
  public void testSimpleModifierExtensionWithStringField() {

    String expected = (String) testBunsenTestProfilePatient
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    String actual = testBunsenTestProfilePatientDataset.select("stringModifierExt")
        .head().getString(0);

    Assert.assertEquals(expected, actual);

    String decodedStringField = (String) testBunsenTestProfilePatientDecoded
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedStringField);
  }

  @Test
  public void testMultiModifierExtensionsWithCodeableConceptField() {

    CodeableConcept expected1 = (CodeableConcept) testBunsenTestProfilePatient
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
        .get(0).getValue();

    CodeableConcept expected2 = (CodeableConcept) testBunsenTestProfilePatient
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
        .get(1).getValue();

    CodeableConcept decodedCodeableConceptField1 =
        (CodeableConcept) testBunsenTestProfilePatientDecoded
            .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
            .get(0).getValue();

    CodeableConcept decodedCodeableConceptField2 =
        (CodeableConcept) testBunsenTestProfilePatientDecoded
            .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
            .get(1).getValue();

    Assert.assertTrue(expected1.equalsDeep(decodedCodeableConceptField1));
    Assert.assertTrue(expected2.equalsDeep(decodedCodeableConceptField2));

    final List<Row> codings = testBunsenTestProfilePatientDataset
        .select(functions.explode(functions.col("codeableConceptModifierExt.coding"))
            .alias("coding")).select("coding.code", "coding.display").collectAsList();

    Assert.assertEquals(decodedCodeableConceptField1.getCoding().get(0).getCode(),
        codings.get(0).getList(0).get(0));
    Assert.assertEquals(decodedCodeableConceptField1.getCoding().get(0).getDisplay(),
        codings.get(0).getList(1).get(0));

    Assert.assertEquals(decodedCodeableConceptField2.getCoding().get(0).getCode(),
        codings.get(1).getList(0).get(0));
    Assert.assertEquals(decodedCodeableConceptField2.getCoding().get(0).getDisplay(),
        codings.get(1).getList(1).get(0));
  }

  @Test
  public void testMetaElement() {

    String id =  testPatient.getId();
    Meta meta = testPatient.getMeta();

    Assert.assertEquals(id, testPatientDecoded.getId());
    Assert.assertEquals(meta.getTag().size(), testPatientDecoded.getMeta().getTag().size());
    Assert.assertEquals(meta.getTag().get(0).getCode(),
        testPatientDecoded.getMeta().getTag().get(0).getCode());
    Assert.assertEquals(meta.getTag().get(0).getSystem(),
        testPatientDecoded.getMeta().getTag().get(0).getSystem());
  }
}
