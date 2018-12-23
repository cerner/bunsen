package com.cerner.bunsen.com.cerner.bunsen.avro;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.cerner.bunsen.stu3.TestData;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.exceptions.FHIRException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvroConverterTest {

  private static final Observation testObservation = TestData.newObservation();

  public static Record avroObservation;

  private static Observation testObservationDecoded;

  private static final Patient testPatient = TestData.newPatient();

  public static Record avroPatient;

  private static Patient testPatientDecoded;

  private static final Condition testCondition = TestData.newCondition();

  public static Record avroCondition;

  private static Condition testConditionDecoded;

  @BeforeClass
  public static void convertTestData() {

    AvroConverter observationConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        "Observation");

    avroObservation = (Record) observationConverter.resourceToAvro(testObservation);

    testObservationDecoded = (Observation) observationConverter.avroToResource(avroObservation);

    AvroConverter patientConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        TestData.US_CORE_PATIENT);

    avroPatient = (Record) patientConverter.resourceToAvro(testPatient);

    testPatientDecoded = (Patient) patientConverter.avroToResource(avroPatient);

    AvroConverter conditionConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        TestData.US_CORE_CONDITION);

    avroCondition = (Record) conditionConverter.resourceToAvro(testCondition);

    testConditionDecoded =  (Condition) conditionConverter.avroToResource(avroCondition);
  }

  @Test
  public void testHack() {

    System.out.println(avroObservation.toString());
    System.out.println(avroPatient.toString());

//    System.out.println(avroObservation.getSchema().toString(true));

  }

  @Test
  public void testDecimal() {

    BigDecimal originalDecimal = ((Quantity) testObservation.getValue()).getValue();

    // Decode the Avro decimal to ensure the expected value is there.
    ByteBuffer decimalBytes = (ByteBuffer) ((Record)
        ((Record) avroObservation.get("value"))
        .get("quantity"))
        .get("value");

    // rewind the buffer to read the full number.
    decimalBytes.rewind();

    LogicalTypes.Decimal decimalPrecision
        = LogicalTypes.decimal(12, 4);

    Schema decimalSchema =
        decimalPrecision.addToSchema(Schema.create(Type.BYTES));

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    BigDecimal avroDecimal = conversion.fromBytes(decimalBytes, decimalSchema, decimalPrecision);

    Assert.assertEquals(originalDecimal.compareTo(avroDecimal), 0);

    Assert.assertEquals(originalDecimal.compareTo(
        ((Quantity) testObservationDecoded
            .getValue())
            .getValue()), 0);

  }

  @Test
  public void testChoice() throws FHIRException {

    // Ensure that a decoded choice type matches the original
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(testPatientDecoded.getMultipleBirth()));

  }

  @Test
  public void testInteger() {

    Integer expectedMultipleBirth = ((IntegerType) testPatient.getMultipleBirth()).getValue();

    Assert.assertEquals(expectedMultipleBirth,
        ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());

    Assert.assertEquals(expectedMultipleBirth,
        ((Record) avroPatient.get("multipleBirth")).get("integer"));

  }

  @Test
  public void testBoundCode() {

    Assert.assertEquals(testObservation.getStatus().toCode(),
        avroObservation.get("status"));

    Assert.assertEquals(testObservation.getStatus(),
        testObservationDecoded.getStatus());
  }

  @Test
  public void testCoding() {

    Coding testCoding = testCondition.getSeverity().getCodingFirstRep();
    Coding decodedCoding = testConditionDecoded.getSeverity().getCodingFirstRep();

    List<Record> severityCodings = (List) ((Record)  avroCondition.get("severity")).get("coding");

    Record severityCoding = severityCodings.get(0);

    Assert.assertEquals(testCoding.getCode(),
        severityCoding.get("code"));
    Assert.assertEquals(testCoding.getCode(),
        decodedCoding.getCode());

    Assert.assertEquals(testCoding.getSystem(),
        severityCoding.get("system"));
    Assert.assertEquals(testCoding.getSystem(),
        decodedCoding.getSystem());

    Assert.assertEquals(testCoding.getUserSelected(),
        severityCoding.get("userSelected"));
    Assert.assertEquals(testCoding.getUserSelected(),
        decodedCoding.getUserSelected());

    Assert.assertEquals(testCoding.getDisplay(),
        severityCoding.get("display"));
    Assert.assertEquals(testCoding.getDisplay(),
        decodedCoding.getDisplay());
  }

  @Test
  public void testSingleReference() {

    Record subject = (Record) avroCondition.get("subject");

    Assert.assertEquals(testCondition.getSubject().getReference(),
        subject.get("reference"));

    Assert.assertEquals("12345",  subject.get("PatientId"));

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testMultiReferenceTypes() {

    // Row containing the general practitioner from our dataset.
    /*
    Row practitioner = testPatientDataset
        .select(functions.explode(functions.col("generalpractitioner")))
        .select("col.organizationId", "col.practitionerId")
        .head();
        */

    // avroPatient.

    Record practitioner = (Record) ((List) avroPatient.get("generalPractitioner")).get(0);

    String organizationId = (String) practitioner.get("OrganizationId");
    String practitionerId = (String) practitioner.get("PractitionerId");

    // The reference is not of this type, so the field should be null.
    Assert.assertNull(organizationId);

    // The field with the expected prefix should match the original data.
    Assert.assertEquals(testPatient.getGeneralPractitionerFirstRep().getReference(),
        "Practitioner/" + practitionerId);

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
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
        ((Record) avroPatient).get("birthsex"));
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

    String decodedText = decodedEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertTrue(testOmbCategory.equalsDeep(decodedOmbCategory));
    Assert.assertEquals(testText, decodedText);

    Record ethnicityRecord = (Record) avroPatient.get("ethnicity");

    Record ombCategoryRecord =  (Record) ethnicityRecord.get("ombCategory");

    Assert.assertEquals(testOmbCategory.getSystem(), ombCategoryRecord.get("system"));
    Assert.assertEquals(testOmbCategory.getCode(), ombCategoryRecord.get("code"));
    Assert.assertEquals(testOmbCategory.getDisplay(), ombCategoryRecord.get("display"));


    Assert.assertEquals(testText, ethnicityRecord.get("text"));
  }

}
