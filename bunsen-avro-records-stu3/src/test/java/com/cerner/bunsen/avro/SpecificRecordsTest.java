package com.cerner.bunsen.avro;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.stu3.avro.Observation;
import com.cerner.bunsen.stu3.avro.OrganizationPractitionerReference;
import com.cerner.bunsen.stu3.avro.PatientReference;
import com.cerner.bunsen.stu3.avro.us.core.UsCoreEthnicity;
import com.cerner.bunsen.stu3.TestData;
import java.math.BigDecimal;
import java.util.List;

import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.exceptions.FHIRException;
import org.junit.Assert;
import org.junit.Test;

import com.cerner.bunsen.stu3.avro.us.core.Patient;

/**
 * Unit test for conversion of specific records to and from HAPI resources.
 */
public class SpecificRecordsTest {

  private static final AvroConverter PATIENT_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), TestData.US_CORE_PATIENT);

  private static final org.hl7.fhir.dstu3.model.Patient testPatient = TestData.newPatient();

  private static final Patient avroPatient =
      (Patient) PATIENT_CONVERTER.resourceToAvro(testPatient);

  private static final org.hl7.fhir.dstu3.model.Patient  testPatientDecoded =
      (org.hl7.fhir.dstu3.model.Patient ) PATIENT_CONVERTER.avroToResource(avroPatient);

  private static final AvroConverter OBSERVATION_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), "Observation");

  private static final org.hl7.fhir.dstu3.model.Observation testObservation
      = TestData.newObservation();

  public static Observation avroObservation =
      (Observation) OBSERVATION_CONVERTER.resourceToAvro(testObservation);

  private static org.hl7.fhir.dstu3.model.Observation testObservationDecoded =
      (org.hl7.fhir.dstu3.model.Observation) OBSERVATION_CONVERTER.avroToResource(avroObservation);

  private static final AvroConverter CONDITION_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), TestData.US_CORE_CONDITION);

  private static final org.hl7.fhir.dstu3.model.Condition testCondition = TestData.newCondition();

  public static com.cerner.bunsen.stu3.avro.us.core.Condition avroCondition =
      (com.cerner.bunsen.stu3.avro.us.core.Condition) CONDITION_CONVERTER.resourceToAvro(testCondition);

  private static org.hl7.fhir.dstu3.model.Condition  testConditionDecoded =
      (org.hl7.fhir.dstu3.model.Condition ) CONDITION_CONVERTER.avroToResource(avroCondition);


  @Test
  public void testInteger() {

    Integer expectedMultipleBirth = ((IntegerType) testPatient.getMultipleBirth()).getValue();

    Assert.assertEquals(expectedMultipleBirth,
        ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());

    Assert.assertEquals(expectedMultipleBirth,
        avroPatient.getMultipleBirth().getInteger());
  }

  @Test
  public void testBoundCode() {

    Assert.assertEquals(testObservation.getStatus().toCode(),
        avroObservation.getStatus());

    Assert.assertEquals(testObservation.getStatus(),
        testObservationDecoded.getStatus());
  }

  @Test
  public void testDecimal() {

    BigDecimal originalDecimal = ((org.hl7.fhir.dstu3.model.Quantity)
        testObservation.getValue()).getValue();

    // Decode the Avro decimal to ensure the expected value is there.
    BigDecimal avroDecimal  = avroObservation.getValue().getQuantity().getValue();

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

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
            avroPatient.getMultipleBirth().getInteger());

    // Choice types not populated should be null.
    Assert.assertNull(avroPatient.getMultipleBirth().getBoolean$());
  }

  @Test
  public void testCoding() {

    org.hl7.fhir.dstu3.model.Coding testCoding = testCondition.getSeverity().getCodingFirstRep();
    Coding decodedCoding = testConditionDecoded.getSeverity().getCodingFirstRep();

    List<com.cerner.bunsen.stu3.avro.Coding> severityCodings = avroCondition.getSeverity().getCoding();

    com.cerner.bunsen.stu3.avro.Coding severityCoding = severityCodings.get(0);

    Assert.assertEquals(testCoding.getCode(),
        severityCoding.getCode());
    Assert.assertEquals(testCoding.getCode(),
        decodedCoding.getCode());

    Assert.assertEquals(testCoding.getSystem(),
        severityCoding.getSystem());
    Assert.assertEquals(testCoding.getSystem(),
        decodedCoding.getSystem());

    Assert.assertEquals(testCoding.getUserSelected(),
        severityCoding.getUserSelected());
    Assert.assertEquals(testCoding.getUserSelected(),
        decodedCoding.getUserSelected());

    Assert.assertEquals(testCoding.getDisplay(),
        severityCoding.getDisplay());
    Assert.assertEquals(testCoding.getDisplay(),
        decodedCoding.getDisplay());
  }

  @Test
  public void testSingleReference() {

    PatientReference subject = avroCondition.getSubject();

    Assert.assertEquals(testCondition.getSubject().getReference(),
        subject.getReference());

    Assert.assertEquals("12345",  subject.getPatientId());

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testMultiReferenceTypes() {

    OrganizationPractitionerReference practitioner = avroPatient.getGeneralPractitioner().get(0);

    String organizationId = practitioner.getOrganizationId();
    String practitionerId = practitioner.getPractitionerId();

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

    Assert.assertEquals(testBirthSex, avroPatient.getBirthsex());
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

    UsCoreEthnicity ethnicityRecord = avroPatient.getEthnicity();

    com.cerner.bunsen.stu3.avro.Coding ombCategoryRecord =  ethnicityRecord.getOmbCategory();

    Assert.assertEquals(testOmbCategory.getSystem(), ombCategoryRecord.getSystem());
    Assert.assertEquals(testOmbCategory.getCode(), ombCategoryRecord.getCode());
    Assert.assertEquals(testOmbCategory.getDisplay(), ombCategoryRecord.getDisplay());

    Assert.assertEquals(testText, ethnicityRecord.getText());
  }

}
