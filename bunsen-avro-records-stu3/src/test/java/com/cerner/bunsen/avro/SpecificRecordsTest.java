package com.cerner.bunsen.avro;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.stu3.avro.Observation;
import com.cerner.bunsen.stu3.avro.OrganizationPractitionerReference;
import com.cerner.bunsen.stu3.avro.PatientReference;
import com.cerner.bunsen.stu3.avro.Provenance;
import com.cerner.bunsen.stu3.avro.us.core.UsCoreEthnicity;
import com.cerner.bunsen.stu3.TestData;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.MedicationRequest;
import org.hl7.fhir.dstu3.model.Quantity;
import org.junit.Assert;
import org.junit.Test;

import com.cerner.bunsen.stu3.avro.us.core.Patient;

/**
 * Unit test for conversion of specific records to and from HAPI resources.
 */
public class SpecificRecordsTest {

  // Patient
  private static final AvroConverter PATIENT_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), TestData.US_CORE_PATIENT);

  private static final org.hl7.fhir.dstu3.model.Patient testPatient = TestData.newPatient();

  private static final Patient avroPatient =
      (Patient) PATIENT_CONVERTER.resourceToAvro(testPatient);

  private static final org.hl7.fhir.dstu3.model.Patient  testPatientDecoded =
      (org.hl7.fhir.dstu3.model.Patient ) PATIENT_CONVERTER.avroToResource(avroPatient);

  // Observation
  private static final AvroConverter OBSERVATION_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), "Observation");

  private static final org.hl7.fhir.dstu3.model.Observation testObservation =
      TestData.newObservation();

  private static final Observation avroObservation =
      (Observation) OBSERVATION_CONVERTER.resourceToAvro(testObservation);

  private static final org.hl7.fhir.dstu3.model.Observation testObservationDecoded =
      (org.hl7.fhir.dstu3.model.Observation) OBSERVATION_CONVERTER.avroToResource(avroObservation);

  // Condition
  private static final AvroConverter CONDITION_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), TestData.US_CORE_CONDITION);

  private static final org.hl7.fhir.dstu3.model.Condition testCondition = TestData.newCondition();

  private static final com.cerner.bunsen.stu3.avro.us.core.Condition avroCondition =
      (com.cerner.bunsen.stu3.avro.us.core.Condition) CONDITION_CONVERTER
          .resourceToAvro(testCondition);

  private static final org.hl7.fhir.dstu3.model.Condition testConditionDecoded =
      (org.hl7.fhir.dstu3.model.Condition) CONDITION_CONVERTER.avroToResource(avroCondition);

  // Medication
  private static final AvroConverter MEDICATION_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), TestData.US_CORE_MEDICATION);

  private static final org.hl7.fhir.dstu3.model.Medication testMedication =
      TestData.newMedication("test-medication-1");

  private static final com.cerner.bunsen.stu3.avro.us.core.Medication avroMedication =
      (com.cerner.bunsen.stu3.avro.us.core.Medication) MEDICATION_CONVERTER
          .resourceToAvro(testMedication);

  private static final org.hl7.fhir.dstu3.model.Medication testMedicationDecoded =
      (org.hl7.fhir.dstu3.model.Medication) MEDICATION_CONVERTER.avroToResource(avroMedication);

  // MedicationRequest
  private static final AvroConverter MEDICATION_REQUEST_CONVERTER =
      AvroConverter.forResource(FhirContexts.forStu3(), TestData.US_CORE_MEDICATION_REQUEST,
          Arrays.asList(TestData.PROVENANCE));

  private static final org.hl7.fhir.dstu3.model.Provenance testProvenance =
      TestData.newProvenance();

  private static final org.hl7.fhir.dstu3.model.MedicationRequest testMedicationRequest =
      (MedicationRequest) TestData.newMedicationRequest().addContained(testProvenance);

  private static final com.cerner.bunsen.stu3.avro.us.core.MedicationRequest avroMedicationRequest =
      (com.cerner.bunsen.stu3.avro.us.core.MedicationRequest) MEDICATION_REQUEST_CONVERTER
          .resourceToAvro(testMedicationRequest);

  private static final org.hl7.fhir.dstu3.model.MedicationRequest testMedicationRequestDecoded =
      (org.hl7.fhir.dstu3.model.MedicationRequest) MEDICATION_REQUEST_CONVERTER
          .avroToResource(avroMedicationRequest);


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
  public void testChoice() {

    // Ensure that a decoded choice type matches the original
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(testPatientDecoded.getMultipleBirth()));

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
            avroPatient.getMultipleBirth().getInteger());

    // Choice types not populated should be null.
    Assert.assertNull(avroPatient.getMultipleBirth().getBoolean$());
  }

  /**
   * Tests that FHIR StructureDefinitions that contain fields having identical ChoiceTypes generate
   * an Avro definition that does not trigger an erroneous re-definition of the Avro, and that the
   * converter functions can populate the separate fields even when they share an underlying Avro
   * class for the ChoiceType.
   */
  @Test
  public void testIdenticalChoicesTypes() {

    Assert.assertTrue(testMedication.getIngredientFirstRep()
        .equalsDeep(testMedicationDecoded.getIngredientFirstRep()));

    Assert.assertTrue(testMedication.getPackage().getContentFirstRep()
        .equalsDeep(testMedicationDecoded.getPackage().getContentFirstRep()));
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

  @Test
  public void testInstantiationUsingBuilder() {

    Patient obj = Patient.newBuilder()
        .setId("123").build();
    Assert.assertEquals("123", obj.getId());
    Assert.assertEquals(null, obj.getGender());
  }

  @Test
  public void testSimpleBackboneElement() {

    Assert.assertTrue(avroMedicationRequest.getSubstitution().getAllowed());

    Assert.assertTrue(testMedicationRequestDecoded.getSubstitution().getAllowed());

    Assert.assertEquals(
        testMedicationRequest.getSubstitution().getAllowed(),
        testMedicationRequestDecoded.getSubstitution().getAllowed());
  }

  @Test
  public void testSimpleElement() {

    Assert.assertEquals(10,
        avroMedicationRequest.getDosageInstruction().get(0).getTiming().getRepeat().getCount()
            .intValue());

    Assert.assertEquals(10,
        testMedicationRequestDecoded.getDosageInstructionFirstRep().getTiming().getRepeat()
            .getCount());

    Assert.assertEquals(
        testMedicationRequest.getDosageInstructionFirstRep().getTiming().getRepeat().getCount(),
        testMedicationRequestDecoded.getDosageInstructionFirstRep().getTiming().getRepeat()
            .getCount());
  }

  @Test
  public void testContainedResource() {

    org.hl7.fhir.dstu3.model.Provenance testProvenance =
        (org.hl7.fhir.dstu3.model.Provenance) testMedicationRequest.getContained().get(0);
    String testProvenanceId = testProvenance.getId();

    org.hl7.fhir.dstu3.model.Provenance decodedProvenance =
        (org.hl7.fhir.dstu3.model.Provenance) testMedicationRequestDecoded.getContained().get(0);
    String decodedProvenanceId = decodedProvenance.getId();

    Assert.assertEquals(testProvenanceId, decodedProvenanceId);
  }

}
