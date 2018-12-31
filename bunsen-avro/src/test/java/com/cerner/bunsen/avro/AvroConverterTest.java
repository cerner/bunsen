package com.cerner.bunsen.avro;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.cerner.bunsen.stu3.TestData;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData.Record;
import org.hl7.fhir.dstu3.hapi.ctx.IValidationSupport;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.dstu3.model.StructureDefinition.StructureDefinitionKind;
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


  /**
   * Initialize test data.
   */
  @BeforeClass
  public static void convertTestData() throws IOException {

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
  public void testDecimal() {

    BigDecimal originalDecimal = ((Quantity) testObservation.getValue()).getValue();

    // Decode the Avro decimal to ensure the expected value is there.
    BigDecimal avroDecimal  = (BigDecimal) ((Record)
        ((Record) avroObservation.get("value"))
        .get("quantity"))
        .get("value");

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

  @Test
  public void testCompile() throws IOException {

    List<Schema> schemas = AvroConverter.generateSchemas(FhirContexts.forStu3(),
        ImmutableList.of(TestData.US_CORE_PATIENT,
            TestData.VALUE_SET));

    // Wrap the schemas in a protocol to simplify the invocation of the compiler.
    Protocol protocol = new Protocol("fhir-test",
        "FHIR Resources for Testing",
        null);

    protocol.setTypes(schemas);

    SpecificCompiler compiler = new SpecificCompiler(protocol);

    Path generatedCodePath = Files.createTempDirectory("generated_code");

    generatedCodePath.toFile().deleteOnExit();

    compiler.compileToDestination(null, generatedCodePath.toFile());

    // Check that java files were created as expected.
    Set<String> javaFiles = Files.find(generatedCodePath,
        10,
        (path, basicFileAttributes) -> true)
        .map(path -> generatedCodePath.relativize(path))
        .map(Object::toString)
        .collect(Collectors.toSet());

    // Ensure common types were generated
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/Period.java"));
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/Coding.java"));
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/ValueSet.java"));

    // The specific profile should be created in the expected sub-package.
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/us/core/Patient.java"));

    // Check extension types.
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/us/core/UsCoreRace.java"));

    // Choice types include each choice that could be used.
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/ChoiceBooleanInteger.java"));
  }
}
