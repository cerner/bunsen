package com.cerner.bunsen.stu3;

import java.util.Arrays;
import org.hl7.fhir.dstu3.model.Address;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.DateTimeType;
import org.hl7.fhir.dstu3.model.DateType;
import org.hl7.fhir.dstu3.model.Dosage;
import org.hl7.fhir.dstu3.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Medication;
import org.hl7.fhir.dstu3.model.Medication.MedicationIngredientComponent;
import org.hl7.fhir.dstu3.model.Medication.MedicationPackageComponent;
import org.hl7.fhir.dstu3.model.Medication.MedicationPackageContentComponent;
import org.hl7.fhir.dstu3.model.MedicationRequest;
import org.hl7.fhir.dstu3.model.MedicationRequest.MedicationRequestSubstitutionComponent;
import org.hl7.fhir.dstu3.model.Narrative;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.Timing;
import org.hl7.fhir.dstu3.model.Timing.TimingRepeatComponent;
import org.hl7.fhir.utilities.xhtml.NodeType;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;

/**
 * Common test resources for Bunsen STU3 usage.
 */
public class TestData {

  public static final String US_CORE_BIRTHSEX
      = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex";

  public static final String US_CORE_ETHNICITY
      = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity";

  public static final String US_CORE_PATIENT =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  public static final String US_CORE_OBSERVATION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observationresults";

  public static final String US_CORE_CONDITION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition";

  public static final String US_CORE_MEDICATION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medication";

  public static final String US_CORE_MEDICATION_REQUEST =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medicationrequest";

  public static final String VALUE_SET =
      "http://hl7.org/fhir/StructureDefinition/ValueSet";

  /**
   * Returns a FHIR Condition for testing purposes.
   *
   * @return a FHIR Condition for testing.
   */
  public static Condition newCondition() {

    Condition condition = new Condition();

    // Condition based on example from FHIR:
    // https://www.hl7.org/fhir/condition-example.json.html
    condition.setId("Condition/example");

    condition.setLanguage("en_US");

    // Narrative text
    Narrative narrative = new Narrative();
    narrative.setStatusAsString("generated");
    narrative.setDivAsString("This data was generated for test purposes.");
    XhtmlNode node = new XhtmlNode();
    node.setNodeType(NodeType.Text);
    node.setValue("Severe burn of left ear (Date: 24-May 2012)");
    condition.setText(narrative);

    condition.setSubject(new Reference("Patient/12345").setDisplay("Here is a display for you."));

    condition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

    // Condition code
    CodeableConcept code = new CodeableConcept();
    code.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("39065001")
        .setDisplay("Severe");
    condition.setSeverity(code);

    // Severity code
    CodeableConcept severity = new CodeableConcept();
    severity.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("24484000")
        .setDisplay("Burn of ear")
        .setUserSelected(true);
    condition.setSeverity(severity);

    // Onset date time
    DateTimeType onset = new DateTimeType();
    onset.setValueAsString("2012-05-24");
    condition.setOnset(onset);

    return condition;
  }

  /**
   * Returns a new Observation for testing.
   *
   * @return a FHIR Observation for testing.
   */
  public static Observation newObservation() {
    Observation observation = new Observation();

    observation.setId("blood-pressure");

    Identifier identifier = observation.addIdentifier();
    identifier.setSystem("urn:ietf:rfc:3986");
    identifier.setValue("urn:uuid:187e0c12-8dd2-67e2-99b2-bf273c878281");

    observation.setStatus(Observation.ObservationStatus.FINAL);

    CodeableConcept obsCode = new CodeableConcept();

    observation.setCode(obsCode);

    Quantity quantity = new Quantity();
    quantity.setValue(new java.math.BigDecimal("123.45"));
    quantity.setUnit("mm[Hg]");
    quantity.setSystem("http://unitsofmeasure.org");
    observation.setValue(quantity);

    ObservationComponentComponent component = observation.addComponent();

    CodeableConcept code = new CodeableConcept()
        .addCoding(new Coding()
            .setCode("abc")
            .setSystem("PLACEHOLDER"));

    component.setCode(code);

    return observation;
  }

  /**
   * Returns a new Patient for testing.
   *
   * @return a FHIR Patient for testing.
   */
  public static Patient newPatient() {

    Patient patient = new Patient();

    patient.setId("test-patient");
    patient.setGender(AdministrativeGender.MALE);
    patient.setActive(true);
    patient.setMultipleBirth(new IntegerType(1));

    patient.setBirthDateElement(new DateType("1945-01-02"));

    Address address = patient.addAddress();

    patient.addGeneralPractitioner().setReference("Practitioner/12345");

    address.addLine("123 Fake Street");
    address.setCity("Chicago");
    address.setState("IL");
    address.setDistrict("12345");

    Extension birthSex = patient.addExtension();

    birthSex.setUrl(US_CORE_BIRTHSEX);
    birthSex.setValue(new CodeType("M"));

    Extension ethnicity = patient.addExtension();
    ethnicity.setUrl(US_CORE_ETHNICITY);
    ethnicity.setValue(null);

    Coding ombCoding = new Coding();

    ombCoding.setSystem("urn:oid:2.16.840.1.113883.6.238");
    ombCoding.setCode("2186-5");
    ombCoding.setDisplay("Not Hispanic or Latino");

    // Add category to ethnicity extension
    Extension ombCategory = ethnicity.addExtension();

    ombCategory.setUrl("ombCategory");
    ombCategory.setValue(ombCoding);

    // Add text display to ethnicity extension
    Extension ethnicityText = ethnicity.addExtension();
    ethnicityText.setUrl("text");
    ethnicityText.setValue(new StringType("Not Hispanic or Latino"));

    return patient;
  }

  /**
   * Returns a new Medication for testing.
   *
   * @return a FHIR Medication for testing.
   */
  public static Medication newMedication() {

    Medication medication = new Medication();

    medication.setId("test-medication");

    CodeableConcept itemCodeableConcept = new CodeableConcept();
    itemCodeableConcept.addCoding()
        .setSystem("http://www.nlm.nih.gov/research/umls/rxnorm")
        .setCode("103109")
        .setDisplay("Vitamin E 3 MG Oral Tablet [Ephynal]")
        .setUserSelected(true);

    MedicationIngredientComponent ingredientComponent = new MedicationIngredientComponent();
    ingredientComponent.setItem(itemCodeableConcept);

    medication.addIngredient(ingredientComponent);

    Reference itemReference = new Reference("test-item-reference");

    MedicationPackageContentComponent medicationPackageContentComponent =
        new MedicationPackageContentComponent();
    medicationPackageContentComponent.setItem(itemReference);

    MedicationPackageComponent medicationPackageComponent = new MedicationPackageComponent();
    medicationPackageComponent.addContent(medicationPackageContentComponent);

    medication.setPackage(medicationPackageComponent);

    return medication;
  }

  /**
   * Returns a new MedicationRequest for testing.
   *
   * @return a FHIR MedicationRequest for testing.
   */
  public static MedicationRequest newMedicationRequest() {

    MedicationRequest medicationRequest = new MedicationRequest();

    medicationRequest.setId("test-medication-request");

    CodeableConcept itemCodeableConcept = new CodeableConcept();
    itemCodeableConcept.addCoding()
        .setSystem("http://www.nlm.nih.gov/research/umls/rxnorm")
        .setCode("103109")
        .setDisplay("Vitamin E 3 MG Oral Tablet [Ephynal]")
        .setUserSelected(true);

    medicationRequest.setMedication(itemCodeableConcept);

    medicationRequest
        .setSubject(new Reference("Patient/12345").setDisplay("Here is a display for you."));

    medicationRequest.setDosageInstruction(Arrays.asList(
        new Dosage().setTiming(new Timing().setRepeat(new TimingRepeatComponent().setCount(10)))));

    medicationRequest
        .setSubstitution(new MedicationRequestSubstitutionComponent().setAllowed(true));

    return medicationRequest;
  }

}
