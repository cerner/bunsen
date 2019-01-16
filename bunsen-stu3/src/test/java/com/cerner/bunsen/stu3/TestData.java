package com.cerner.bunsen.stu3;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.hl7.fhir.dstu3.model.Annotation;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.DateTimeType;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.ImagingStudy;
import org.hl7.fhir.dstu3.model.ImagingStudy.ImagingStudySeriesComponent;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Medication;
import org.hl7.fhir.dstu3.model.Medication.MedicationIngredientComponent;
import org.hl7.fhir.dstu3.model.Medication.MedicationStatus;
import org.hl7.fhir.dstu3.model.MedicationRequest;
import org.hl7.fhir.dstu3.model.Narrative;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.OidType;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Procedure;
import org.hl7.fhir.dstu3.model.Provenance;
import org.hl7.fhir.dstu3.model.Provenance.ProvenanceEntityRole;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.utilities.xhtml.NodeType;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;

/**
 * Helper class to create data for testing purposes.
 */
public class TestData {

  /**
   * Returns a FHIR Condition for testing purposes.
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

    condition.setSubject(new Reference("Patient/example").setDisplay("Here is a display for you."));

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
   * Returns a FHIR Observation for testing purposes.
   */
  public static Observation newObservation() {

    // Observation based on https://www.hl7.org/FHIR/observation-example-bloodpressure.json.html
    Observation observation = new Observation();

    observation.setId("blood-pressure");

    Identifier identifier = observation.addIdentifier();
    identifier.setSystem("urn:ietf:rfc:3986");
    identifier.setValue("urn:uuid:187e0c12-8dd2-67e2-99b2-bf273c878281");

    observation.setStatus(Observation.ObservationStatus.FINAL);

    Quantity quantity = new Quantity();
    quantity.setValue(new java.math.BigDecimal("123.45"));
    quantity.setUnit("mm[Hg]");
    observation.setValue(quantity);

    return observation;
  }

  /**
   * Returns a FHIR Patient for testing purposes.
   */
  public static Patient newPatient() {

    Patient patient = new Patient();

    patient.setId("test-patient");
    patient.setMultipleBirth(new IntegerType(1));

    return patient;
  }

  /**
   * Returns a FHIR medication to be contained to a medication request for testing purposes.
   */
  public static Medication newMedication() {

    Medication medication = new Medication();

    medication.setId("test-med");

    MedicationIngredientComponent ingredient = new MedicationIngredientComponent();

    CodeableConcept item = new CodeableConcept();
    item.addCoding()
        .setSystem("test/ingredient/system")
        .setCode("test-code");

    ingredient.setItem(item);

    medication.addIngredient(ingredient);

    return medication;
  }

  /**
   * Returns a FHIR Provenance to be contained to a medication request for testing purposes.
   */
  public static Provenance newProvenance() {

    Provenance provenance = new Provenance();

    provenance.setId("test-provenance");

    provenance.setTarget(ImmutableList.of(new Reference("test-target")));

    provenance.getEntityFirstRep()
        .setRole(ProvenanceEntityRole.SOURCE)
        .setWhat(new Reference("test-entity"));

    return provenance;
  }

  /**
   * Returns a FHIR medication request for testing purposes.
   */
  public static MedicationRequest newMedRequest() {

    MedicationRequest medReq = new MedicationRequest();

    medReq.setId("test-medreq");

    // Medication code
    CodeableConcept med = new CodeableConcept();
    med.addCoding()
        .setSystem("http://www.nlm.nih.gov/research/umls/rxnorm")
        .setCode("582620")
        .setDisplay("Nizatidine 15 MG/ML Oral Solution [Axid]");

    med.setText("Nizatidine 15 MG/ML Oral Solution [Axid]");

    medReq.setMedication(med);

    Annotation annotation = new Annotation();

    annotation.setText("Test medication note.");

    annotation.setAuthor(
        new Reference("Provider/example")
            .setDisplay("Example provider."));

    medReq.addNote(annotation);

    // Add contained resources
    medReq.addContained(newMedication());
    medReq.addContained(newProvenance());

    return medReq;
  }

  /**
   * Returns a FHIR ImagingStudy resource for testing purposes.
   */
  public static ImagingStudy newImagingStudy() {
    ImagingStudy imagingStudy = new ImagingStudy();

    imagingStudy.setId("test-imaging-study");
    imagingStudy.setUid("1.3.12.2.1107.5.1.4.45533.30000008102706401187500000052");
    imagingStudy.setPatient(new Reference("Patient/test-patient"));
    List<ImagingStudySeriesComponent> series = new ArrayList<>();
    ImagingStudySeriesComponent seriesComponent = new ImagingStudySeriesComponent(
        new OidType("1.3.12.2.1107.5.1.4.45533.30000008102706401187500000053"),
        new Coding("http://dicom.nema.org/resources/ontology/DCM", "HD", "Hemodynamic Waveform"));
    series.add(seriesComponent);
    imagingStudy.setSeries(series);

    return imagingStudy;
  }
}
