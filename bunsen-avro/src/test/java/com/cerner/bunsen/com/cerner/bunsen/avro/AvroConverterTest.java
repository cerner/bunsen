package com.cerner.bunsen.com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.cerner.bunsen.stu3.TestData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvroConverterTest {

  private static final Observation testObservation = TestData.newObservation();

  public static IndexedRecord avroObservation;

  private static Observation testObservationDecoded;

  private static final Patient testPatient = TestData.newPatient();

  public static IndexedRecord avroPatient;

  private static Patient testPatientDecoded;

  @BeforeClass
  public static void convertTestData() {

    AvroConverter observationConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        "Observation");

    avroObservation = observationConverter.resourceToAvro(testObservation);

    testObservationDecoded = (Observation) observationConverter.avroToResource(avroObservation);

    AvroConverter patientConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        TestData.US_CORE_PATIENT);

    avroPatient = patientConverter.resourceToAvro(testPatient);

    testPatientDecoded = (Patient) patientConverter.avroToResource(avroPatient);
  }

  @Test
  public void testHack() {

    System.out.println(avroObservation.toString());
    System.out.println(avroPatient.toString());

    System.out.println(avroObservation.getSchema().toString(true));

  }

  @Test
  public void testInteger() {

    Integer expectedMultipleBirth = ((IntegerType) testPatient.getMultipleBirth()).getValue();

    Assert.assertEquals(expectedMultipleBirth,
        ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());

    Assert.assertEquals(expectedMultipleBirth,
        ((Record) ((Record) avroPatient).get("multipleBirth")).get("integer"));

  }
}
