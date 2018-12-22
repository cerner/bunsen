package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.avro.converters.DefinitionToAvroVisitor;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.StructureDefinitions;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Converter to change HAPI objects into Avro structures and vice versa.
 */
public class AvroConverter {

  private final HapiConverter<Schema> hapiToAvroConverter;

  private final HapiObjectConverter avroToHapiConverter;

  private final DefinitionToAvroVisitor visitor;

  AvroConverter(FhirContext context,
      StructureDefinitions structureDefinitions,
      String resourceTypeUrl) {

    this.visitor = new DefinitionToAvroVisitor(structureDefinitions.conversionSupport());

    this.hapiToAvroConverter = structureDefinitions.transform(visitor, resourceTypeUrl);

    RuntimeResourceDefinition resourceDefinition =
        context.getResourceDefinition(hapiToAvroConverter.getElementType());

    this.avroToHapiConverter  =
        (HapiObjectConverter) hapiToAvroConverter.toHapiConverter(resourceDefinition);
  }

  /**
   * Returns a row converter for the given resource type. The resource type can
   * either be a relative URL for a base resource (e.g., "Condition" or "Observation"),
   * or a URL identifying the structure definition for a given profile, such as
   * "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient".
   *
   * @param context the FHIR context
   * @param resourceTypeUrl the URL of the resource type
   * @return an avro converter instance.
   */
  public static AvroConverter forResource(FhirContext context,
      String resourceTypeUrl) {

    StructureDefinitions structureDefinitions = StructureDefinitions.create(context);

    return new AvroConverter(context, structureDefinitions, resourceTypeUrl);
  }

  /**
   * Converts a given FHIR resource to a Spark row.
   *
   * @param resource the FHIR resource
   * @return the row
   */
  public IndexedRecord resourceToAvro(IBaseResource resource) {

    return (IndexedRecord) hapiToAvroConverter.fromHapi(resource);
  }

  public IBaseResource avroToResource(IndexedRecord record) {

    return (IBaseResource) avroToHapiConverter.toHapi(record);
  }

  /**
   * Returns the Avro schema equivalent for the FHIR resource.
   *
   * @return the Spark schema
   */
  public Schema getSchema() {

    return hapiToAvroConverter.getDataType();
  }

  /**
   * Returns the FHIR type of the resource being converted.
   *
   * @return the FHIR type of the resource being converted.
   */
  public String getResourceType() {
    return hapiToAvroConverter.getElementType();
  }

}
