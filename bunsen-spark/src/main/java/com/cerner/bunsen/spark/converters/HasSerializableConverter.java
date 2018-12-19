package com.cerner.bunsen.spark.converters;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.SparkRowConverter;
import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract base class to support functions that need to be serialized
 * with a a row converter. Typically sub-classes would inherit from
 * this and implement the appropriate functional interface for the
 * needed operation.
 */
public abstract class HasSerializableConverter implements Serializable {

  private String resourceTypeUrl;

  private FhirVersionEnum fhirVersion;

  protected transient SparkRowConverter converter;

  protected HasSerializableConverter(String resourceTypeUrl,
      FhirVersionEnum fhirVersion) {

    this.resourceTypeUrl = resourceTypeUrl;
    this.fhirVersion = fhirVersion;

    this.converter = SparkRowConverter.forResource(FhirContexts.contextFor(fhirVersion),
        resourceTypeUrl);
  }

  private void readObject(java.io.ObjectInputStream stream) throws IOException,
      ClassNotFoundException {

    stream.defaultReadObject();

    this.converter = SparkRowConverter.forResource(FhirContexts.contextFor(fhirVersion),
        resourceTypeUrl);
  }

}
