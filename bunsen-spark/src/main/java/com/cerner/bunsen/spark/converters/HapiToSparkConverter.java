package com.cerner.bunsen.spark.converters;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;

import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Base class for converting HAPI resources to Spark rows.
 */
public abstract class HapiToSparkConverter {

  /**
   * Supporting interface to set a field on a HAPI object.
   */
  public interface HapiFieldSetter {

    /**
     * Sets the value to the corresponding field on a FHIR object.
     *
     * @param parentObject the composite object getting its field set
     * @param fieldToSet the runtime definition of the field to set.
     * @param sparkObject the Spark value to be converted and set on the FHIR object.
     */
    public void setField(IBase parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject);
  }

  /**
   * Converts the given HAPI structure to its spark equivalent.
   *
   * @param input a HAPI object.
   * @return the spark equivalent.
   */
  public abstract Object toSpark(Object input);

  /**
   * Returns the Spark datatype of the converted item.
   *
   * @return The Spark data type of the converted item.
   */
  public abstract DataType getDataType();

  /**
   * The extension URL if the field is an extension, null otherwise.
   *
   * @return extension URL if the field is an extension, null otherwise.
   */
  public String extensionUrl() {
    return null;
  }

  /**
   * The FHIR type of the elemen to be converted.
   *
   * @return FHIR type of the elemen to be converted.
   */
  public String getElementType() {
    return null;
  }

  /**
   * Returns a field setter to be used when converting a Spark object
   * to HAPI.
   *
   * @param elementDefinitions the set of element definitions that the element can be.
   * @return the field setter.
   */
  public abstract HapiFieldSetter toHapiConverter(
      BaseRuntimeElementDefinition... elementDefinitions);
}
