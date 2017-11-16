package com.cerner.bunsen;

import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Spark Encoders for FHIR Resources. This object is thread safe.
 */
public class FhirEncoders {

  /**
   * Cache of Encoders instances.
   */
  private static Map<EncodersKey, FhirEncoders> ENCODERS = new HashMap<>();
  /**
   * The FHIR context used to define the data models.
   */
  private final FhirContext context;
  /**
   * Cached encoders to avoid having to re-create them.
   */
  private final Map<Class, ExpressionEncoder> encoderCache = new HashMap<>();

  FhirEncoders(FhirContext context) {
    this.context = context;
  }

  /**
   * Returns a builder to create encoders
   * for FHIR STU3.
   *
   * @return a builder for encoders.
   */
  public static Builder forStu3() {

    return new Builder(FhirVersionEnum.DSTU3);
  }

  /**
   * Returns an encoder for the given FHIR resource.
   *
   * @param type the type of the resource to encode.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public <T extends IBaseResource> ExpressionEncoder<T> of(Class<T> type) {

    BaseRuntimeElementCompositeDefinition definition =
        context.getResourceDefinition(type);

    synchronized (encoderCache) {

      ExpressionEncoder<T> encoder = encoderCache.get(type);

      if (encoder == null) {

        encoder = (ExpressionEncoder<T>)
            EncoderBuilder.of(definition,
                context,
                new SchemaConverter(context));

        encoderCache.put(type, encoder);
      }

      return encoder;
    }
  }

  /**
   * Returns an encoder for the given FHIR resource by name, as defined
   * by the FHIR specification.
   *
   * @param resourceName the name of the FHIR resource to encode, such as
   *     "Encounter", "Condition", "Observation", etc.
   * @param <T> the type of the resource to be encoded.
   * @return an encoder for the resource.
   */
  public <T extends IBaseResource> ExpressionEncoder<T> of(String resourceName) {

    RuntimeResourceDefinition definition = context.getResourceDefinition(resourceName);

    return of((Class<T>) definition.getImplementingClass());
  }

  /**
   * Immutable key to look up a matching encoders instance
   * by configuration.
   */
  private static class EncodersKey {

    final FhirVersionEnum fhirVersion;

    EncodersKey(FhirVersionEnum fhirVersion) {
      this.fhirVersion = fhirVersion;
    }

    @Override
    public int hashCode() {
      return fhirVersion.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof EncodersKey)) {
        return false;
      }

      EncodersKey that = (EncodersKey) obj;

      return this.fhirVersion == that.fhirVersion;
    }
  }

  /**
   * Encoder builder.
   */
  public static class Builder {

    final FhirVersionEnum fhirVersion;

    Builder(FhirVersionEnum fhirVersion) {

      this.fhirVersion = fhirVersion;
    }

    /**
     * Get or create an {@link FhirEncoders} instance that
     * matches the builder's configuration.
     *
     * @return an Encoders instance.
     */
    public FhirEncoders getOrCreate() {

      EncodersKey key = new EncodersKey(fhirVersion);

      synchronized (ENCODERS) {

        FhirEncoders encoders = ENCODERS.get(key);

        // No instance with the given configuration found,
        // so create one.
        if (encoders == null) {

          FhirContext context = new FhirContext(fhirVersion);

          encoders = new FhirEncoders(context);

          ENCODERS.put(key, encoders);
        }

        return encoders;
      }
    }
  }
}
