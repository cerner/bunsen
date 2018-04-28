package com.cerner.bunsen;

import static ca.uhn.fhir.context.FhirVersionEnum.DSTU3;
import static ca.uhn.fhir.context.FhirVersionEnum.R4;

import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.datatypes.DataTypeMappings;
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
  private static final Map<EncodersKey, FhirEncoders> ENCODERS = new HashMap<>();

  /**
   * Cache of mappings between Spark and FHIR types.
   */
  private static final  Map<FhirVersionEnum, DataTypeMappings> DATA_TYPE_MAPPINGS = new HashMap();

  /**
   * Cache of FHIR contexts.
   */
  private static final Map<FhirVersionEnum, FhirContext> FHIR_CONTEXTS = new HashMap();

  /**
   * The FHIR context used by the encoders instance.
   */
  private final FhirContext context;

  /**
   * The data type mappings used by the encoders instance.
   */
  private final DataTypeMappings mappings;

  /**
   * Cached encoders to avoid having to re-create them.
   */
  private final Map<Class, ExpressionEncoder> encoderCache = new HashMap<>();

  /**
   * Consumers should generally use the {@link #forStu3()} or {@link #forR4()}
   * methods, but this is made available for test purposes and additional
   * experimental mappings.
   *
   * @param context the FHIR context to use.
   * @param mappings mappings between Spark and FHIR data types.
   */
  public FhirEncoders(FhirContext context, DataTypeMappings mappings) {
    this.context = context;
    this.mappings = mappings;
  }

  /**
   * Returns the FHIR context for the given version. This is effectively a cache
   * so consuming code does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirContext contextFor(FhirVersionEnum fhirVersion) {

    synchronized (FHIR_CONTEXTS) {

      FhirContext context = FHIR_CONTEXTS.get(fhirVersion);

      if (context == null) {

        context = new FhirContext(fhirVersion);

        FHIR_CONTEXTS.put(fhirVersion, context);
      }

      return context;
    }
  }

  /**
   * Returns the {@link DataTypeMappings} instance for the given FHIR version.
   *
   * @param fhirVersion the FHIR version for the data type mappings.
   * @return a DataTypeMappings instance.
   */
  static DataTypeMappings mappingsFor(FhirVersionEnum fhirVersion) {

    synchronized (DATA_TYPE_MAPPINGS) {

      DataTypeMappings mappings = DATA_TYPE_MAPPINGS.get(fhirVersion);

      if (mappings == null) {
        String dataTypesClassName;

        switch (fhirVersion)  {

          case DSTU3 :
            dataTypesClassName = "com.cerner.bunsen.stu3.Stu3DataTypeMappings";
            break;

          case R4:
            dataTypesClassName = "com.cerner.bunsen.r4.R4DataTypeMappings";
            break;

          default:
            throw new IllegalArgumentException("Unsupported FHIR version: " + fhirVersion);
        }

        try {

          mappings = (DataTypeMappings) Class.forName(dataTypesClassName).newInstance();

          DATA_TYPE_MAPPINGS.put(fhirVersion, mappings);

        } catch (Exception createClassException) {

          throw new IllegalStateException("Unable to create the data mappings "
              + dataTypesClassName
              + ". This is typically because the HAPI FHIR dependencies for "
              + "the underlying data model are note present. Make sure the "
              + " hapi-fhir-structures-* and hapi-fhir-validation-resources-* "
              + " jars for the desired FHIR version are available on the class path.",
              createClassException);
        }
      }

      return mappings;
    }
  }

  /**
   * Returns a builder to create encoders
   * for FHIR STU3.
   *
   * @return a builder for encoders.
   */
  public static Builder forStu3() {

    return forVersion(DSTU3);
  }

  /**
   * Returns a builder to create encoders
   * for FHIR R4.
   *
   * @return a builder for encoders.
   */
  public static Builder forR4() {

    return forVersion(R4);
  }

  /**
   * Returns a builder to create encoders for the
   * given FHIR version.
   *
   * @param fhirVersion the version of FHIR to use.
   * @return a builder for encoders.
   */
  public static Builder forVersion(FhirVersionEnum fhirVersion) {
    return new Builder(fhirVersion);
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
                mappings,
                new SchemaConverter(context, mappings));

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
   * Returns the version of FHIR used by encoders produced by this instance.
   *
   * @return the version of FHIR used by encoders produced by this instance.
   */
  public FhirVersionEnum getFhirVersion() {

    return context.getVersion().getVersion();
  }

  /**
   * Encoder builder. Today only the FHIR version is specified, but
   * future builders may allow customization of the profile used.
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

          FhirContext context = contextFor(fhirVersion);
          DataTypeMappings mappings = mappingsFor(fhirVersion);

          encoders = new FhirEncoders(context, mappings);

          ENCODERS.put(key, encoders);
        }

        return encoders;
      }
    }
  }
}
