package com.cerner.bunsen.spark;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.StructureDefinitions;
import com.cerner.bunsen.spark.converters.DefinitionToSparkVisitor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;


/**
 * Support for converting FHIR resources to Apache Spark rows and vice versa.
 */
public class SparkRowConverter {

  private final HapiConverter hapiToSparkConverter;

  private final HapiObjectConverter sparkToHapiConverter;

  private SparkRowConverter(HapiConverter hapiToSparkConverter,
      RuntimeResourceDefinition... resources) {

    this.hapiToSparkConverter = hapiToSparkConverter;
    this.sparkToHapiConverter =
        (HapiObjectConverter) hapiToSparkConverter.toHapiConverter(resources);
  }

  /**
   * Returns a row converter for the given resource type. The resource type can
   * either be a relative URL for a base resource (e.g., "Condition" or "Observation"),
   * or a URL identifying the structure definition for a given profile, such as
   * "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient".
   *
   * @param context the FHIR context
   * @param resourceTypeUrl the URL of the resource type
   * @return a row converter instance.
   */
  public static SparkRowConverter forResource(FhirContext context,
      String resourceTypeUrl) {

    return forResource(context, resourceTypeUrl, Collections.emptyList());
  }

  /**
   * Returns a row converter for the given resource type. The resource type can
   * either be a relative URL for a base resource (e.g. "Condition" or "Observation"),
   * or a URL identifying the structure definition for a given profile, such as
   * "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient".
   * <p>
   * Resources that would be contained must be statically declared through this method
   * via similar URLs.
   * </p>
   * @param context the FHIR context
   * @param resourceTypeUrl the URL of the resource type
   * @param containedResourceTypeUrls the list of URLs of contained resource types
   * @return an Avro converter instance.
   */
  public static synchronized SparkRowConverter forResource(FhirContext context,
      String resourceTypeUrl,
      List<String> containedResourceTypeUrls) {

    StructureDefinitions structureDefinitions = StructureDefinitions.create(context);

    Map<String, HapiConverter<DataType>> converters = new HashMap<>();

    String basePackage;
    FhirVersionEnum fhirVersion = context.getVersion().getVersion();
    if (FhirVersionEnum.DSTU3.equals(fhirVersion)) {

      basePackage = "com.cerner.bunsen.stu3.spark";

    } else if (FhirVersionEnum.R4.equals(fhirVersion)) {

      basePackage = "com.cerner.bunsen.r4.spark";

    } else {

      throw new IllegalArgumentException("Unsupported FHIR version " + fhirVersion.toString());
    }

    DefinitionToSparkVisitor visitor = new DefinitionToSparkVisitor(
        structureDefinitions.conversionSupport(), basePackage, converters);

    HapiConverter<DataType> converter = structureDefinitions.transform(visitor,
        resourceTypeUrl,
        containedResourceTypeUrls);

    RuntimeResourceDefinition[] resources =
        new RuntimeResourceDefinition[1 + containedResourceTypeUrls.size()];

    resources[0] = context.getResourceDefinition(converter.getElementType());

    for (int i = 0; i < containedResourceTypeUrls.size(); i++) {

      StructType parentType = (StructType) converter.getDataType();
      ArrayType containerArrayType = (ArrayType) parentType.apply("contained").dataType();
      StructType containerType = (StructType) containerArrayType.elementType();

      resources[i + 1] = context.getResourceDefinition(containerType.apply(i).name());
    }

    return new SparkRowConverter(converter, resources);
  }

  /**
   * Converts a given FHIR resource to a Spark row.
   *
   * @param resource the FHIR resource
   * @return the row
   */
  public Row resourceToRow(IBaseResource resource) {

    return (Row) hapiToSparkConverter.fromHapi(resource);
  }

  public IBaseResource rowToResource(Row row) {

    return (IBaseResource) sparkToHapiConverter.toHapi(row);
  }

  /**
   * Returns the Spark schema equivalent for the FHIR resource.
   *
   * @return the Spark schema
   */
  public StructType getSchema() {

    return (StructType) hapiToSparkConverter.getDataType();
  }

  /**
   * Returns the FHIR type of the resource being converted.
   *
   * @return the FHIR type of the resource being converted.
   */
  public String getResourceType() {
    return hapiToSparkConverter.getElementType();
  }

  /**
   * Returns an empty data frame that has the Spark schema for the resource.
   *
   * @param spark the spark session
   * @return an empty data frame with the expected schema.
   */
  public Dataset<Row> emptyDataFrame(SparkSession spark) {

    return toDataFrame(spark, Collections.emptyList());
  }

  /**
   * Returns a dataframe.
   *
   * @param spark the spark session.
   * @param resources the resources to convert into Spark rows.
   * @return a dataframe containing the resources in row form.
   */
  public Dataset<Row> toDataFrame(SparkSession spark, List<IBaseResource> resources) {

    List<Row> rows = resources.stream()
        .map(this::resourceToRow)
        .collect(Collectors.toList());

    return spark.createDataFrame(rows, getSchema());
  }

}
