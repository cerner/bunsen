package com.cerner.bunsen.stu3.python;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.SparkRowConverter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.dstu3.model.BaseResource;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.instance.model.api.IBaseResource;


/**
 * Common functions for working with FHIR resources.
 */
public class Functions {

  private static final FhirContext CONTEXT = FhirContexts.forStu3();

  /**
   * Converts a set of FHIR resources to JSON.
   *
   * @param dataset a dataset containing FHIR resources
   * @param resourceTypeUrl the FHIR resource type
   * @return a dataset of JSON strings for the FHIR resources
   */
  public static Dataset<String> toJson(Dataset<Row> dataset, String resourceTypeUrl) {

    return dataset.map(new ToJson(resourceTypeUrl), Encoders.STRING());
  }

  /**
   * Returns a bundle containing all resources in the dataset. This should be used
   * with caution for large datasets, since the returned bundle will include all data.
   *
   * @param dataset a dataset of FHIR resoruces
   * @param resourceTypeUrl the FHIR resource type
   * @return a bundle containing those resources.
   */
  public static Bundle toBundle(Dataset<Row> dataset,
      String resourceTypeUrl) {

    List<Row> resources = (List<Row>) dataset.collectAsList();

    SparkRowConverter converter = SparkRowConverter.forResource(CONTEXT,resourceTypeUrl);

    Bundle bundle = new Bundle();

    for (Row row : resources) {

      IBaseResource resource = converter.rowToResource(row);

      bundle.addEntry().setResource((Resource) resource);
    }

    return bundle;
  }

  /**
   * Returns a JSON string representing the resources as a bundle. This should be used
   * with caution for large datasets, since the returned string will include all data.
   *
   * @param dataset a dataset of FHIR resources.
   * @param resourceTypeUrl the URL of the FHIR resource type contained in the dataset.
   * @return A string containing the JSON representation of the bundle.
   */
  public static String toJsonBundle(Dataset<Row> dataset, String resourceTypeUrl) {

    Bundle bundle = toBundle(dataset, resourceTypeUrl);

    return CONTEXT.newJsonParser().encodeResourceToString(bundle);
  }

  static class ToJson implements MapFunction<Row, String> {

    /**
     * Create a new parser instance for each object, since they are not
     * guaranteed to be thread safe.
     */
    private transient IParser parser = CONTEXT.newJsonParser();

    private String resourceTypeUrl;

    private transient SparkRowConverter converter;

    ToJson(String resourceTypeUrl) {

      this.resourceTypeUrl = resourceTypeUrl;
    }

    private void readObject(ObjectInputStream in) throws IOException,
        ClassNotFoundException {

      in.defaultReadObject();

      parser = CONTEXT.newJsonParser();

      converter = SparkRowConverter.forResource(CONTEXT, resourceTypeUrl);
    }

    public String call(Row row) throws Exception {

      IBaseResource resource = converter.rowToResource(row);

      return parser.encodeResourceToString(resource);
    }
  }

  /**
   * Converts a resource to its XML representation.
   *
   * @param resource the resource
   * @return a string containing XML for the resource.
   */
  public static String resourceToXml(Resource resource) {

    IParser parser = CONTEXT.newXmlParser();

    return parser.encodeResourceToString(resource);
  }

  /**
   * Converts a resource to its JSON representation.
   *
   * @param resource the resource
   * @return a string containing JSON for the resource.
   */
  public static String resourceToJson(Resource resource) {

    IParser parser = CONTEXT.newJsonParser();

    return parser.encodeResourceToString(resource);
  }
}
