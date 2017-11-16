package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.instance.model.api.IBaseResource;
import scala.Tuple2;

/**
 * Utilities to work with FHIR bundles.
 */
public class Bundles {

  private static final FhirContext context = FhirContext.forDstu3();

  /**
   * Returns an RDD of bundles loaded from the given path.
   *
   * @param spark the spark session
   * @param path a path to a directory of FHIR Bundles
   * @param minPartitions a suggested value for the minimal number of partitions
   * @return an RDD of FHIR Bundles
   */
  public static JavaRDD<Bundle> loadFromDirectory(SparkSession spark,
      String path,
      int minPartitions) {

    return spark.sparkContext()
        .wholeTextFiles(path, minPartitions)
        .toJavaRDD()
        .map(new ToBundle());
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns
   * it as a Dataset of that type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceClass the type of resource to extract.
   * @param <T> the type of the resource being extracted from the bundles.
   * @return a dataset of the given resource
   */
  public static <T extends IBaseResource> Dataset<T> extractEntry(SparkSession spark,
      JavaRDD<Bundle> bundles,
      Class<T> resourceClass) {

    RuntimeResourceDefinition definition = context.getResourceDefinition(resourceClass);

    return extractEntry(spark, bundles, definition.getName());
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns
   * it as a Dataset of that type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceName the FHIR name of the resource type to extract
   *     (e.g., condition, patient, etc).
   * @param <T> the type of the resource being extracted from the bundles.
   * @return a dataset of the given resource
   */
  public static <T extends IBaseResource> Dataset<T> extractEntry(SparkSession spark,
      JavaRDD<Bundle> bundles,
      String resourceName) {

    return extractEntry(spark,
        bundles,
        resourceName,
        FhirEncoders.forStu3().getOrCreate());
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns
   * it as a Dataset of that type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceName the FHIR name of the resource type to extract
   *     (e.g., condition, patient. etc).
   * @param encoders the Encoders instance defining how the resources are encoded.
   * @param <T> the type of the resource being extracted from the bundles.
   * @return a dataset of the given resource
   */
  public static <T extends IBaseResource> Dataset<T> extractEntry(SparkSession spark,
      JavaRDD<Bundle> bundles,
      String resourceName,
      FhirEncoders encoders) {

    RuntimeResourceDefinition def = context.getResourceDefinition(resourceName);

    JavaRDD<T> resourceRdd = bundles.flatMap(new ToResource<T>(def.getName()));

    Encoder<T> encoder = encoders.of((Class<T>) def.getImplementingClass());

    return spark.createDataset(resourceRdd.rdd(), encoder);
  }

  /**
   * Saves an RDD of bundles as a database, where each table
   * has the resource name. This offers a simple way to load and query
   * bundles in a system, although users with more sophisticated ETL
   * operations may want to explicitly write different entities.
   *
   * <p>
   * Note this will access the given RDD of bundles once per resource name,
   * so consumers with enough memory should consider calling
   * {@link JavaRDD#cache()} so that RDD is not recomputed for each.
   * </p>
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param database the name of the database to write to
   * @param resourceNames names of resources to be extracted from the bundle and written
   */
  public static void saveAsDatabase(SparkSession spark,
      JavaRDD<Bundle> bundles,
      String database,
      String... resourceNames) {

    spark.sql("create database if not exists " + database);

    for (String resourceName : resourceNames) {

      Dataset ds = extractEntry(spark, bundles, resourceName);

      ds.write().saveAsTable(database + "." + resourceName.toLowerCase());
    }
  }

  private static class ToBundle implements Function<Tuple2<String, String>, Bundle> {

    private static final IParser parser = context.newXmlParser();

    @Override
    public Bundle call(Tuple2<String, String> fileContentTuple) throws Exception {

      return (Bundle) parser.parseResource(fileContentTuple._2());
    }
  }

  private static class ToResource<T> implements FlatMapFunction<Bundle, T> {

    private String resourceName;

    ToResource(String resourceName) {
      this.resourceName = resourceName;
    }

    @Override
    public Iterator<T> call(Bundle bundle) throws Exception {

      List<T> items = new ArrayList<>();

      for (Bundle.BundleEntryComponent component : bundle.getEntry()) {

        Resource resource = component.getResource();

        if (resource != null
            && resourceName.equals(resource.getResourceType().name())) {

          items.add((T) resource);
        }

      }

      return items.iterator();
    }
  }
}
