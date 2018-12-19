package com.cerner.bunsen.spark;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import scala.Tuple2;

/**
 * Utilities to work with FHIR bundles.
 */
public class Bundles {

  /**
   * A wrapper class for bundles that supports the use of Bundles from various
   * FHIR versions in Spark RDDs.
   */
  public static class BundleContainer implements Serializable {

    private FhirVersionEnum fhirVersion;

    private transient IBaseBundle bundle;

    BundleContainer(IBaseBundle bundle, FhirVersionEnum fhirVersion) {

      this.bundle = bundle;
      this.fhirVersion = fhirVersion;
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();

      String encodedBundle = FhirContexts.contextFor(fhirVersion)
          .newJsonParser()
          .encodeResourceToString(bundle);

      stream.writeUTF(encodedBundle);
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      stream.defaultReadObject();

      String encodedBundle = stream.readUTF();

      bundle = (IBaseBundle) FhirContexts.contextFor(fhirVersion)
          .newJsonParser()
          .parseResource(encodedBundle);
    }

    /**
     * Returns a list containing the resources of the given name
     * that exist in the bundle.
     *
     * @param name the name of the FHIR resource to return
     * @return the resources of the given name that exist in the bundle.
     */
    public List<IBaseResource> extractResource(String name)  {

      return FhirConversionSupport.supportFor(fhirVersion).extractEntryFromBundle(bundle, name);
    }

    /**
     * Returns the contained FHIR bundle.
     *
     * @return the contained bundle.
     */
    public IBaseBundle getBundle() {
      return bundle;
    }

  }

  private final FhirVersionEnum fhirVersion;

  private Bundles(FhirVersionEnum fhirVersion) {
    this.fhirVersion = fhirVersion;
  }

  public static Bundles forStu3() {

    return new Bundles(FhirVersionEnum.DSTU3);
  }

  public static Bundles forR4() {

    return new Bundles(FhirVersionEnum.R4);
  }

  /**
   * Returns an RDD of bundles loaded from the given path.
   *
   * @param spark the spark session
   * @param path a path to a directory of FHIR Bundles
   * @param minPartitions a suggested value for the minimal number of partitions
   * @return an RDD of FHIR Bundles
   */
  public JavaRDD<BundleContainer> loadFromDirectory(SparkSession spark,
      String path,
      int minPartitions) {

    return spark.sparkContext()
        .wholeTextFiles(path, minPartitions)
        .toJavaRDD()
        .map(new ToBundle(fhirVersion));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset that has JSON-encoded
   * bundles in the given column.
   *
   * @param jsonBundles a dataset of JSON-encoded bundles
   * @param column the column in which the JSON bundle is stored
   * @return an RDD of FHIR Bundles
   */
  public JavaRDD<BundleContainer> fromJson(Dataset<Row> jsonBundles, String column) {

    return fromJson(jsonBundles.select(column).as(Encoders.STRING()));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset of JSON-encoded
   * bundles.
   *
   * @param jsonBundles a dataset of JSON-encoded bundles
   * @return an RDD of FHIR Bundles
   */
  public JavaRDD<BundleContainer> fromJson(Dataset<String> jsonBundles) {

    return jsonBundles.toJavaRDD().map(new StringToBundle(false, fhirVersion));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset that has XML-encoded
   * bundles in the given column.
   *
   * @param xmlBundles a dataset of XML-encoded bundles
   * @param column the column in which the XML bundle is stored
   * @return an RDD of FHIR Bundles
   */
  public JavaRDD<BundleContainer> fromXml(Dataset<Row> xmlBundles, String column) {

    return fromXml(xmlBundles.select(column).as(Encoders.STRING()));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset of XML-encoded
   * bundles.
   *
   * @param xmlBundles a dataset of XML-encoded bundles
   * @return an RDD of FHIR Bundles
   */
  public JavaRDD<BundleContainer> fromXml(Dataset<String> xmlBundles) {

    return xmlBundles.toJavaRDD().map(new StringToBundle(true, fhirVersion));
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns
   * it as a Dataset of that type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceClass the type of resource to extract.
   * @return a dataset of the given resource
   */
  public Dataset<Row> extractEntry(SparkSession spark,
      JavaRDD<BundleContainer> bundles,
      Class resourceClass) {

    RuntimeResourceDefinition definition = FhirContexts.contextFor(fhirVersion)
        .getResourceDefinition(resourceClass);

    return extractEntry(spark, bundles, definition.getName());
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns
   * it as a Dataset of that type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceTypeUrl the URL identifying the FHIR resource type or profile.
   * @return a dataset of the given resource
   */
  public Dataset<Row> extractEntry(SparkSession spark,
      JavaRDD<BundleContainer> bundles,
      String resourceTypeUrl) {

    FhirContext context = FhirContexts.contextFor(fhirVersion);

    SparkRowConverter converter = SparkRowConverter.forResource(context, resourceTypeUrl);


    JavaRDD<Row> resourceRdd = bundles.flatMap(
        new ToResourceRow(converter.getResourceType(),
            resourceTypeUrl,
            fhirVersion,
            converter));

    return spark.createDataFrame(resourceRdd.rdd(), converter.getSchema());
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
  public void saveAsDatabase(SparkSession spark,
      JavaRDD<BundleContainer> bundles,
      String database,
      String... resourceNames) {

    spark.sql("create database if not exists " + database);

    for (String resourceName : resourceNames) {

      Dataset ds = extractEntry(spark, bundles, resourceName);

      ds.write().saveAsTable(database + "." + resourceName.toLowerCase());
    }
  }

  private static class StringToBundle implements Function<String, BundleContainer> {

    private boolean isXml;

    private FhirVersionEnum fhirVersion;

    private transient IParser parser;

    StringToBundle(boolean isXml, FhirVersionEnum fhirVersion) {
      this.isXml = isXml;
      this.fhirVersion = fhirVersion;

      parser = isXml
          ? FhirContexts.contextFor(fhirVersion).newXmlParser()
          : FhirContexts.contextFor(fhirVersion).newJsonParser();
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      stream.defaultReadObject();

      parser = isXml
          ? FhirContexts.contextFor(fhirVersion).newXmlParser()
          : FhirContexts.contextFor(fhirVersion).newJsonParser();
    }

    @Override
    public BundleContainer call(String bundleString) throws Exception {

      IBaseBundle bundle = (IBaseBundle) parser.parseResource(bundleString);

      return new BundleContainer(bundle, fhirVersion);
    }
  }

  private static class ToBundle implements Function<Tuple2<String, String>, BundleContainer> {

    private FhirVersionEnum fhirVersion;

    private transient IParser xmlParser;

    private transient IParser jsonParser;

    ToBundle(FhirVersionEnum fhirVersion) {
      this.fhirVersion = fhirVersion;

      xmlParser = FhirContexts.contextFor(fhirVersion).newXmlParser();
      jsonParser = FhirContexts.contextFor(fhirVersion).newJsonParser();
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      stream.defaultReadObject();

      xmlParser = FhirContexts.contextFor(fhirVersion).newXmlParser();
      jsonParser = FhirContexts.contextFor(fhirVersion).newJsonParser();
    }

    @Override
    public BundleContainer call(Tuple2<String, String> fileContentTuple) throws Exception {

      String filePath = fileContentTuple._1.toLowerCase();

      if (filePath.endsWith(".xml")) {

        return new BundleContainer((IBaseBundle) xmlParser.parseResource(fileContentTuple._2()),
            fhirVersion);

      } else if (filePath.endsWith(".json")) {

        return new BundleContainer((IBaseBundle) jsonParser.parseResource(fileContentTuple._2()),
            fhirVersion);

      } else {

        throw new RuntimeException("Unrecognized file extension for resource: " + filePath);
      }
    }
  }

  private static class ToResourceRow implements FlatMapFunction<BundleContainer, Row> {

    private transient String resourceName;

    private transient String resourceTypeUrl;

    private transient FhirVersionEnum fhirVersion;

    private transient SparkRowConverter converter;

    private transient FhirConversionSupport support;

    ToResourceRow(String resourceName,
        String resourceTypeUrl,
        FhirVersionEnum fhirVersion,
        SparkRowConverter converter) {

      this.resourceName = resourceName;
      this.resourceTypeUrl = resourceTypeUrl;
      this.fhirVersion = fhirVersion;

      this.converter = converter;
      this.support = FhirConversionSupport.supportFor(fhirVersion);
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();

      stream.writeUTF(resourceName);
      stream.writeUTF(resourceTypeUrl);
      stream.writeObject(fhirVersion);
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      this.resourceName = stream.readUTF();
      this.resourceTypeUrl = stream.readUTF();
      this.fhirVersion =  (FhirVersionEnum) stream.readObject();

      FhirContext context = FhirContexts.contextFor(fhirVersion);

      this.converter = SparkRowConverter.forResource(context, resourceTypeUrl);

      this.support = FhirConversionSupport.supportFor(fhirVersion);
    }

    @Override
    public Iterator<Row> call(BundleContainer bundle) {

      List<IBaseResource> resources = support.extractEntryFromBundle(bundle.bundle, resourceName);

      return resources.stream()
          .map(resource -> converter.resourceToRow(resource))
          .iterator();
    }
  }
}
