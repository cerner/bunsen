package com.cerner.bunsen.mappings;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.FhirEncoders;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.exceptions.FHIRException;
import scala.Tuple2;

/**
 * An immutable collection of FHIR ConceptMaps. This class is used to import concept
 * map content, explore it, and persist it to a database.
 */
public class ConceptMaps {

  private static final FhirContext FHIR_CONTEXT = FhirContext.forDstu3();

  private static final IParser PARSER = FHIR_CONTEXT.newXmlParser();

  /**
   * An encoder for serializing mappings.
   */
  private static final Encoder<Mapping> MAPPING_ENCODER = Encoders.bean(Mapping.class);

  private static final Encoder<ConceptMap> CONCEPT_MAP_ENCODER = FhirEncoders.forStu3()
      .getOrCreate()
      .of(ConceptMap.class);

  private static final Encoder<UrlAndVersion> URL_AND_VERSION_ENCODER =
      Encoders.bean(UrlAndVersion.class);

  /**
   * Returns the encoder for mappings.
   *
   * @return an encoder for mappings.
   */
  public static Encoder<Mapping> getMappingEncoder() {

    return MAPPING_ENCODER;
  }

  /**
   * Returns the encoder for concept maps.
   *
   * @return an encoder for concept maps.
   */
  public static Encoder<ConceptMap> getConceptMapEncoder() {

    return CONCEPT_MAP_ENCODER;
  }

  /**
   * Returns the encoder for UrlAndVersion tuples.
   *
   * @return an encoder for UrlAndVersion tuples.
   */
  public static Encoder<UrlAndVersion> getUrlAndVersionEncoder() {
    return URL_AND_VERSION_ENCODER;
  }

  /**
   * Default database name where mapping information is stored.
   */
  public static final String MAPPING_DATABASE = "ontologies";

  /**
   * Default table name where expanded mapping information is stored.
   */
  public static final String MAPPING_TABLE = "mappings";

  /**
   * Defalt table name where concept maps are stored.
   */
  public static final String CONCEPT_MAP_TABLE = "conceptmaps";

  private static final Pattern TABLE_NAME_PATTERN =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*\\.?[A-Za-z0-9_]*");

  private final SparkSession spark;

  private final Dataset<ConceptMap> conceptMaps;

  private final Dataset<Mapping> mappings;

  private final Dataset<UrlAndVersion> members;

  private ConceptMaps(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<ConceptMap> conceptMaps,
      Dataset<Mapping> mappings) {

    this.spark = spark;
    this.members = members;
    this.conceptMaps = conceptMaps;
    this.mappings = mappings;
  }

  /**
   * Returns the collection of concept maps from the default database and tables.
   *
   * @param spark the spark session
   * @return a ConceptMaps instance.
   */
  public static ConceptMaps getDefault(SparkSession spark) {

    return getFromDatabase(spark, MAPPING_DATABASE);
  }

  /**
   * Returns the collection of concept maps from the tables in the given database.
   *
   * @param spark the spark session
   * @param databaseName name of the database containing the conceptmaps and mappings tables.
   * @return a ConceptMaps instance.
   */
  public static ConceptMaps getFromDatabase(SparkSession spark, String databaseName) {

    Dataset<Mapping> mappings = spark.sql(
        "SELECT * FROM " + databaseName + "." + MAPPING_TABLE).as(MAPPING_ENCODER);

    Dataset<ConceptMap> conceptMaps = spark
        .sql("SELECT * FROM " + databaseName + "." + CONCEPT_MAP_TABLE)
        .as(CONCEPT_MAP_ENCODER);

    return new ConceptMaps(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        conceptMaps,
        mappings);
  }

  /**
   * Returns an empty ConceptMaps instance.
   *
   * @param spark the spark session
   * @return an empty ConceptMaps instance.
   */
  public static ConceptMaps getEmpty(SparkSession spark) {

    Dataset<ConceptMap> emptyConceptMaps = spark.emptyDataset(CONCEPT_MAP_ENCODER)
        .withColumn("timestamp", lit(null).cast("timestamp"))
        .as(CONCEPT_MAP_ENCODER);

    return new ConceptMaps(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        emptyConceptMaps,
        spark.emptyDataset(MAPPING_ENCODER));
  }

  /**
   * Adds the given mappings to the concept map.
   *
   * @param map the concept map
   * @param mappings the mappings to add
   */
  private static void addToConceptMap(ConceptMap map, Dataset<Mapping> mappings) {

    // Sort the items so they are grouped together optimally, and so
    // we consistently produce the same ordering, therefore making
    // inspection and comparison of the concept maps easier.
    List<Mapping> sortedMappings = mappings.sort("sourceSystem",
        "targetSystem",
        "sourceValue",
        "targetValue")
        .collectAsList();

    ConceptMapGroupComponent currentGroup = null;
    SourceElementComponent element = null;

    // Workaround for the decoder producing an immutable array by
    // replacing it with a mutable one.
    map.setGroup(new ArrayList<>(map.getGroup()));
    for (Mapping mapping: sortedMappings) {

      // Add a new group if we don't match the previous one.
      if (currentGroup == null
          || !mapping.getSourceSystem().equals(currentGroup.getSource())
          || !mapping.getTargetSystem().equals(currentGroup.getTarget())) {

        currentGroup = null;

        // Find a matching group.
        for (ConceptMapGroupComponent candidate: map.getGroup()) {

          if (mapping.getSourceSystem().equals(candidate.getSource())
              && mapping.getTargetSystem().equals(candidate.getTarget())) {

            currentGroup = candidate;

            // Workaround for the decoder producing an immutable array by
            // replacing it with a mutable one.
            currentGroup.setElement(new ArrayList<>(currentGroup.getElement()));
            break;
          }
        }

        // No matching group found, so add it.
        if (currentGroup == null) {
          currentGroup = map.addGroup();

          currentGroup.setSource(mapping.getSourceSystem());
          currentGroup.setTarget(mapping.getTargetSystem());

          // Ensure a new element is created for the newly created group.
          element = null;
        }
      }

      // There is an element for each distinct source value in the map,
      // so add one if it does not match the previous.
      if (element == null
          || !mapping.getSourceValue().equals(element.getCode())) {

        element = currentGroup.addElement();
        element.setCode(mapping.getSourceValue());
      }

      element.addTarget().setCode(mapping.getTargetValue());
    }
  }

  /**
   * Given a concept map, returns a list of mapping records it contains.
   *
   * @param map a concept map
   * @return a list of Mapping records.
   */
  public static List<Mapping> expandMappings(ConceptMap map) {

    List<Mapping> mappings = new ArrayList<>();

    expandMappingsIterator(map).forEachRemaining(mappings::add);

    return mappings;
  }

  private static Iterator<Mapping> expandMappingsIterator(ConceptMap map) {

    List<Mapping> mappings = new ArrayList<>();

    for (ConceptMapGroupComponent group: map.getGroup()) {

      for (SourceElementComponent element: group.getElement()) {

        for (TargetElementComponent target: element.getTarget()) {

          Mapping mapping = new Mapping();

          mapping.setConceptMapUri(map.getUrl());
          mapping.setConceptMapVersion(map.getVersion());

          try {
            String sourceValue = map.getSource() instanceof UriType
                ? map.getSourceUriType().getValue()
                : map.getSourceReference().getReference();

            mapping.setSourceValueSet(sourceValue);

            String targetValue = map.getTarget() instanceof UriType
                ? map.getTargetUriType().getValue()
                : map.getTargetReference().getReference();

            mapping.setTargetValueSet(targetValue);

          } catch (FHIRException fhirException) {

            // This should not happen because we check the types,
            // but rethrow to avoid any possibility of swallowing
            // an exception.
            throw new RuntimeException(fhirException);
          }

          mapping.setSourceSystem(group.getSource());
          mapping.setSourceValue(element.getCode());

          mapping.setTargetSystem(group.getTarget());
          mapping.setTargetValue(target.getCode());

          if (target.getEquivalence() != null) {
            mapping.setEquivalence(target.getEquivalence().toCode());
          }

          mappings.add(mapping);
        }
      }
    }

    return mappings.iterator();
  }

  /**
   * Returns a simple dataset of URL and versions of concept maps.
   */
  private Dataset<UrlAndVersion> getUrlAndVersions(Dataset<ConceptMap> conceptMaps) {

    return conceptMaps.select(functions.col("url"), functions.col("version"))
        .distinct()
        .as(URL_AND_VERSION_ENCODER);
  }

  /**
   * Returns a new ConceptMaps instance that includes the given maps.
   *
   * @param conceptMaps concept maps to add to the returned collection.
   * @return a new ConceptMaps instance with the values added.
   */
  public ConceptMaps withConceptMaps(Dataset<ConceptMap> conceptMaps) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(conceptMaps);

    if (hasDuplicateUrlAndVersions(newMembers) || conceptMaps.count() != newMembers.count()) {

      throw new IllegalArgumentException(
          "Cannot add concept maps having duplicate conceptMapUri and conceptMapVersion");
    }

    // Remove the concept contents for persistence. This is most easily done in the ConceptMap
    // object by setting the group to an empty list.
    Dataset<ConceptMap> withoutConcepts = conceptMaps
        .map((MapFunction<ConceptMap,ConceptMap>) conceptMap -> {

          // Remove the elements rather than the groups to preserved the
          // "unmapped" structure in a group that can refer to other
          // concept maps.
          ConceptMap withoutElements = conceptMap.copy();

          List<ConceptMapGroupComponent> updatedGroups = new ArrayList<>();

          for (ConceptMapGroupComponent group: withoutElements.getGroup()) {

            group.setElement(new ArrayList<>());
            updatedGroups.add(group);
          }

          withoutElements.setGroup(updatedGroups);

          return withoutElements;
        }, CONCEPT_MAP_ENCODER);

    Dataset<Mapping> newMappings = conceptMaps.flatMap(ConceptMaps::expandMappingsIterator,
        MAPPING_ENCODER);

    return withConceptMaps(withoutConcepts, newMappings);
  }

  /**
   * Returns a new ConceptMaps instance that includes the given map.
   *
   * @param conceptMap concept maps to add
   * @return a new ConceptMaps instance with the values added.
   */
  public ConceptMaps withConceptMaps(ConceptMap... conceptMap) {

    return withConceptMaps(Arrays.asList(conceptMap));
  }

  public ConceptMaps withConceptMaps(List<ConceptMap> conceptMaps) {

    return withConceptMaps(this.spark.createDataset(conceptMaps, CONCEPT_MAP_ENCODER));
  }

  private ConceptMaps withConceptMaps(Dataset<ConceptMap> newMaps, Dataset<Mapping> newMappings) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(newMaps);

    // Instantiating a new composite ConceptMaps requires a new timestamp
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    Dataset<ConceptMap> newMapsWithTimestamp = newMaps
        .withColumn("timestamp", lit(timestamp.toString()).cast("timestamp"))
        .as(CONCEPT_MAP_ENCODER);

    return new ConceptMaps(spark,
        this.members.union(newMembers),
        this.conceptMaps.union(newMapsWithTimestamp),
        this.mappings.union(newMappings));
  }

  /**
   * Reads all concept maps from a given directory and adds them to
   * our collection. The directory may be anything readable from a Spark path,
   * including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which concept maps will be loaded
   * @return a instance of ConceptMaps that includes the contents from that directory.
   */
  public ConceptMaps withMapsFromDirectory(String path) {

    return withConceptMaps(conceptMapsDatasetFromDirectory(path));
  }

  private Dataset<ConceptMap> conceptMapsDatasetFromDirectory(String path) {

    JavaRDD<Tuple2<String,String>> fileNamesAndContents = this.spark.sparkContext()
        .wholeTextFiles(path, 1)
        .toJavaRDD();

    return this.spark.createDataset(fileNamesAndContents
        .map(tuple -> (ConceptMap) PARSER.parseResource(tuple._2))
        .rdd(), CONCEPT_MAP_ENCODER);
  }

  /**
   * Returns all concept maps that are disjoint with concept maps stored in the default database and
   * adds them to our collection. The directory may be anything readable from a Spark path,
   * including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which disjoint concept maps will be loaded
   * @return an instance of ConceptMaps that includes content from that directory that is disjoint
   *         with content already contained in the default database.
   */
  public ConceptMaps withDisjointMapsFromDirectory(String path) {

    return withDisjointMapsFromDirectory(path, MAPPING_DATABASE);
  }

  /**
   * Returns all concept maps that are disjoint with concept maps stored in the default database and
   * adds them to our collection. The directory may be anything readable from a Spark path,
   * including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which disjoint concept maps will be loaded
   * @param database the database to check concept maps against
   * @return an instance of ConceptMaps that includes content from that directory that is disjoint
   *         with content already contained in the default database.
   */
  public ConceptMaps withDisjointMapsFromDirectory(String path, String database) {

    Dataset<UrlAndVersion> currentMembers = this.spark
        .sql("SELECT url, version FROM " + database + "." + CONCEPT_MAP_TABLE)
        .as(URL_AND_VERSION_ENCODER)
        .alias("current");

    Dataset<ConceptMap> maps = conceptMapsDatasetFromDirectory(path)
        .alias("new")
        .join(currentMembers, col("new.url").equalTo(col("current.url"))
            .and(col("new.version").equalTo(col("current.version"))),
            "leftanti")
        .as(CONCEPT_MAP_ENCODER);

    return withConceptMaps(maps);
  }

  /**
   * Returns the concept map with the given uri and version, or null if there is no such map.
   *
   * @param uri the uri of the map to return
   * @param version the version of the map to return
   * @return the specified concept map.
   */
  public ConceptMap getConceptMap(String uri, String version) {

    // Load the concept maps, which may contain zero items
    // if the map does not exist.

    // Typecast necessary to placate the Java compiler calling this Scala function.
    ConceptMap[] maps = (ConceptMap[]) this.conceptMaps.filter(
        functions.col("url").equalTo(lit(uri))
            .and(functions.col("version").equalTo(lit(version))))
        .head(1);

    if (maps.length == 0) {

      return null;

    } else {

      ConceptMap map = maps[0];

      Dataset<Mapping> filteredMappings = getMappings(uri, version);

      addToConceptMap(map, filteredMappings);

      return map;
    }
  }

  /**
   * Returns a dataset of concept maps to inspect metadata. Since the mappings
   * themselves can be quite large, the maps in this dataset do not contain them.
   * Instead, users should use the {@link #getMappings()} method to query mappings
   * in depth.
   *
   * @return a dataset of concept maps that do not contain mappings.
   */
  public Dataset<ConceptMap> getMaps() {
    return this.conceptMaps;
  }

  /**
   * Returns a dataset of all mappings in this collection. This is generally used
   * for inspection and debugging of mappings.
   *
   * @return a dataset of all mappings.
   */
  public Dataset<Mapping> getMappings() {
    return this.mappings;
  }

  /**
   * Returns the mappings for the given URI and version.
   *
   * @param uri the uri of the concept map for which we get mappings
   * @param version the version of the concept map for which we get mappings
   * @return a dataset of mappings for the given URI and version.
   */
  public Dataset<Mapping> getMappings(String uri, String version) {

    return this.mappings.where(functions.col("conceptmapuri").equalTo(lit(uri))
        .and(functions.col("conceptmapversion").equalTo(lit(version))));
  }

  /**
   * Returns a dataset with the mappings for each uri and version.
   *
   * @param uriToVersion a map of concept map URI to the version to load
   * @return a dataset of mappings for the given URIs and versions.
   */
  public Dataset<Mapping> getMappings(Map<String,String> uriToVersion) {

    JavaSparkContext context = new JavaSparkContext(this.spark.sparkContext());

    Broadcast<Map<String,String>> broadcastMaps = context.broadcast(uriToVersion);

    return this.mappings.filter((FilterFunction<Mapping>) mapping -> {

      String latestVersion = broadcastMaps.getValue().get(mapping.getConceptMapUri());

      return latestVersion != null && latestVersion.equals(mapping.getConceptMapVersion());
    });
  }

  /**
   * Returns a dataset with the latest mappings for each valueset here.
   *
   * @param uris URIs for the value sets
   * @param includeExperimental include valuesets labeled as experimental
   * @return a dataset of the latest mappings for them.
   */
  public Dataset<Mapping> getLatestMappings(Set<String> uris, boolean includeExperimental) {

    // Since mappings are partitioned by URL and version, in most cases
    // it is more efficient to load separately for each partition
    // and union the results.
    Map<String,String> latestMaps = getLatestVersions(uris, includeExperimental);

    return getMappings(latestMaps);
  }

  /**
   * Returns the latest versions of all concept maps.
   *
   * @param includeExperimental flag to include concept maps marked as experimental
   *
   * @return a map of concept map URLs to the latest version for them.
   */
  public Map<String,String> getLatestVersions(boolean includeExperimental) {

    return getLatestVersions(null, includeExperimental);
  }

  /**
   * Returns the latest versions of a given set of concept maps.
   *
   * @param urls a set of URLs to retrieve the latest version for, or null to load them all.
   * @param includeExperimental flag to include concept maps marked as experimental
   *
   * @return a map of concept map URLs to the latest version for them.
   */
  public Map<String,String> getLatestVersions(final Set<String> urls,
      boolean includeExperimental) {

    // Reduce by the concept map URI to return only the latest version
    // per concept map. Spark's provided max aggregation function
    // only works on numeric types, so we jump into RDDs and perform
    // the reduce by hand.
    JavaRDD<UrlAndVersion> changes = this.conceptMaps.select(col("url"),
        col("version"),
        col("experimental"))
        .toJavaRDD()
        .filter(row -> (urls == null || urls.contains(row.getString(0)))
            && (includeExperimental || row.isNullAt(2) || !row.getBoolean(2)))
        .mapToPair(row -> new Tuple2<>(row.getString(0), row.getString(1)))
        .reduceByKey((leftVersion, rightVersion) ->
            leftVersion.compareTo(rightVersion) > 0 ? leftVersion : rightVersion)
        .map(tuple -> new UrlAndVersion(tuple._1, tuple._2));

    return this.spark.createDataset(changes.rdd(), URL_AND_VERSION_ENCODER)
        .collectAsList()
        .stream()
        .collect(Collectors.toMap(UrlAndVersion::getUrl,
            UrlAndVersion::getVersion));
  }

  /**
   * Returns true if the UrlAndVersions of new value sets contains duplicates with the current
   * ValueSets.
   */
  private boolean hasDuplicateUrlAndVersions(Dataset<UrlAndVersion> membersToCheck) {

    return this.members.intersect(membersToCheck).count() > 0;
  }

  /**
   * Creates a table of mapping records partitioned by conceptmapuri and
   * conceptmapversion.
   *
   * @param spark the spark session
   * @param tableName the name of the mapping table
   * @param location the location to store the table, or null to create a Hive-managed table.
   * @throws IllegalArgumentException if the table name or location are malformed.
   */
  private static void createMappingTable(SparkSession spark,
      String tableName,
      String location) {

    if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
      throw new IllegalArgumentException("Invalid table name: " + tableName);
    }

    // Hive will check for well-formed paths, so we just ensure
    // a user isn't attempting to inject additional SQL into the statement.
    if (location != null && location.contains(";")) {
      throw new IllegalArgumentException("Invalid path for mapping table: "
          + location);
    }

    StringBuilder builder = new StringBuilder();

    if (location != null) {

      builder.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");

    } else {

      builder.append("CREATE TABLE IF NOT EXISTS ");
    }

    builder.append(tableName);

    // Note the partitioned by columns are deliberately lower case here,
    // since Spark does not appear to match columns to
    // Hive partitions if they are not.
    builder.append("(sourceValueSet STRING, "
        + "targetValueSet STRING, "
        + "sourceSystem STRING, "
        + "sourceValue STRING, "
        + "targetSystem STRING, "
        + "targetValue STRING, "
        + "equivalence STRING)\n"
        + "PARTITIONED BY (conceptmapuri STRING, conceptmapversion STRING)\n");

    builder.append("STORED AS PARQUET\n");

    if (location != null) {
      builder.append("LOCATION '")
          .append(location)
          .append("'");
    }

    spark.sql(builder.toString());
  }

  /**
   * Writes the updated concept maps to a database using the default "mappings" and "conceptmaps"
   * table names.
   *
   * @param database the database name to which the concepts are saved.
   */
  public void writeToDatabase(String database) {

    writeToTables(database + "." + MAPPING_TABLE,
        database + "." + CONCEPT_MAP_TABLE);
  }

  /**
   * Writes mapping records to a table. This class ensures the columns and partitions are mapped
   * properly, and is a workaround similar to the problem described <a
   * href="http://stackoverflow.com/questions/35313077/pyspark-order-of-column-on-write-to-mysql-with-jdbc">here</a>.
   *
   * @param mappings a dataset of mapping records
   * @param tableName the table to write them to
   */
  private static void writeMappingsToTable(Dataset<Mapping> mappings,
      String tableName) {

    // Note the last two columns here must be the partitioned-by columns
    // in order and in lower case for Spark to properly match
    // them to the partitions.
    Dataset<Row> orderedColumnDataset =
        mappings.select("sourceValueSet",
            "targetValueSet",
            "sourceSystem",
            "sourceValue",
            "targetSystem",
            "targetValue",
            "equivalence",
            "conceptmapuri",
            "conceptmapversion");

    orderedColumnDataset
        .write()
        .insertInto(tableName);
  }

  /**
   * Writes mappings to the given tables.
   *
   * <p>Warning: these updates are likely <em>not</em> atomic due to the lack of transactional
   * semantics in the underlying data store. Concurrent users may see previous items
   * removed before new ones are added, or items appear separately than others. This is intended
   * for use in a user-specific sandbox or staging environment.
   *
   * @param mappingsTable name of the table containing the mapping records
   * @param conceptMapTable name of the table containing the concept map metadata
   */
  public void writeToTables(String mappingsTable, String conceptMapTable) {

    boolean hasExistingMaps;

    try {

      this.spark.sql("describe table " + conceptMapTable);

      hasExistingMaps = true;

    } catch (Exception describeException) {

      // Checked exceptions when calling into Scala upset the Java compiler,
      // hence the need for this workaround and re-throw to propagate unexpected
      // failures.
      if (describeException instanceof NoSuchTableException) {

        hasExistingMaps = false;

      } else {

        throw new RuntimeException(describeException);
      }
    }

    if (!hasExistingMaps) {

      // No target tables exist, so create and write them. The mappings
      // and ancestors tables are created explicitly to meet our
      // partitioning system.
      createMappingTable(this.spark, mappingsTable, null);

      // Create a concept map table by writing empty data having the proper schema and properties
      this.spark.emptyDataset(CONCEPT_MAP_ENCODER)
          .withColumn("timestamp", lit(null).cast("timestamp"))
          .write()
          .format("parquet")
          .partitionBy("timestamp")
          .saveAsTable(conceptMapTable);
    }

    Dataset<UrlAndVersion> currentMembers = this.spark
        .sql("SELECT url, version FROM " + conceptMapTable)
        .distinct()
        .as(URL_AND_VERSION_ENCODER);

    if (hasDuplicateUrlAndVersions(currentMembers)) {

      throw new IllegalArgumentException("The given concept maps contains duplicates url and "
          + "versions against concept maps already stored in the table, " + conceptMapTable);
    }

    writeMappingsToTable(this.mappings, mappingsTable);

    this.conceptMaps.write()
        .mode(SaveMode.ErrorIfExists)
        .insertInto(conceptMapTable);
  }
}
