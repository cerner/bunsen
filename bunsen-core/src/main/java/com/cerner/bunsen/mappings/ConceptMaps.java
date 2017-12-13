package com.cerner.bunsen.mappings;

import static org.apache.spark.sql.functions.col;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.FhirEncoders;
import com.cerner.bunsen.FhirEncoders;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

  /**
   * An encoder for serializing mappings.
   */
  private static final Encoder<Mapping> MAPPING_ENCODER = Encoders.bean(Mapping.class);

  private static final Encoder<ConceptMap> CONCEPT_MAP_ENCODER = FhirEncoders.forStu3()
      .getOrCreate()
      .of(ConceptMap.class);

  private static final Encoder<Ancestor> ANCESTOR_ENCODER = Encoders.bean(Ancestor.class);

  private static final Encoder<UrlAndVersion> URL_AND_VERSION_ENCODER =
      Encoders.bean(UrlAndVersion.class);

  /**
   * The number of records to put in a slice of expanded ancestors.
   * This just needs to be small enough to fit in a reasonable amount of memory
   * when converting to a Dataset.
   */
  private static final long ANCESTOR_SLICE_SIZE = 100000;

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
   * Returns the encoder for ancestors.
   *
   * @return an encoder for ancestors.
   */
  public static Encoder<Ancestor> getAncestorEncoder() {

    return ANCESTOR_ENCODER;
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
   * Default table name where ancestor information is stored.
   */
  public static final String ANCESTOR_TABLE = "ancestors";

  /**
   * Defualt table name where concept maps are stored.
   */
  public static final String CONCEPT_MAP_TABLE = "conceptmaps";

  private static final Pattern TABLE_NAME_PATTERN =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*\\.?[A-Za-z0-9_]*");

  private static final StructType MAP_AND_VERSION_SCHEMA =
      DataTypes.createStructType(new StructField[]{
          DataTypes.createStructField("conceptmapuri", DataTypes.StringType, false),
          DataTypes.createStructField("conceptmapversion", DataTypes.StringType, false)});

  private final SparkSession spark;

  private final Dataset<ConceptMap> conceptMaps;

  private final Dataset<Mapping> mappings;

  private final Dataset<Ancestor> ancestors;

  /**
   * Concept maps that have been changed from the original source.
   */
  private final Dataset<UrlAndVersion> changes;

  private ConceptMaps(SparkSession spark,
      Dataset<UrlAndVersion> changes,
      Dataset<ConceptMap> conceptMaps,
      Dataset<Mapping> mappings,
      Dataset<Ancestor> ancestors) {
    this.spark = spark;
    this.changes = changes;
    this.conceptMaps = conceptMaps;
    this.mappings = mappings;
    this.ancestors = ancestors;
  }

  /**
   * Returns the collection of concept maps from the default table.
   *
   * @param spark the spark session
   * @return a ConceptMaps instance
   */
  public static ConceptMaps getDefault(SparkSession spark) {

    return getFromDatabase(spark, MAPPING_DATABASE);
  }

  /**
   * Returns the collection of concept maps from the tables in the given database
   *
   * @param spark the spark session
   * @param databaseName name of the datase containing the conceptmaps and mappings tables.
   * @return a ConceptMaps instance
   */
  public static ConceptMaps getFromDatabase(SparkSession spark, String databaseName) {

    Dataset<Mapping> mappings = asMappings(spark.sql(
        "select * from " + databaseName + "." + MAPPING_TABLE));

    Dataset<Ancestor> ancestors = asAncestors(spark.sql(
        "select * from " + databaseName + "." + ANCESTOR_TABLE));

    return new ConceptMaps(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        spark.sql("select * from " + databaseName + "." + CONCEPT_MAP_TABLE)
            .as(CONCEPT_MAP_ENCODER),
        mappings,
        ancestors);
  }

  /**
   * Returns an empty ConceptMaps instance.
   *
   * @param spark the spark session
   * @return an empty ConceptMaps instance
   */
  public static ConceptMaps getEmpty(SparkSession spark) {

    return new ConceptMaps(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        spark.emptyDataset(CONCEPT_MAP_ENCODER),
        spark.emptyDataset(MAPPING_ENCODER),
        spark.emptyDataset(ANCESTOR_ENCODER));
  }

  /**
   * URL and version tuple used to uniquely identify a concept map.
   */
  public static class UrlAndVersion {

    String url;

    String version;

    public UrlAndVersion(String url, String version) {
      this.url = url;
      this.version = version;
    }


    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getVersion() {
      return version;
    }

    public void setVersion(String version) {
      this.version = version;
    }

    /**
     * Nullary constructor for use in Spark data sets.
     */
    public UrlAndVersion() {
    }

    @Override
    public int hashCode() {

      return 17 * url.hashCode() * version.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof UrlAndVersion)) {
        return false;
      }

      UrlAndVersion that = (UrlAndVersion) obj;

      return this.url.equals(that.url)
          && this.version.equals(that.version);
    }
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

            // Workaround for the decoder producing an immutable  array by
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
   * Given a concept map, returns the list of mapping records it contains.
   *
   * @param map a concept map
   * @return a list of Mapping records.
   */
  public static List<Mapping> expandMappings(ConceptMap map) {

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

    return mappings;
  }

  /**
   * Returns the mapping entries from a given concept maps.
   *
   * @param spark the spark session
   * @param maps the concept maps
   * @return a map from the concept map url and version to its mapping content.
   */
  private static Map<UrlAndVersion,Dataset<Mapping>> fromConceptMaps(SparkSession spark,
      List<ConceptMap> maps) {

    Map<UrlAndVersion,Dataset<Mapping>> datasets = new HashMap<>();

    for (ConceptMap map: maps) {

      datasets.put(new UrlAndVersion(map.getUrl(), map.getVersion()),
          asMappings(spark.createDataset(expandMappings(map),
              MAPPING_ENCODER)));
    }

    return datasets;
  }

  /**
   * Returns a simple dataset of URL and versions of concept maps.
   */
  private Dataset<UrlAndVersion> getUrlAndVersions(Dataset<ConceptMap> conceptMaps) {

    return conceptMaps.select(
        functions.col("url"),
        functions.col("version"))
        .as(URL_AND_VERSION_ENCODER);
  }

  /**
   * Convert a dataset into mappings with a consistent order, as Spark operations seem
   * to have some surprising behavior if this isn't the case.
   */
  private static Dataset<Mapping> asMappings(Dataset<?> ds) {

    return ds.select(
        "sourceValueSet",
        "targetValueSet",
        "sourceSystem",
        "sourceValue",
        "targetSystem",
        "targetValue",
        "equivalence",
        "conceptmapuri",
        "conceptmapversion")
        .as(MAPPING_ENCODER);
  }

  /**
   * Convert a dataset into ancestors with a consistent order, as Spark operations seem
   * to have some surprising behavior if this isn't the case.
   */
  private static Dataset<Ancestor> asAncestors(Dataset<?> ds) {

    return ds.select(
        "descendantValue",
        "descendantSystem",
        "ancestorSystem",
        "ancestorValue",
        "conceptmapuri",
        "conceptmapversion")
        .as(ANCESTOR_ENCODER);
  }


  /**
   * A single system,value tuple and its parents. Additional connection
   * types beyond parents may be added as necessary.
   */
  private static class ConceptNode implements Serializable {

    String system;
    String value;

    /**
     * The set of parents. This purposefully relies on the Java
     * default equality semantics, since we only use it internally
     * and it is an efficient way to check for the direct parent
     * of a record.
     */
    Set<ConceptNode> parents;

    ConceptNode(String system, String value) {

      this.system = system;
      this.value = value;
      this.parents = new HashSet<>();
    }

    /**
     * Returns the node's ancestors.
     */
    Set<ConceptNode> getAncestors() {

      Set<ConceptNode> output = new HashSet<>();

      getAncestors(output);

      // The current node is included so we can check for cycles,
      // but it should not produce an ancestor record, so remove it.
      output.remove(this);

      return output;
    }

    private void getAncestors(Set<ConceptNode> visited) {

      // Some input data can contain cycles, so we must explicitly check for that.
      if (!visited.contains(this)) {

        visited.add(this);

        for (ConceptNode parent: parents) {

          parent.getAncestors(visited);
        }
      }
    }
  }

  /**
   * Expands a mapping dataset into its ancestors.
   */
  private Dataset<Ancestor> expandAncestors(Map<UrlAndVersion,Dataset<Mapping>> newMappings) {

    return newMappings.entrySet().stream().map(entry ->
        expandAncestors(entry.getKey().getUrl(),
            entry.getKey().getVersion(),
            entry.getValue()))
        .reduce(Dataset::union)
        .get();
  }

  /**
   * Expands the mappings into a dataset of ancestors.
   */
  private Dataset<Ancestor> expandAncestors(String conceptMapUri,
      String conceptMapVersion,
      Dataset<Mapping> mappings) {

    // Map used to find previously created concept nodes so we can
    // use them to build a graph.
    final Map<String, Map<String, ConceptNode>> conceptNodes = new HashMap<>();

    // List of all nodes for simpler iteration.
    final List<ConceptNode> allNodes = new ArrayList<>();

    // Helper function to get or add a node to our colleciton of nodes.
    BiFunction<String,String,ConceptNode> getOrAddNode = (system, value) -> {

      Map<String, ConceptNode> systemMap = conceptNodes.get(system);

      if (systemMap == null) {

        systemMap = new HashMap<>();

        conceptNodes.put(system, systemMap);
      }

      ConceptNode node = systemMap.get(value);

      if (node == null) {

        node = new ConceptNode(system, value);
        systemMap.put(value, node);
        allNodes.add(node);

      }

      return node;
    };

    List<Mapping> subsumesMappings = mappings.where(functions.col("equivalence")
        .equalTo(functions.lit("subsumes")))
        .collectAsList();

    // Build our graph of nodes.
    for (Mapping mapping: subsumesMappings) {

      ConceptNode node = getOrAddNode.apply(mapping.getSourceSystem(),
          mapping.getSourceValue());

      ConceptNode parent = getOrAddNode.apply(mapping.getTargetSystem(),
          mapping.getTargetValue());

      node.parents.add(parent);
    }

    // The graph is built, now translate it into ancestors.
    List<Ancestor> ancestors = allNodes.stream()
        .flatMap(node ->
            node.getAncestors()
                .stream()
                .map(ancestorNode ->
                    new Ancestor(conceptMapUri,
                        conceptMapVersion,
                        node.system,
                        node.value,
                        ancestorNode.system,
                        ancestorNode.value)))
        .collect(Collectors.toList());

    // We convert into a sliced RDD, then to a dataset,
    // so we can specify a slice size and prevent Spark from
    // attempting to copy everything at once for very large
    // expansions.
    int slices = (int) (ancestors.size() / ANCESTOR_SLICE_SIZE);

    if (slices > 1) {

      JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

      JavaRDD<Ancestor> rdd = jsc.parallelize(ancestors, slices);

      return spark.createDataset(rdd.rdd(), ANCESTOR_ENCODER);

    } else {

      return spark.createDataset(ancestors, ANCESTOR_ENCODER);
    }
  }

  /**
   * Returns a new ConceptMaps instance that includes the given maps.
   *
   * @param conceptMaps concept maps to add to the returned collection.
   * @return a new ConceptMaps instance with the values added
   */
  public ConceptMaps withConceptMaps(List<ConceptMap> conceptMaps) {

    // Remove the concept contents for persistence.
    // This is most easily done in the ConcpeptMap object by setting
    // the group to an empty list.
    List<ConceptMap> withoutConcepts = conceptMaps.stream()
        .map(conceptMap -> {

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
        })
        .collect(Collectors.toList());

    // Convert to datasets.
    Dataset<ConceptMap> newMaps = spark.createDataset(withoutConcepts, CONCEPT_MAP_ENCODER);
    Map<UrlAndVersion,Dataset<Mapping>> newMappings = fromConceptMaps(spark, conceptMaps);

    return withConceptMaps(newMaps, newMappings);
  }

  /**
   * Returns a new ConceptMaps instance that includes the given map.
   *
   * @param conceptMap concept maps to add
   * @return a new ConceptMaps instance with the values added
   */
  public ConceptMaps withConceptMaps(ConceptMap... conceptMap) {

    return withConceptMaps(Arrays.asList(conceptMap));
  }

  private ConceptMaps withConceptMaps(Dataset<ConceptMap> newMaps,
      Map<UrlAndVersion,Dataset<Mapping>> newMappings) {

    Dataset<Ancestor> newAncestors = expandAncestors(newMappings);

    // Get the changed changedVersion and column so we can filter
    // existing items that have been changed.
    Dataset<UrlAndVersion> changes = getUrlAndVersions(newMaps);

    Dataset<ConceptMap> unchangedMaps = this.conceptMaps.alias("maps")
        .join(changes.alias("changes"),
            functions.col("maps.url").equalTo(functions.col("changes.url")).and(
                functions.col("maps.version").equalTo(functions.col("changes.version"))),
            "leftanti")
        .as(CONCEPT_MAP_ENCODER);

    Dataset<Mapping> unchangedMappings =
        asMappings(this.mappings.join(changes.alias("changes"),
            functions.col("conceptmapuri")
                .equalTo(functions.col("changes.url")).and(
                functions.col("conceptmapversion")
                    .equalTo(functions.col("changes.version"))),
            "leftanti"));

    Dataset<Ancestor> unchangedAncestors =
        asAncestors(this.ancestors.join(changes.alias("changes"),
            functions.col("conceptmapuri")
                .equalTo(functions.col("changes.url")).and(
                functions.col("conceptmapversion")
                    .equalTo(functions.col("changes.version"))),
            "leftanti"));

    // Reduce the new mappings into values
    Dataset<Mapping> allNewMappings = newMappings.values()
        .stream()
        .reduce(Dataset::union)
        .get();

    // Return a new instance with new or updated values unioned with previous, unchanged values.
    return new ConceptMaps(spark,
        this.changes.unionAll(changes).distinct(),
        unchangedMaps.unionAll(newMaps),
        unchangedMappings.unionAll(asMappings(allNewMappings)),
        unchangedAncestors.unionAll(asAncestors(newAncestors)));
  }

  /**
   * Returns a new ConceptMaps instance that includes the expanded content from the map.
   *
   * @param map a concept map that contains only metadata, without concept groups
   * @param mappings the mappings associated with the given concept map
   * @return a new ConceptMaps instance with the values added
   */
  public ConceptMaps withExpandedMap(ConceptMap map, Dataset<Mapping> mappings) {

    if (map.getGroup().size() != 0) {
      throw new IllegalArgumentException("The concept concepts themselves should be in the"
          + " provided mappings parameter.");
    }

    return withConceptMaps(spark.createDataset(ImmutableList.of(map),
        CONCEPT_MAP_ENCODER),
        ImmutableMap.of(new UrlAndVersion(map.getUrl(), map.getVersion()), mappings));
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

    final IParser parser = FHIR_CONTEXT.newXmlParser();

    List<Tuple2<String,String>> fileNamesAndContents =
        spark.sparkContext()
            .wholeTextFiles(path, 1)
            .toJavaRDD().collect();

    List<ConceptMap> mapList = fileNamesAndContents.stream()
        .map(tuple -> (ConceptMap) parser.parseResource(tuple._2))
        .collect(Collectors.toList());

    return withConceptMaps(mapList);
  }

  /**
   * Returns the concept map with the given uri and version, or null if there is no such map.
   *
   * @param uri the uri of the map to return
   * @param version the version of the map to return
   * @return the specified concept map
   */
  public ConceptMap getConceptMap(String uri, String version) {

    // Load the concept maps, which may contain zero items
    // if the map does not exist.

    // Typecast necessary to placate the Java compiler calling this Scala function.
    ConceptMap[] maps = (ConceptMap[]) conceptMaps.filter(
        functions.col("url").equalTo(functions.lit(uri))
            .and(functions.col("version").equalTo(functions.lit(version))))
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
   * @return a dataset of concept maps that do not containmappings.
   */
  public Dataset<ConceptMap> getMaps() {
    return conceptMaps;
  }

  /**
   * Returns a dataset of all mappings in this collection. This is generally used
   * for inspection and debugging of mappings.
   *
   * @return a dataset of all mappings.
   */
  public Dataset<Mapping> getMappings() {
    return mappings;
  }

  /**
   * Returns the mappings for the given URI and version.
   *
   * @param uri the uri of the concept map for which we get mappings
   * @param version the version of the concept map for which we get mappings
   * @return a dataset of mappings for the given URI and version
   */
  public Dataset<Mapping> getMappings(String uri, String version) {

    return mappings.where(functions.col("conceptmapuri").equalTo(functions.lit(uri))
        .and(functions.col("conceptmapversion").equalTo(functions.lit(version))));
  }

  /**
   * Returns a dataset with the mappings for each uri and version.
   *
   * @param uriToVersion a map of concept map URI to the version to load
   * @return a datset of mapppings for the given URIs and versions
   */
  public Dataset<Mapping> getMappings(Map<String,String> uriToVersion) {

    JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

    Broadcast<Map<String,String>> broadcastMaps = context.broadcast(uriToVersion);

    return mappings.filter((FilterFunction<Mapping>) mapping -> {

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
   * Returns a dataset of all mappings in this collection. This is generally used
   * for inspection and debugging of these relationships.
   *
   * @return a dataset of all transitive ancestors.
   */
  public Dataset<Ancestor> getAncestors() {
    return ancestors;
  }

  /**
   * Returns the latest versions of all concept maps.
   *
   * @param includeExperimental flag to include concept maps marked as experimental
   *
   * @return a map of concept map URLs to the latest version for them
   */
  public Map<String,String> getLatestVersions(boolean includeExperimental) {

    return getLatestVersions(null, includeExperimental);
  }

  /**
   * Returns the latest versions of a given set of concept maps.
   *
   * @param urls a set of URLs to retreieve the latest version for, or null to load them all.
   * @param includeExperimental flag to include concept maps marked as experimental
   *
   * @return a map of concept map URLs to the latest version for them
   */
  public Map<String,String> getLatestVersions(final Set<String> urls,
      boolean includeExperimental) {

    // Reduce by the concept map URI to return only the latest version
    // per concept map. Spark's provided max aggregation function
    // only works on numeric types, so we jump into RDDs and perform
    // the reduce by hand.
    JavaRDD<UrlAndVersion> changes = conceptMaps.select(col("url"),
        col("version"),
        col("experimental"))
        .toJavaRDD()
        .filter(row -> {
          return (urls == null || urls.contains(row.getString(0)))
              && (includeExperimental || row.isNullAt(2) || !row.getBoolean(2));
        })
        .mapToPair(row -> new Tuple2<>(row.getString(0), row.getString(1)))
        .reduceByKey((leftVersion, rightVersion) ->
            leftVersion.compareTo(rightVersion) > 0 ? leftVersion : rightVersion)
        .map(tuple -> new UrlAndVersion(tuple._1, tuple._2));

    return spark.createDataset(changes.rdd(), URL_AND_VERSION_ENCODER)
        .collectAsList()
        .stream()
        .collect(Collectors.toMap(UrlAndVersion::getUrl,
            UrlAndVersion::getVersion));
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
   * Creates a table of ancestor records partitioned by conceptMapUri and
   * conceptMapVersion.
   *
   * @param spark the spark session
   * @param tableName the name of the ancestors table
   * @param location the location to store the table, or null to create a Hive-managed table.
   * @throws IllegalArgumentException if the table name or location are malformed.
   */
  private static void createAncestorsTable(SparkSession spark,
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
    builder.append("(descendantSystem STRING, "
        + "descendantValue STRING, "
        + "ancestorSystem STRING, "
        + "ancestorValue STRING)\n"
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
        database + "." + CONCEPT_MAP_TABLE,
        database + "." + ANCESTOR_TABLE);
  }

  /**
   * Returns the concept maps that in our local concept maps, but not
   * in the given table.
   */
  private Dataset<UrlAndVersion> getMissingConceptMaps(String conceptMapTable) {

    Dataset<Row> mapsInDatabase = spark.sql("select url, version from " + conceptMapTable)
        .alias("in_db");

    Dataset<UrlAndVersion> localConcepts = getUrlAndVersions(conceptMaps).alias("local");

    return localConcepts.join(mapsInDatabase,
        functions.col("in_db.url")
            .equalTo(functions.col("local.url")).and(
            functions.col("in_db.version")
                .equalTo(functions.col("local.version"))),
        "leftanti")
        .as(URL_AND_VERSION_ENCODER);
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
   * Writes ancestor records to a table. This class ensures the columns and partitions are mapped
   * properly, and is a workaround similar to the problem described <a
   * href="http://stackoverflow.com/questions/35313077/pyspark-order-of-column-on-write-to-mysql-with-jdbc">here</a>.
   *
   * @param ancestors a dataset of ancestor records
   * @param tableName the table to write them to
   */
  private static void writeAncestorsToTable(Dataset<Ancestor> ancestors,
      String tableName) {

    // Note the last two columns here must be the partitioned-by columns
    // in order and in lower case for Spark to properly match
    // them to the partitions.
    Dataset<Row> orderedColumnDataset =
        ancestors.select("descendantSystem",
            "descendantValue",
            "ancestorSystem",
            "ancestorValue",
            "conceptmapuri",
            "conceptmapversion");

    orderedColumnDataset
        .write()
        .insertInto(tableName);
  }

  /**
   * Write a dataset to a temporary location and reloads it into the return value.
   *
   * <p>This is to workaround Spark's laziness, which can lead to reading and writing
   * from the same place causing issues.
   */
  private <T> Dataset<T> writeAndReload(Dataset<T> dataset, String tempName) {

    dataset.write().saveAsTable(tempName);

    return spark.sql("select * from " + tempName).as(dataset.exprEnc());
  }

  /**
   * Update or insert ancestors by partition.
   */
  private void upsertAncestorsByPartition(String ancestorsTable,
      Dataset<UrlAndVersion> mapsToWrite,
      String partitionDefs) {

    // Remove the ancestors table partitions we are replacing.
    spark.sql("alter table " + ancestorsTable + " drop if exists partition " + partitionDefs);

    // Get only the ancestors to write and save them.
    Dataset<Ancestor> ancestorsToWrite = this.ancestors.join(mapsToWrite,
        functions.col("conceptmapuri")
            .equalTo(functions.col("url")).and(
            functions.col("conceptmapversion")
                .equalTo(functions.col("version"))),
        "leftsemi")
        .as(ANCESTOR_ENCODER);

    String tempAncestorsTable = "TEMP_ANCESTORS_TABLE_REMOVEME";
    Dataset<Ancestor> tempAncestors = writeAndReload(ancestorsToWrite, tempAncestorsTable);

    // Write the mappings, appending so we don't affect others.
    writeAncestorsToTable(tempAncestors, ancestorsTable);

    // Clean up our temporary table since the mappings write operation has finished.
    spark.sql("drop table " + tempAncestorsTable);
  }

  /**
   * Update or insert mappings by partition.
   */
  private void upsertMappingsByPartition(String mappingsTable,
      Dataset<UrlAndVersion> mapsToWrite,
      String partitionDefs) {

    // Remove the mappings table partitions we are replacing.
    spark.sql("alter table " + mappingsTable + " drop if exists partition " + partitionDefs);

    // Get only mappings to write and save them.
    Dataset<Mapping> mappingsToWrite = this.mappings.join(mapsToWrite,
        functions.col("conceptmapuri")
            .equalTo(functions.col("url")).and(
            functions.col("conceptmapversion")
                .equalTo(functions.col("version"))),
        "leftsemi")
        .as(MAPPING_ENCODER);

    // Create a temporary table of mappings to write. This must be done before we
    // remove the partitions, since Spark's lazy execution will remove the data we
    // are reading and trying to update as well.
    String tempMappingsTable = "TEMP_MAPPINGS_TABLE_REMOVEME";
    Dataset<Mapping> tempMappings = writeAndReload(mappingsToWrite, tempMappingsTable);

    // Write the mappings, appending so we don't affect others.
    writeMappingsToTable(tempMappings, mappingsTable);

    // Clean up our temporary table since the mappings write operation has finished.
    spark.sql("drop table " + tempMappingsTable);
  }

  /**
   * Update or insert concept maps.
   */
  private void upsertConceptMaps(String conceptMapTable,
      Dataset<UrlAndVersion> mapsToWrite) {

    // Get existing maps that didn't change...
    Dataset<ConceptMap> existingUnchangedMaps = spark.sql(
        "select * from " + conceptMapTable)
        .alias("maps")
        .join(mapsToWrite.alias("to_write"),
            functions.col("maps.url")
                .equalTo(functions.col("to_write.url"))
                .and(functions.col("maps.version")
                    .equalTo(functions.col("to_write.version"))),
            "leftanti")
        .as(CONCEPT_MAP_ENCODER);

    // ... and our local maps that did change...
    Dataset<ConceptMap> changedMaps = conceptMaps
        .alias("maps")
        .join(mapsToWrite.alias("to_write"),
            functions.col("maps.url")
                .equalTo(functions.col("to_write.url"))
                .and(functions.col("maps.version")
                    .equalTo(functions.col("to_write.version"))),
            "leftsemi")
        .as(CONCEPT_MAP_ENCODER);

    String tempMapsTableName = "TEMP_MAPS_TABLE_REMOVEME";

    // Union the items we need to write with existing, unchanged content, and write it.
    Dataset<ConceptMap> unioned = writeAndReload(changedMaps.unionAll(existingUnchangedMaps),
        tempMapsTableName);

    unioned.write()
        .mode(SaveMode.Overwrite)
        .saveAsTable(conceptMapTable);

    spark.sql("drop table " + tempMapsTableName);
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
   * @param ancestorsTable name of the table containing transitive ancestors
   */
  public void writeToTables(String mappingsTable,
      String conceptMapTable,
      String ancestorsTable) {

    boolean hasExistingMaps;

    try {

      spark.sql("describe table " + conceptMapTable);

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

    if (hasExistingMaps) {

      // Build the collection of concept maps we need to write, which can be:
      // 1. All experimental maps in the local session
      // 2. Maps that exist locally but not in the target database

      // Concept maps not in the target
      Dataset<UrlAndVersion> existsLocally = getMissingConceptMaps(conceptMapTable);

      // Concept maps marked as experimental
      Dataset<UrlAndVersion> experimental = getUrlAndVersions(
          conceptMaps.filter(functions.col("experimental")
              .equalTo(functions.lit(true))));

      // Create a union to determine what to write.
      Dataset<UrlAndVersion> mapsToWrite = existsLocally
          .union(experimental)
          .distinct();

      // Get the mappings and ancestors partitions to drop
      // as we are replacing them with new content.
      String partitionDefs = mapsToWrite.collectAsList().stream()
          .map(changedMap ->
              new StringBuilder()
                  .append("(conceptmapuri=\"")
                  .append(changedMap.url)
                  .append("\", conceptmapversion=\"")
                  .append(changedMap.version)
                  .append("\")").toString())
          .collect(Collectors.joining(", "));

      // Write mappings that have been changed.
      upsertMappingsByPartition(mappingsTable, mapsToWrite, partitionDefs);

      // Write ancestors that have been changed.
      upsertAncestorsByPartition(ancestorsTable, mapsToWrite, partitionDefs);

      // Write the FHIR ConceptMaps themselves.
      upsertConceptMaps(conceptMapTable, mapsToWrite);

    } else {

      // No target tables exist, so create and write them. The mappings
      // and ancestors tables are created explicitly to meet our
      // partitioning system.
      createMappingTable(spark, mappingsTable, null);
      writeMappingsToTable(mappings, mappingsTable);

      createAncestorsTable(spark, ancestorsTable, null);
      writeAncestorsToTable(ancestors, ancestorsTable);

      // The concept maps table itself is not partitioned, so simply save it.
      conceptMaps.write().saveAsTable(conceptMapTable);
    }
  }
}
