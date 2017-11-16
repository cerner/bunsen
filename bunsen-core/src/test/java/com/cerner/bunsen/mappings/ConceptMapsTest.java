package com.cerner.bunsen.mappings;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.dstu3.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.exceptions.FHIRException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for storing, loading, and manipulationg ConceptMaps.
 */
public class ConceptMapsTest {

  private static SparkSession spark;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() throws IOException {

    // Create a local spark session using an in-memory metastore.
    // We must also use Hive and set the partition mode to non-strict to
    // support dynamic partitions.
    spark = SparkSession.builder()
        .master("local[2]")
        .appName("ConceptMapsTest")
        .enableHiveSupport()
        .config("javax.jdo.option.ConnectionURL",
            "jdbc:derby:memory:metastore_db;create=true")
        .config("hive.exec.dynamic.partition.mode",
            "nonstrict")
        .config("spark.sql.warehouse.dir",
            Files.createTempDirectory("spark_warehouse").toString())
        .getOrCreate();

    spark.sql("create database mappingtestdb");
  }

  /**
   * Tears down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }


  private static final ConceptMap conceptMap(String url, String version) {
    ConceptMap conceptMap = new ConceptMap();

    conceptMap.setUrl(url);
    conceptMap.setVersion(version);
    conceptMap.setExperimental(true);
    conceptMap.setSource(new UriType("urn:source:valueset"));
    conceptMap.setTarget(new UriType("urn:target:valueset"));
    conceptMap.addGroup()
        .setSource("urn:source:system")
        .setTarget("urn:target:system")
        .addElement()
        .setCode("urn:source:code:a")
        .addTarget().setCode("urn:target:code:1");

    return conceptMap;
  }

  /**
   * Returns a concept map that includes ancesters where
   * A is B is C in this test map, and D is also C.
   */
  private static final ConceptMap ancestorMap(String url, String version) {
    ConceptMap conceptMap = new ConceptMap();

    //
    conceptMap.setUrl(url);
    conceptMap.setVersion(version);
    conceptMap.setExperimental(true);
    conceptMap.setSource(new UriType("urn:test:valueset"));
    conceptMap.setTarget(new UriType("urn:test:valueset"));

    ConceptMapGroupComponent group = conceptMap.addGroup()
        .setSource("urn:test:system")
        .setTarget("urn:test:system");

    group.addElement()
        .setCode("urn:test:code:a")
        .addTarget().setCode("urn:test:code:b")
        .setEquivalence(ConceptMapEquivalence.SUBSUMES);

    group.addElement()
        .setCode("urn:test:code:b")
        .addTarget().setCode("urn:test:code:c")
        .setEquivalence(ConceptMapEquivalence.SUBSUMES);

    group.addElement()
        .setCode("urn:test:code:d")
        .addTarget().setCode("urn:test:code:c")
        .setEquivalence(ConceptMapEquivalence.SUBSUMES);

    return conceptMap;
  }

  private static void checkMap(ConceptMap map, String url, String version) throws FHIRException {

    Assert.assertNotNull("Could not find concept map + " + url, map);

    Assert.assertEquals(url, map.getUrl());
    Assert.assertEquals(version, map.getVersion());

    Assert.assertEquals("urn:source:valueset", map.getSourceUriType().asStringValue());
    Assert.assertEquals("urn:target:valueset", map.getTargetUriType().asStringValue());

    Assert.assertEquals(1, map.getGroup().size());
  }

  @Test
  public void testCreateSimpleMappings() throws FHIRException {

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"),
            conceptMap("urn:cerner:map:othermap", "1"));

    Dataset<Mapping> mappings = maps.getMappings();

    Assert.assertEquals(2, mappings.count());

    ConceptMap firstMap = maps.getConceptMap("urn:cerner:map:testmap", "1");
    checkMap(firstMap, "urn:cerner:map:testmap", "1");

    ConceptMap secondMap = maps.getConceptMap("urn:cerner:map:othermap", "1");
    checkMap(secondMap, "urn:cerner:map:othermap", "1");
  }

  @Test
  public void testMappingsAreSorted() {

    String database = "test_mappings_are_sorted";

    spark.sql("create database " + database);

    // Ensure that mappings we create are sorted
    ConceptMap testMap = new ConceptMap();

    testMap.setUrl("urn:test");
    testMap.setVersion("0.1");
    testMap.setExperimental(true);
    testMap.setSource(new UriType("urn:source:valueset"));
    testMap.setTarget(new UriType("urn:target:valueset"));

    SourceElementComponent element =  testMap.addGroup()
        .setSource("urn:test:x")
        .setTarget("urn:test:y")
        .addElement();

    element.setCode("urn:test:code:4")
        .addTarget()
        .setCode("urn:test:code:5");


    ConceptMaps.getEmpty(spark)
        .withConceptMaps(testMap)
        .writeToDatabase(database);

    ConceptMap reloaded = ConceptMaps
        .getFromDatabase(spark, database)
        .getConceptMap("urn:test", "0.1");

    assertContentIsSorted(reloaded);
  }

  /**
   * Helper function to assert the concepts are sorted within the given map.
   */
  private void assertContentIsSorted(ConceptMap map) {

    ConceptMapGroupComponent previousGroup = null;

    for (ConceptMapGroupComponent group: map.getGroup()) {

      Assert.assertTrue(previousGroup == null
          || ComparisonChain.start()
          .compare(group.getSource(), previousGroup.getSource())
          .compare(group.getTarget(), previousGroup.getTarget())
          .result() >= 0);

      previousGroup = group;

      SourceElementComponent previousElement = null;

      for (SourceElementComponent element : group.getElement()) {

        Assert.assertTrue(previousElement == null
            || element.getCode().compareTo(previousElement.getCode()) >= 0);

        previousElement = element;

        TargetElementComponent previousTarget = null;

        for (TargetElementComponent target: element.getTarget()) {

          Assert.assertTrue(previousTarget == null
              || target.getCode().compareTo(previousTarget.getCode()) >= 0);
        }
      }
    }
  }

  @Test
  public void testAppendMappings() throws FHIRException {

    ConceptMaps original = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"),
            conceptMap("urn:cerner:map:othermap", "1"));

    ConceptMaps maps = original.withConceptMaps(
        conceptMap("urn:cerner:map:newmap", "1"));

    // The original should be unchanged.
    Assert.assertEquals(2, original.getMappings().count());
    Assert.assertEquals(3,  maps.getMappings().count());

    ConceptMap firstMap = maps.getConceptMap("urn:cerner:map:testmap", "1");
    checkMap(firstMap, "urn:cerner:map:testmap", "1");

    ConceptMap secondMap = maps.getConceptMap("urn:cerner:map:othermap", "1");
    checkMap(secondMap, "urn:cerner:map:othermap", "1");

    ConceptMap newMap = maps.getConceptMap("urn:cerner:map:newmap", "1");
    checkMap(newMap, "urn:cerner:map:newmap", "1");
  }

  @Test
  public void testModifyMapping() throws FHIRException {

    ConceptMaps original = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"));

    // Modify the map to ensure the change is reflected.
    ConceptMap modifiedMap = original.getConceptMap("urn:cerner:map:testmap", "1");

    modifiedMap.getGroup()
        .get(0)
        .addElement()
        .setCode("urn:new:source:code")
        .addTarget()
        .setCode("urn:new:target:code");

    ConceptMaps modified = original.withConceptMaps(modifiedMap);

    // The new mapping should be visible in the modified but not the original.
    Assert.assertEquals(1, original.getMappings().count());
    Assert.assertEquals(2, modified.getMappings().count());

    ConceptMap reloadedOriginal = original.getConceptMap("urn:cerner:map:testmap", "1");
    ConceptMap reloadedModified = modified.getConceptMap("urn:cerner:map:testmap", "1");

    Assert.assertEquals(1, reloadedOriginal.getGroup().get(0).getElement().size());
    Assert.assertEquals(2, reloadedModified.getGroup().get(0).getElement().size());
  }

  @Test
  public void testFromDirectory() {

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withMapsFromDirectory("src/test/resources/conceptmaps");

    ConceptMap genderMap = maps.getConceptMap(
        "urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        "0.0.1");

    Assert.assertNotNull(genderMap);

    Assert.assertEquals("urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        genderMap.getUrl());

    Assert.assertEquals("0.0.1", genderMap.getVersion());
  }

  @Test
  public void testWriteToNewTables() {

    spark.sql("create database test_mapping_write");

    ConceptMap ancestorMap = ancestorMap("urn:cerner:test:write:ancestormap", "0");

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withMapsFromDirectory("src/test/resources/conceptmaps")
        .withConceptMaps(ancestorMap);

    maps.writeToDatabase("test_mapping_write");

    ConceptMaps reloadedMaps = ConceptMaps.getFromDatabase(spark, "test_mapping_write");

    ConceptMap genderMap = reloadedMaps.getConceptMap(
        "urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        "0.0.1");

    Assert.assertNotNull(genderMap);

    Assert.assertEquals("urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        genderMap.getUrl());

    Assert.assertEquals("0.0.1", genderMap.getVersion());

    Assert.assertEquals(3, genderMap.getGroup().size());

    Assert.assertEquals(4, reloadedMaps.getAncestors().count());
  }

  @Test
  public void testUpdateMap() {

    spark.sql("create database test_mapping_update");

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"))
        .writeToDatabase("test_mapping_update");

    ConceptMaps original = ConceptMaps.getFromDatabase(spark, "test_mapping_update");

    Assert.assertEquals(1, original.getMappings().count());

    // Modify the map to ensure the change is reflected.
    ConceptMap modifiedMap = original.getConceptMap("urn:cerner:map:testmap", "1");

    // The test adds codes lexigraphically later than the original so
    // it is deeply equal to the reloaded version, which sorts elements.
    modifiedMap.getGroup()
        .get(0)
        .addElement()
        .setCode("urn:source:code:new")
        .addTarget()
        .setCode("urn:target:code:new");

    original.withConceptMaps(modifiedMap)
        .writeToDatabase("test_mapping_update");

    ConceptMaps reloaded = ConceptMaps.getFromDatabase(spark, "test_mapping_update");

    // Ensure the new mapping is visible in the modified map.
    Assert.assertEquals(2, reloaded.getMappings().count());

    ConceptMap reloadedMap = reloaded.getConceptMap("urn:cerner:map:testmap", "1");

    Assert.assertTrue(reloadedMap.equalsDeep(modifiedMap));
  }

  @Test
  public void testPreserveUnchangedPartitions() {

    String database = "test_preserve_unchanged";

    spark.sql("create database " + database);

    ConceptMap original = conceptMap("urn:cerner:map:testmap", "1");

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(original,
            conceptMap("urn:cerner:map:othermap", "1"))
        .writeToDatabase(database);

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:newmap", "1"))
        .writeToDatabase(database);

    ConceptMaps loaded = ConceptMaps.getFromDatabase(spark, database);

    Assert.assertEquals(3, loaded.getMappings().count());

    ConceptMap reloaded = loaded.getConceptMap("urn:cerner:map:testmap", "1");

    Assert.assertTrue(original.equalsDeep(reloaded));
  }

  @Test
  public void testGetLatest() {

    String database = "test_get_latest";
    spark.sql("create database " + database);

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(
            conceptMap("urn:cerner:map:newmap", "1"),
            conceptMap("urn:cerner:map:newmap", "2"),
            conceptMap("urn:cerner:map:othermap", "1"))
        .writeToDatabase(database);

    Dataset<Mapping> latest = ConceptMaps.getFromDatabase(spark, database)
        .getLatestMappings(
            ImmutableSet.of("urn:cerner:map:newmap",
                "urn:cerner:map:othermap"),
            true);

    latest.cache();

    Assert.assertEquals(2, latest.count());

    Assert.assertEquals(0,
        latest.where("conceptMapUri == 'urn:cerner:map:newmap' and conceptMapVersion == '1'")
            .count());

    Assert.assertEquals(1,
        latest.where("conceptMapUri == 'urn:cerner:map:newmap' and conceptMapVersion == '2'")
            .count());

    Assert.assertEquals(1,
        latest.where("conceptMapUri == 'urn:cerner:map:othermap' and conceptMapVersion == '1'")
            .count());
  }

  @Test
  public void testGetLatestExperimental() {

    String database = "test_get_latest_experimental";
    spark.sql("create database " + database);

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(
            conceptMap("urn:cerner:map:expmap", "1").setExperimental(false),
            conceptMap("urn:cerner:map:expmap", "2"),
            conceptMap("urn:cerner:map:otherexpmap", "1"))
        .writeToDatabase(database);

    ConceptMaps conceptMaps = ConceptMaps.getFromDatabase(spark, database);

    Dataset<Mapping> latestWithExperimental = conceptMaps.getLatestMappings(
            ImmutableSet.of("urn:cerner:map:expmap"),
            true);

    // We include experimental versions, so we shoudl see that.
    Assert.assertEquals(1,
        latestWithExperimental
            .where("conceptMapUri == 'urn:cerner:map:expmap' and conceptMapVersion == '2'")
            .count());

    Dataset<Mapping> latestWithoutExperimental = conceptMaps.getLatestMappings(
            ImmutableSet.of("urn:cerner:map:expmap"),
            false);

    // Version 1 is not experimental, so we should see it.
    Assert.assertEquals(1,
        latestWithoutExperimental
            .where("conceptMapUri == 'urn:cerner:map:expmap' and conceptMapVersion == '1'")
            .count());

    // Loading a map with only experimental versions should find nothing.
    Dataset<Mapping> onlyExperimentalMaps = conceptMaps.getLatestMappings(
        ImmutableSet.of("urn:cerner:map:otherexpmap"),
        false);

    Assert.assertEquals(0, onlyExperimentalMaps.count());
  }

  @Test
  public void testAncestors() {

    ConceptMap conceptMap = ancestorMap("urn:cerner:ancestormap", "0");

    ConceptMaps withHierarchy = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap);

    Set<Ancestor> expected = ImmutableSet.of(
        new Ancestor("urn:cerner:ancestormap",
            "0",
            "urn:test:system",
            "urn:test:code:a",
            "urn:test:system",
            "urn:test:code:b"),
        new Ancestor("urn:cerner:ancestormap",
            "0",
            "urn:test:system",
            "urn:test:code:a",
            "urn:test:system",
            "urn:test:code:c"),
        new Ancestor("urn:cerner:ancestormap",
            "0",
            "urn:test:system",
            "urn:test:code:b",
            "urn:test:system",
            "urn:test:code:c"),
        new Ancestor("urn:cerner:ancestormap",
            "0",
            "urn:test:system",
            "urn:test:code:d",
            "urn:test:system",
            "urn:test:code:c"));

    // Ensure all of the expected values are present.
    List<Ancestor> actual = withHierarchy.getAncestors().collectAsList();

    Assert.assertEquals(expected.size(), actual.size());
    Assert.assertTrue(expected.containsAll(actual));
  }
}
