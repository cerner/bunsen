package com.cerner.bunsen.mappings.systems;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import com.cerner.bunsen.mappings.ConceptMaps;
import com.cerner.bunsen.mappings.Mapping;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.ConceptMap;

/**
 * Support for SNOMED CT.
 */
public class Snomed {

  /**
   * Concept map URI used for SNOMED is-a relationships.
   */
  public static final String SNOMED_HIERARCHY_MAPPING_URI =
      "uri:cerner:bunsen:mapping:snomed-hierarchy";

  /**
   * Value set URI for all of SNOMED. See the
   * <a href="https://www.hl7.org/fhir/snomedct.html">FHIR SNOMED documentation</a>.
   */
  public static final String SNOMED_CODES_VALUESET_URI = "http://snomed.info/sct?fhir_vs";

  /**
   * SNOMED code system URI.
   */
  public static final String SNOMED_CODE_SYSTEM_URI = "http://snomed.info/sct";

  /**
   * The relationship ID used to identify "is a" relationships in SNOMED.
   */
  private static final String SNOMED_ISA_RELATIONSHIP_ID = "116680003";

  /**
   * Reads a Snomed relationship file
   *
   * @param spark the Spark session
   * @param snomedRelationshipPath path to the SNOMED relationship file
   * @param snomedVersion the version of the SNOMED CT being read.
   * @return a dataset of mappings representing the hierarchical relationship.
   */
  public static Dataset<Mapping> readRelationshipFile(SparkSession spark,
      String snomedRelationshipPath,
      String snomedVersion) {

    return spark.read()
        .option("header", true)
        .option("delimiter", "\t")
        .csv(snomedRelationshipPath)
        .where(col("typeId").equalTo(lit(SNOMED_ISA_RELATIONSHIP_ID)))
        .where(col("active").equalTo(lit("1")))
        .select(col("sourceId"), col("destinationId"))
        .where(col("sourceId").isNotNull()
            .and(col("sourceId").notEqual(lit(""))))
        .where(col("destinationId").isNotNull()
            .and(col("destinationId").notEqual(lit(""))))
        .map((MapFunction<Row, Mapping>) row -> {

          Mapping mapping = new Mapping();

          mapping.setConceptMapUri(SNOMED_HIERARCHY_MAPPING_URI);
          mapping.setConceptMapVersion(snomedVersion);
          mapping.setSourceValueSet(SNOMED_CODES_VALUESET_URI);
          mapping.setTargetValueSet(SNOMED_CODES_VALUESET_URI);
          mapping.setSourceSystem(SNOMED_CODE_SYSTEM_URI);
          mapping.setSourceValue(row.getString(0));
          mapping.setTargetSystem(SNOMED_CODE_SYSTEM_URI);
          mapping.setTargetValue(row.getString(1));
          mapping.setEquivalence(Mapping.SUBSUMES);

          return mapping;
        }, ConceptMaps.getMappingEncoder());
  }

  /**
   * Returns a ConceptMaps instance with the specified multiaxial hierarchy. This method
   * reads the LOINC multiaxial hierarchy file and convert it to a mapping dataset, and
   * adds it to the given concept maps.
   *
   * @param spark the Spark session
   * @param maps a ConceptMaps instance to which the hierarchy will be added
   * @param snomedRelationshipPath path to the relationship CSV
   * @param snomedVersion the version of SNOMED being read.
   * @return a ConceptMaps instance that includes the read hierarchy.
   */
  public static ConceptMaps withRelationships(SparkSession spark,
      ConceptMaps maps,
      String snomedRelationshipPath,
      String snomedVersion) {

    ConceptMap conceptMap = new ConceptMap();

    conceptMap.setUrl(SNOMED_HIERARCHY_MAPPING_URI);
    conceptMap.setVersion(snomedVersion);
    conceptMap.setExperimental(false);

    Dataset<Mapping> mappings = readRelationshipFile(spark,
        snomedRelationshipPath, snomedVersion);

    return maps.withExpandedMap(conceptMap, mappings);
  }
}
