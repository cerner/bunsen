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
 * Support for LOINC.
 */
public class Loinc {

  /**
   * Concept map URI used for the LOINC hierarchy.
   */
  public static final String LOINC_HIERARCHY_MAPPING_URI =
      "uri:cerner:bunsen:mapping:loinc-hierarchy";

  /**
   * Valueset URI used for LOINC codes. See the
   * <a href="https://www.hl7.org/fhir/loinc.html">FHIR LOINC documentation</a>.
   */
  public static final String LOINC_VALUESET_URI = "http://loinc.org/vs";

  /**
   * LOINC code system URI.
   */
  public static final String LOINC_CODE_SYSTEM_URI = "http://loinc.org";

  /**
   * Reads the LOINC multiaxial hierarchy file and convert it to a mapping dataset.
   *
   * @param spark the Spark session
   * @param loincHierarchyPath path to the multiaxial hierarchy CSV
   * @param loincVersion the version of the LOINC hierarchy being read.
   * @return a dataset of mappings representing the hierarchical relationship.
   */
  public static Dataset<Mapping> readMultiaxialHierarchyFile(SparkSession spark,
      String loincHierarchyPath,
      String loincVersion) {

    return spark.read()
        .option("header", true)
        .csv(loincHierarchyPath)
        .select(col("CODE"), col("IMMEDIATE_PARENT"))
        .where(col("CODE").isNotNull()
            .and(col("CODE").notEqual(lit(""))))
        .where(col("IMMEDIATE_PARENT").isNotNull()
            .and(col("IMMEDIATE_PARENT").notEqual(lit(""))))
        .map((MapFunction<Row, Mapping>) row -> {

          Mapping mapping = new Mapping();

          mapping.setConceptMapUri(LOINC_HIERARCHY_MAPPING_URI);
          mapping.setConceptMapVersion(loincVersion);
          mapping.setSourceValueSet(LOINC_VALUESET_URI);
          mapping.setTargetValueSet(LOINC_VALUESET_URI);
          mapping.setSourceSystem(LOINC_CODE_SYSTEM_URI);
          mapping.setSourceValue(row.getString(0));
          mapping.setTargetSystem(LOINC_CODE_SYSTEM_URI);
          mapping.setTargetValue(row.getString(1));
          mapping.setEquivalence(Mapping.SUBSUMES);

          return mapping;
        }, ConceptMaps.getMappingEncoder());
  }

  /**
   * Returns a ConceptMaps instance with the specified multiaxial hierarchy. This method
   * reads the LOINC multiaxial hierarchy file and converts it to a mapping dataset, and
   * adds it to the given concept maps.
   *
   * @param spark the Spark session
   * @param maps a ConceptMaps instance to which the hierarchy will be added
   * @param loincHierarchyPath path to the multiaxial hierarchy CSV
   * @param loincVersion the version of the LOINC hierarchy being read.
   * @return a ConceptMaps instance that includes the read hierarchy.
   */
  public static ConceptMaps withLoincHierarchy(SparkSession spark,
      ConceptMaps maps,
      String loincHierarchyPath,
      String loincVersion) {

    ConceptMap conceptMap = new ConceptMap();

    conceptMap.setUrl(LOINC_HIERARCHY_MAPPING_URI);
    conceptMap.setVersion(loincVersion);
    conceptMap.setExperimental(false);

    Dataset<Mapping> mappings = readMultiaxialHierarchyFile(spark,
        loincHierarchyPath, loincVersion);

    return maps.withExpandedMap(conceptMap, mappings);
  }
}
