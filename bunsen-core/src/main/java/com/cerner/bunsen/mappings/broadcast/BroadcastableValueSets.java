package com.cerner.bunsen.mappings.broadcast;

import static org.apache.spark.sql.functions.col;

import com.cerner.bunsen.mappings.Ancestor;
import com.cerner.bunsen.mappings.ConceptMaps;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * An immutable collection of value sets that can be broadcast for use in Spark
 * transformations or user-defined functions.
 */
public class BroadcastableValueSets implements Serializable {

  /**
   * Spark encoder for ancestor values.
   */
  private static Encoder<AncestorValue> ANCESTOR_VALUE_ENCODER =
      Encoders.bean(AncestorValue.class);

  /**
   * Map from concept reference to code system to a set of values that
   * are contained in that code system.
   */
  private final Map<String,Map<String,Set<String>>> values;

  /**
   * Private constructor for use by the builder.
   */
  private BroadcastableValueSets(Map<String,Map<String,Set<String>>> values) {

    this.values = values;
  }

  /**
   * Bean-style class to represent ancestor values locally and as Spark datasets.
   */
  public static class AncestorValue {

    private String referenceName;

    private String conceptMapUri;

    private String conceptMapVersion;

    private String ancestorSystem;

    private String ancestorValue;

    /**
     * Nullary constructor so Spark can encode this as a bean.
     */
    public AncestorValue() {
    }

    /**
     * Constructs an ancestor value bean.
     *
     * @param referenceName the reference name to be used in user code.
     * @param conceptMapUri the URI of the concept map defining this relationship
     * @param conceptMapVersion the version of the concept map defining this relationship
     * @param ancestorSystem the code system of the ancestor
     * @param ancestorValue the code value of the ancestor
     */
    public AncestorValue(String referenceName,
        String conceptMapUri,
        String conceptMapVersion,
        String ancestorSystem,
        String ancestorValue) {

      this.referenceName = referenceName;
      this.conceptMapUri = conceptMapUri;
      this.conceptMapVersion = conceptMapVersion;
      this.ancestorSystem = ancestorSystem;
      this.ancestorValue = ancestorValue;
    }

    /**
     * Returns the value set reference name.
     *
     * @return the value set reference name
     */
    public String getReferenceName() {
      return referenceName;
    }

    /**
     * Sets the value set reference name.
     *
     * @param referenceName the value set reference name
     */
    public void setReferenceName(String referenceName) {
      this.referenceName = referenceName;
    }

    /**
     * Returns the URI of the concept map that defines this ancestor value.
     *
     * @return the URI of the concept map
     */
    public String getConceptMapUri() {
      return conceptMapUri;
    }

    /**
     * Sets the URI of the concept map that defines this ancestor value.
     *
     * @param conceptMapUri the URI of the concept map
     */
    public void setConceptMapUri(String conceptMapUri) {
      this.conceptMapUri = conceptMapUri;
    }

    /**
     * Returns the version of the concept map that defines this ancestor value.
     *
     * @return the version of the concept map.
     */
    public String getConceptMapVersion() {
      return conceptMapVersion;
    }

    /**
     * Sets the version of the concept map that defines this ancestor value.
     *
     * @param conceptMapVersion the version of the concept map.
     */
    public void setConceptMapVersion(String conceptMapVersion) {
      this.conceptMapVersion = conceptMapVersion;
    }

    /**
     * Returns the system of the ancestor code.
     *
     * @return the system of the ancestor code
     */
    public String getAncestorSystem() {
      return ancestorSystem;
    }

    /**
     * Sets the system of the ancestor code.
     *
     * @param ancestorSystem the system of the ancestor code.
     */
    public void setAncestorSystem(String ancestorSystem) {
      this.ancestorSystem = ancestorSystem;
    }

    /**
     * Returns the value of the ancestor code.
     *
     * @return the value of the ancestor code.
     */
    public String getAncestorValue() {
      return ancestorValue;
    }

    /**
     * Sets the value of the ancestor code.
     *
     * @param ancestorValue the value of the ancestor code.
     */
    public void setAncestorValue(String ancestorValue) {
      this.ancestorValue = ancestorValue;
    }

    @Override
    public boolean equals(Object obj) {

      if (!(obj instanceof AncestorValue)) {
        return false;
      }

      AncestorValue that = (AncestorValue) obj;

      return Objects.equals(this.referenceName, that.referenceName)
          && Objects.equals(this.conceptMapUri, that.conceptMapUri)
          && Objects.equals(this.conceptMapVersion, that.conceptMapVersion)
          && Objects.equals(this.ancestorSystem, that.ancestorSystem)
          && Objects.equals(this.ancestorValue, that.ancestorValue);
    }

    @Override
    public int hashCode() {
      return 37
          * Objects.hashCode(this.referenceName)
          * Objects.hashCode(this.conceptMapUri)
          * Objects.hashCode(this.conceptMapVersion)
          * Objects.hashCode(this.ancestorSystem)
          * Objects.hashCode(this.ancestorValue);
    }
  }

  public static class Builder {

    /**
     * Map from concept reference to code system to a set of values that
     * are contained in that code system.
     */
    private Map<String,Map<String,Set<String>>> values = new HashMap<>();

    /**
     * List of ancestor values to be used when creating a broadcastable value set.
     */
    private List<AncestorValue> ancestorValues = new ArrayList<>();

    /**
     * Adds a code under the given reference name to the value set.
     *
     * @param referenceName the referece name of the value set
     * @param system the code system to add
     * @param code the code value to add
     * @return this builder
     */
    public Builder addCode(String referenceName, String system, String code) {

      if (values == null) {
        throw new IllegalStateException("The builder cannot be used after "
            + "the concept map has been built.");
      }

      Map<String,Set<String>> systemToCodes = values.get(referenceName);

      if (systemToCodes == null) {

        systemToCodes = new HashMap<>();

        values.put(referenceName, systemToCodes);
      }

      Set<String> codeSet = systemToCodes.get(system);

      if (codeSet == null) {

        codeSet = new HashSet<>();

        systemToCodes.put(system, codeSet);
      }

      codeSet.add(code);

      return this;
    }

    /**
     * Add "descendants" of a given code value, that is code values
     * that are transitively subsumed by the given value in the given concept map.
     *
     * <p>This function creates a collection of ancestors to query, and the descendants
     * are actually retrieved with the broadcastable value set is built.
     *
     * @param referenceName the valueset reference name to be used in user code.
     * @param ancestorSystem the ancestor system of descendants to include
     * @param ancestorValue the ancestor value of descendants to include
     * @param conceptMapUri the concept map URI that defines the descendants
     * @param conceptMapVersion the concept map version that defines the descendants
     * @return this builder
     */
    public Builder addDescendantsOf(String referenceName,
        String ancestorSystem,
        String ancestorValue,
        String conceptMapUri,
        String conceptMapVersion) {

      ancestorValues.add(new AncestorValue(referenceName,
          conceptMapUri,
          conceptMapVersion,
          ancestorSystem,
          ancestorValue));

      return this;
    }

    /**
     * Add "descendants" of a given code value, that is code values
     * that are transitively subsumed by the given value in the given concept map. This
     * uses the latest version of the provided concept map.
     *
     * <p>This function creates a collection of ancestors to query, and the descendants
     * are actually retrieved with the broadcastable value set is built.
     *
     * @param referenceName the valueset reference name to be used in user code.
     * @param ancestorSystem the ancestor system of descendants to include
     * @param ancestorValue the ancestor value of descendants to include
     * @param conceptMapUri the concept map URI that defines the descendants
     * @return this builder
     */
    public Builder addDescendantsOf(String referenceName,
        String ancestorSystem,
        String ancestorValue,
        String conceptMapUri) {

      return addDescendantsOf(referenceName,
          ancestorSystem,
          ancestorValue,
          conceptMapUri,
          null);
    }

    /**
     * Adds the version information to any unversioned ancestors.
     *
     * @param maps concept maps to use.
     */
    private void addAncestorVersions(ConceptMaps maps) {

      // Identify the maps without versions and load the latest version.
      Set<String> latestMaps = ancestorValues.stream()
          .filter(ancestor -> ancestor.getConceptMapVersion() == null)
          .map(AncestorValue::getConceptMapUri)
          .collect(Collectors.toSet());

      final Map<String,String> versions = maps.getLatestVersions(latestMaps, false);

      // Sets the version in ancestors that were not specified.
      for (AncestorValue ancestor: ancestorValues) {

        if (ancestor.getConceptMapVersion() == null) {

          String version = versions.get(ancestor.getConceptMapUri());

          ancestor.setConceptMapVersion(version);
        }
      }
    }

    /**
     * Returns broadcastable value sets using the content added to this builder, using
     * the default concept maps to load the needed reference data.
     *
     * @param spark a spark session used to load reference data
     * @return broadcastable value sets
     */
    public BroadcastableValueSets build(SparkSession spark) {

      return build(spark, ConceptMaps.getDefault(spark));
    }

    /**
     * Returns broadcastable value sets using the content added to this builder, using
     * the given concept maps to load the needed reference data.
     *
     * @param spark a spark session used to load reference data
     * @param maps a {@link ConceptMaps} instance defining the reference data to load
     * @return broadcastable value sets
     */
    public BroadcastableValueSets build(SparkSession spark, ConceptMaps maps) {

      // Add the pending descendants to the values.
      if (ancestorValues.size() > 0) {

        // Ensure all ancestors have a version, using the latest for those that don't.
        addAncestorVersions(maps);

        Dataset<AncestorValue> ancestorsToLoad = spark.createDataset(ancestorValues,
            ANCESTOR_VALUE_ENCODER)
            .as("toload");

        Dataset<Ancestor> ancestors = maps.getAncestors().as("a");

        List<Row> descendants = ancestors.join(ancestorsToLoad,
            col("a.conceptMapUri").equalTo(col("toload.conceptMapUri"))
                .and(col("a.conceptMapVersion").equalTo(col("toload.conceptMapVersion")))
                .and(col("a.ancestorSystem").equalTo(col("toload.ancestorSystem")))
                .and(col("a.ancestorValue").equalTo(col("toload.ancestorValue"))))
            .select("referenceName", "descendantSystem", "descendantValue")
            .collectAsList();

        // Add a code for each descendant.
        for (Row descendant: descendants) {

          addCode(descendant.getString(0),
              descendant.getString(1),
              descendant.getString(2));
        }

        // The parent value itself is also included in the value set, so add it as well.
        for (AncestorValue value: ancestorValues) {

          addCode(value.getReferenceName(),
              value.getAncestorSystem(),
              value.getAncestorValue());
        }
      }

      // Nullify the builder values so they cannot be further mutated.
      Map<String,Map<String,Set<String>>> values = this.values;

      this.values = null;
      this.ancestorValues = null;

      return new BroadcastableValueSets(values);
    }
  }

  /**
   * Returns a {@link BroadcastableValueSets} builder.
   *
   * @return a {@link BroadcastableValueSets} builder
   */
  public static Builder newBuilder() {

    return new Builder();
  }

  /**
   * Returns true if the valueset with the given reference name includes
   * the given code value.
   *
   * @param referenceName the reference name registered for the value set
   * @param system the code system to check if it is contained
   * @param code the code value to check if it is contained
   * @return true if contained, false otherwise
   * @throws IllegalArgumentException if the referenceName is not known to this object
   */
  public boolean hasCode(String referenceName, String system, String code) {

    Map<String,Set<String>> codeSystemAndValues = values.get(referenceName);

    if (codeSystemAndValues == null) {
      throw new IllegalArgumentException("Unknown value set reference name: " + referenceName);
    }

    Set<String> values = codeSystemAndValues.get(system);

    return values == null
        ? false
        : values.contains(code);
  }


  /**
   * Returns the reference names in the value set.
   *
   * @return the set of reference names.
   */
  public Set<String> getReferenceNames() {

    return values.keySet();
  }

  /**
   * Returns a map of code systems to the set of code values within
   * those systems used by the given valueset reference.
   *
   * @param referenceName the reference name for which to retrieve values
   * @return a map of the code systems to the code values used by the reference
   */
  public Map<String,Set<String>> getValues(String referenceName) {

    // Defensively wrap the contained sets in unmodifiable collections.
    // This is done on read since this is an infrequently called operation
    // so we avoid the cost of creating an immutable set for the primary flow
    // where this is not called.
    return values.get(referenceName)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            entry -> Collections.unmodifiableSet(entry.getValue())));
  }
}
