package com.cerner.bunsen.stu3.codes;

import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirVersionEnum;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.SparkRowConverter;
import com.cerner.bunsen.spark.codes.UrlAndVersion;
import com.cerner.bunsen.spark.codes.Value;
import com.cerner.bunsen.spark.codes.base.AbstractValueSets;
import com.cerner.bunsen.spark.converters.HasSerializableConverter;

import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.hl7.fhir.dstu3.model.ValueSet;
import org.hl7.fhir.dstu3.model.ValueSet.ConceptReferenceComponent;
import org.hl7.fhir.dstu3.model.ValueSet.ConceptSetComponent;
import org.hl7.fhir.dstu3.model.ValueSet.ValueSetComposeComponent;

/**
 * An immutable collection of FHIR ValueSets. This class is used to import value set content,
 * explore it, and persist it to a database.
 */
public class ValueSets extends AbstractValueSets<ValueSet, ValueSets> {


  private static final SparkRowConverter valuesetRowConverter =
      SparkRowConverter.forResource(FhirContexts.forStu3(), "ValueSet");


  private ValueSets(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<Row> valueSets,
      Dataset<Value> values) {

    super(spark, FhirVersionEnum.DSTU3, members, valueSets,values, valuesetRowConverter);
  }

  /**
   * Returns the collection of value sets from the default database and tables.
   *
   * @param spark the spark session
   * @return a ValueSets instance.
   */
  public static ValueSets getDefault(SparkSession spark) {

    return getFromDatabase(spark, VALUE_SETS_DATABASE);
  }

  /**
   * Returns the collection of value sets from the tables in the given database.
   *
   * @param spark the spark session
   * @param databaseName name of the database containing the value sets and values tables
   * @return a ValueSets instance.
   */
  public static ValueSets getFromDatabase(SparkSession spark, String databaseName) {

    Dataset<Value> values = spark.table(databaseName + "." + VALUES_TABLE).as(getValueEncoder());

    Dataset<Row> valueSets = spark.table(databaseName + "." + VALUE_SETS_TABLE);

    Dataset<UrlAndVersion> members = valueSets.select("url", "version").as(URL_AND_VERSION_ENCODER);

    return new ValueSets(spark,
        members,
        valueSets,
        values);
  }

  /**
   * Returns an empty ValueSets instance.
   *
   * @param spark the spark session
   * @return an empty ValueSets instance.
   */
  public static ValueSets getEmpty(SparkSession spark) {

    Dataset<Row> emptyValueSets = valuesetRowConverter.emptyDataFrame(spark)
        .withColumn("timestamp", lit(null).cast("timestamp"));

    return new ValueSets(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        emptyValueSets,
        spark.emptyDataset(getValueEncoder()));
  }

  private static class RemoveConcepts extends HasSerializableConverter
      implements Function<Row,Row> {

    RemoveConcepts(FhirVersionEnum fhirVersion) {

      super("ValueSet", fhirVersion);
    }

    @Override
    public Row call(Row valueSetRow) throws Exception {

      ValueSet valueSet = (ValueSet) converter.rowToResource(valueSetRow);

      ValueSet valueSetWithoutConcepts = valueSet.copy();

      List<ConceptSetComponent> updatedInclusions = new ArrayList<>();

      for (ConceptSetComponent inclusion: valueSet.getCompose().getInclude()) {

        ConceptSetComponent inclusionWithoutConcepts = inclusion.copy();

        inclusionWithoutConcepts.setConcept(new ArrayList<>());
        updatedInclusions.add(inclusionWithoutConcepts);
      }

      valueSetWithoutConcepts.getCompose().setInclude(updatedInclusions);

      return converter.resourceToRow(valueSetWithoutConcepts);
    }
  }

  static class ExtractValues extends HasSerializableConverter
      implements FlatMapFunction<Row,Value> {

    ExtractValues(FhirVersionEnum fhirVersion) {

      super("ValueSet", fhirVersion);
    }

    @Override
    public Iterator<Value> call(Row valueSetRow) throws Exception {
      ValueSet valueSet = (ValueSet) converter.rowToResource(valueSetRow);

      return expandValuesIterator(valueSet);
    }
  }

  /**
   * Returns a new ValueSets instance that includes the given value sets.
   *
   * @param valueSets the value sets to add to the returned collection.
   * @return a new ValueSets instance with the added value sets.
   */
  @Override
  public ValueSets withValueSets(Dataset<Row> valueSets) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(valueSets);

    // Ensure that there are no duplicates among the value sets
    if (hasDuplicateUrlAndVersions(newMembers) || valueSets.count() != newMembers.count()) {

      throw new IllegalArgumentException(
          "Cannot add value sets having duplicate valueSetUri and valueSetVersion");
    }

    JavaRDD<Row> valueSetsRdd = valueSets.javaRDD();

    // The value set concepts will be stored in the values table for persistence, so we remove
    // them from the individual value sets. This can be done most easily by setting concepts to an
    // empty list.
    JavaRDD<Row> withoutConceptsRdd = valueSetsRdd.map(new RemoveConcepts(fhirVersion));

    Dataset<Row> withoutConcepts = spark.createDataFrame(withoutConceptsRdd,
        valueSetRowConverter.getSchema());

    JavaRDD<Value> newValuesRdd = valueSetsRdd.flatMap(new ExtractValues(fhirVersion));

    Dataset<Value> newValues = spark.createDataset(newValuesRdd.rdd(), getValueEncoder());

    return withValueSets(withoutConcepts, newValues);
  }

  private ValueSets withValueSets(Dataset<Row> newValueSets, Dataset<Value> newValues) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(newValueSets);

    // Instantiating a new composite ConceptMaps requires a new timestamp
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    Dataset<Row> newValueSetsWithTimestamp = newValueSets
        .withColumn("timestamp", lit(timestamp.toString()).cast("timestamp"));


    return new ValueSets(spark,
        this.members.union(newMembers),
        this.valueSets.union(newValueSetsWithTimestamp),
        this.values.union(newValues));
  }

  /**
   * Given a value set, returns a list of value records it contains.
   *
   * @param valueSet a value set
   * @return a list of Value records.
   */
  public static List<Value> expandValues(ValueSet valueSet) {

    List<Value> values = new ArrayList<>();

    expandValuesIterator(valueSet).forEachRemaining(values::add);

    return values;
  }

  private static Iterator<Value> expandValuesIterator(ValueSet valueSet) {

    List<Value> values = new ArrayList<>();

    ValueSetComposeComponent compose = valueSet.getCompose();

    for (ConceptSetComponent inclusion: compose.getInclude()) {

      for (ConceptReferenceComponent concept: inclusion.getConcept()) {

        Value value = new Value();

        value.setValueSetUri(valueSet.getUrl());
        value.setValueSetVersion(valueSet.getVersion());

        value.setSystem(inclusion.getSystem());
        value.setVersion(inclusion.getVersion());

        value.setValue(concept.getCode());

        values.add(value);
      }
    }

    return values.iterator();
  }

  @Override
  protected void addToValueSet(ValueSet valueSet, Dataset<Value> values) {

    ValueSetComposeComponent composeComponent = valueSet.getCompose();
    ConceptSetComponent currentInclusion = null;
    ConceptReferenceComponent concept = null;

    List<Value> sortedValues = values.sort("system", "version", "value").collectAsList();

    // Workaround for the decoder producing an immutable array by replacing it with a mutable one
    composeComponent.setInclude(new ArrayList<>(composeComponent.getInclude()));
    for (Value value: sortedValues) {

      if (currentInclusion == null
          || !value.getSystem().equals(currentInclusion.getSystem())
          || !value.getVersion().equals(currentInclusion.getVersion())) {

        // Find a matching inclusion
        for (ConceptSetComponent candidate: composeComponent.getInclude()) {

          if (value.getSystem().equals(candidate.getSystem())
              && value.getVersion().equals(candidate.getVersion())) {

            currentInclusion = candidate;

            // Workaround for the decoder producing an immutable array by replacing it with a
            // mutable one
            currentInclusion.setConcept(new ArrayList<>(currentInclusion.getConcept()));
          }
        }

        // No matching inclusion found, so add one
        if (currentInclusion == null) {

          currentInclusion = composeComponent.addInclude();

          currentInclusion.setSystem(value.getSystem());
          currentInclusion.setVersion(value.getVersion());

          concept = null;
        }
      }

      // Create concept if not exists
      if (concept == null || !value.getValue().equals(concept.getCode())) {

        concept = currentInclusion.addConcept();
        concept.setCode(value.getValue());
      }
    }
  }
}
