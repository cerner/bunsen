package com.cerner.bunsen.spark;

import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.spark.codes.UrlAndVersion;
import com.cerner.bunsen.spark.codes.Value;
import com.cerner.bunsen.spark.codes.base.AbstractValueSets;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.ValueSet;

/**
 * Mock implementation of the ValueSets class for test purposes.
 */
public class MockValueSets extends AbstractValueSets<ValueSet,MockValueSets> {

  /**
   * Creates an empty MockValueSets instance for test purposes.
   */
  public MockValueSets(SparkSession spark, SparkRowConverter valuesetRowConverter) {
    super(spark,
        FhirVersionEnum.DSTU3,
        spark.emptyDataset(AbstractValueSets.getUrlAndVersionEncoder()),
        valuesetRowConverter.emptyDataFrame(spark),
        spark.emptyDataset(AbstractValueSets.getValueEncoder()),
        valuesetRowConverter);
  }

  /**
   * Creates a MockValueSets instance with the given data.
   */
  public MockValueSets(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<Row> valueSets,
      Dataset<Value> values,
      SparkRowConverter valueSetRowConverter) {

    super(spark, FhirVersionEnum.DSTU3, members, valueSets, values, valueSetRowConverter);
  }

  /**
   * Convenience method to create a MockValueSets instance with some test data.
   */
  public static MockValueSets createWithTestValue(SparkSession spark,
      SparkRowConverter valueSetRowConverter) {

    Dataset<UrlAndVersion> urlAndVersion = spark.createDataset(
        ImmutableList.of(new UrlAndVersion(
                "http://hl7.org/fhir/us/core/ValueSet/us-core-encounter-type",
                "1.1.0"),
            new UrlAndVersion(
                "http://hl7.org/fhir/ValueSet/v3-ActPriority",
                "2017-04-19")),
        AbstractValueSets.getUrlAndVersionEncoder());

    Dataset<Row> valueSet = valueSetRowConverter.toDataFrame(spark,
        ImmutableList.of(new ValueSet()
                .setUrl("http://hl7.org/fhir/us/core/ValueSet/us-core-encounter-type")
                .setVersion("1.1.0"),
            new ValueSet()
                .setUrl("http://hl7.org/fhir/ValueSet/v3-ActPriority")
                .setVersion("2017-04-19")))
        .withColumn("timestamp", lit("20180101120000").cast("timestamp"));

    Dataset<Value> values = spark.createDataset(
        ImmutableList.of(new Value(
                "http://hl7.org/fhir/us/core/ValueSet/us-core-encounter-type",
                "1.1.0",
                "http://www.ama-assn.org/go/cpt",
                "0.0.1",
                "99200"),
            new Value(
                "http://hl7.org/fhir/ValueSet/v3-ActPriority",
                "2017-04-19",
                "http://hl7.org/fhir/v3/ActPriority",
                "2017-04-19",
                "EM")),
        AbstractValueSets.getValueEncoder());

    return new MockValueSets(spark,
        urlAndVersion,
        valueSet,
        values,
        valueSetRowConverter);
  }

  /**
   * Convenience method to create a MockValueSets instance with missing valueset version element.
   */
  public static MockValueSets createValuesetWithMissingVersion(SparkSession spark,
      SparkRowConverter valueSetRowConverter) {

    Dataset<UrlAndVersion> urlAndVersion = spark.createDataset(
        ImmutableList.of(new UrlAndVersion(
                "urn:test:valueset:valueset",
                null)),
        AbstractValueSets.getUrlAndVersionEncoder());

    Dataset<Row> valueSet = valueSetRowConverter.toDataFrame(spark,
        ImmutableList.of(new ValueSet()
                .setUrl("urn:test:valueset:valueset")))
        .withColumn("timestamp", lit("20180101120000").cast("timestamp"));

    Dataset<Value> values = spark.createDataset(
        ImmutableList.of(new Value(
                "urn:test:valueset:valueset",
                "1.1.0",
                "urn:test:system",
                null,
                "99200")),
        AbstractValueSets.getValueEncoder());

    return new MockValueSets(spark,
        urlAndVersion,
        valueSet,
        values,
        valueSetRowConverter);
  }

  @Override
  protected void addToValueSet(ValueSet valueSet, Dataset<Value> values) {
    throw new UnsupportedOperationException("Not implemented in mock class.");
  }

  @Override
  public MockValueSets withValueSets(Dataset<Row> valueSets) {
    throw new UnsupportedOperationException("Not implemented in mock class.");
  }
}
