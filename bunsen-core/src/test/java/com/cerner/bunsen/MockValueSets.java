package com.cerner.bunsen;

import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.codes.UrlAndVersion;
import com.cerner.bunsen.codes.Value;
import com.cerner.bunsen.codes.base.AbstractValueSets;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.ValueSet;

/**
 * Mock implementation of the ValueSets class for test purposes.
 */
public class MockValueSets extends AbstractValueSets<ValueSet,MockValueSets> {

  /**
   * Creates an empty MockValueSets instance for test purposes.
   */
  public MockValueSets(SparkSession spark, FhirEncoders encoders) {
    super(spark,
        FhirVersionEnum.DSTU3,
        spark.<UrlAndVersion>emptyDataset(AbstractValueSets.getUrlAndVersionEncoder()),
        spark.<ValueSet>emptyDataset(encoders.of(ValueSet.class)),
        spark.<Value>emptyDataset(AbstractValueSets.getValueEncoder()),
        encoders.of(ValueSet.class));
  }

  /**
   * Creates a MockValueSets instance with the given data.
   */
  public MockValueSets(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<ValueSet> valueSets,
      Dataset<Value> values,
      FhirEncoders encoders) {

    super(spark, FhirVersionEnum.DSTU3, members, valueSets, values, encoders.of(ValueSet.class));
  }

  /**
   * Convenience method to create a MockValueSets instance with some test data.
   */
  public static MockValueSets createWithTestValue(SparkSession spark, FhirEncoders encoders) {

    Dataset<UrlAndVersion> urlAndVersion = spark.createDataset(
        ImmutableList.of(new UrlAndVersion(
                "http://hl7.org/fhir/us/core/ValueSet/us-core-encounter-type",
                "1.1.0"),
            new UrlAndVersion(
                "http://hl7.org/fhir/ValueSet/v3-ActPriority",
                "2017-04-19")),
        AbstractValueSets.getUrlAndVersionEncoder());

    Dataset<ValueSet> valueSet = spark.createDataset(
        ImmutableList.of(new ValueSet()
                .setUrl("http://hl7.org/fhir/us/core/ValueSet/us-core-encounter-type")
                .setVersion("1.1.0"),
            new ValueSet()
                .setUrl("http://hl7.org/fhir/ValueSet/v3-ActPriority")
                .setVersion("2017-04-19")),
        encoders.of(ValueSet.class))
        .withColumn("timestamp", lit("20180101120000").cast("timestamp"))
        .as(encoders.of(ValueSet.class));

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
        encoders);
  }

  @Override
  protected void addToValueSet(ValueSet valueSet, Dataset<Value> values) {
    throw new UnsupportedOperationException("Not implemented in mock class.");
  }

  @Override
  public MockValueSets withValueSets(Dataset<ValueSet> valueSets) {
    throw new UnsupportedOperationException("Not implemented in mock class.");
  }
}
