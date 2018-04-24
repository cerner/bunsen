package com.cerner.bunsen;

import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.codes.UrlAndVersion;
import com.cerner.bunsen.codes.Value;
import com.cerner.bunsen.codes.base.AbstractValueSets;
import java.util.Collections;
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
        Collections.singletonList(new UrlAndVersion(
            "urn:cerner:bunsen:valueset:married_maritalstatus",
            "0.0.1")),
        AbstractValueSets.getUrlAndVersionEncoder());

    Dataset<ValueSet> valueSet = spark.createDataset(
        Collections.singletonList(new ValueSet()
            .setUrl("urn:cerner:bunsen:valueset:married_maritalstatus")
            .setVersion("0.0.1")),
        encoders.of(ValueSet.class))
        .withColumn("timestamp", lit("20180101120000").cast("timestamp"))
        .as(encoders.of(ValueSet.class));

    Dataset<Value> values = spark.createDataset(
        Collections.singletonList(new Value(
            "urn:cerner:bunsen:valueset:married_maritalstatus",
            "0.0.1",
            "http://hl7.org/fhir/v3/MaritalStatus",
            "2016-11-11",
            "M")),
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
