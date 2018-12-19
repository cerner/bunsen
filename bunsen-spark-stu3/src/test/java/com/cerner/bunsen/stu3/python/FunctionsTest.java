package com.cerner.bunsen.stu3.python;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.SparkRowConverter;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Condition;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for common FHIR functions.
 */
public class FunctionsTest {

  private static final FhirContext CONTEXT = FhirContexts.forStu3();

  private static SparkSession spark;

  private static SparkRowConverter conditionConverter =
      SparkRowConverter.forResource(CONTEXT, "Condition");

  private static Condition condition;

  private static Dataset<Row> conditions;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();

    condition = new Condition();

    condition.setId("Condition/testid");

    conditions = conditionConverter.toDataFrame(spark, ImmutableList.of(condition));
  }

  /**
   * Tears down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void resourceToJson() {

    Dataset<String> jsonDs = Functions.toJson(conditions, "Condition");

    String conditionJson = jsonDs.first();

    Condition parsedCondition = (Condition) CONTEXT.newJsonParser()
        .parseResource(conditionJson);

    Assert.assertEquals(condition.getId(), parsedCondition.getId());
  }

  @Test
  public void bundleToJson() {

    String jsonBundle = Functions.toJsonBundle(conditions, "Condition");

    Bundle bundle = (Bundle) CONTEXT.newJsonParser().parseResource(jsonBundle);

    Condition parsedCondition = (Condition) bundle.getEntryFirstRep().getResource();

    Assert.assertEquals(condition.getId(), parsedCondition.getId());
  }
}
