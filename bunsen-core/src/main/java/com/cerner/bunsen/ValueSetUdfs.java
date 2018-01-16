package com.cerner.bunsen;

import com.cerner.bunsen.codes.broadcast.BroadcastableValueSets;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.IndexedSeq;

/**
 * Support for user-defined functions that use valuesets. This class offers users a stack
 * to push and pop valuesets used in the in_valuesets UDF, allowing callers to temporarily
 * override valuesets and then simply pop them to revert to the previous state.
 */
public class ValueSetUdfs {

  /**
   * UDF implementation that checks if a given codeable concept is
   * in a named valueset reference.
   */
  static class InValuesetUdf implements UDF2<Row, String, Boolean> {

    private Broadcast<BroadcastableValueSets> broadcast;

    InValuesetUdf(Broadcast<BroadcastableValueSets> broadcast) {
      this.broadcast = broadcast;
    }

    @Override
    public Boolean call(Row row, String referenceName) throws Exception {

      // A null code or concept never matches.
      if (row == null || referenceName == null) {
        return false;
      }

      BroadcastableValueSets valuesets = broadcast.getValue();

      // Check if we are dealing with a codeable concept or directly with a coding.
      boolean isCodeable = false;

      for (String fieldName : row.schema().fieldNames()) {
        if ("coding".equals(fieldName)) {
          isCodeable = true;
          break;
        }
      }

      if (isCodeable) {

        IndexedSeq<Row> array = row.<IndexedSeq<Row>>getAs("coding");

        for (int i = 0; i < array.length(); ++i) {

          Row coding = array.apply(i);

          String sourceSystem = coding.getAs("system");
          String sourceValue = coding.getAs("code");

          if (valuesets.hasCode(referenceName, sourceSystem, sourceValue)) {
            return true;
          }
        }

        return false;
      } else {

        String sourceSystem = row.getAs("system");
        String sourceValue = row.getAs("code");

        return valuesets.hasCode(referenceName, sourceSystem, sourceValue);
      }
    }
  }

  /**
   * Stack of valuesets used in user-defined functions.
   */
  private static final Deque<Broadcast<BroadcastableValueSets>> valueSetStack = new ArrayDeque<>();

  /**
   * Pushes an "in_valueset" UDF that uses the given {@link BroadcastableValueSets} for its content.
   *
   * @param spark the spark session
   * @param valueSets the valuesets to use in the UDF
   */
  public static synchronized void pushUdf(SparkSession spark, BroadcastableValueSets valueSets) {

    JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());

    Broadcast<BroadcastableValueSets> broadcast = ctx.broadcast(valueSets);

    spark.udf()
        .register("in_valueset",
            new InValuesetUdf(broadcast),
            DataTypes.BooleanType);

    // Push the broadcast variable
    valueSetStack.push(broadcast);
  }


  /**
   * Pops a BroadcastableValueSets from the user-defined function stack.
   *
   * @param spark the spark session
   * @return true if there is still a registered in_valuset UDF, false otherwise
   */
  public static synchronized boolean popUdf(SparkSession spark) {

    if (valueSetStack.isEmpty()) {

      return false;

    } else {

      // Cleanup the previously broadcast valuesets
      Broadcast<BroadcastableValueSets> old = valueSetStack.pop();

      old.destroy();

      if (valueSetStack.isEmpty()) {

        return false;

      } else {

        // Re-apply the previous function.
        Broadcast<BroadcastableValueSets> current = valueSetStack.peek();

        spark.udf()
            .register("in_valueset",
                new InValuesetUdf(current),
                DataTypes.BooleanType);

        return true;
      }
    }
  }

  /**
   * Returns the broadcast variable of the current valuesets so they can be used
   * in other user-defined functions.
   *
   * @return the broadcast variable of the current valuesets, or null if none exists.
   */
  public static synchronized Broadcast<BroadcastableValueSets> currentValueSets() {

    return valueSetStack.isEmpty()
        ? null
        : valueSetStack.peek();
  }
}
