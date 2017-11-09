package com.facebook.hive.udf;

/**
 * Created by dash wang on 12/19/16.
 */
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Compute the SUM of row items for which a condition is true;
 * "true" means "not false and also not null."
 *
 * This sounds like it is the same as SUM(col) with a WHERE or GROUP BY, but
 * it allows for multiple columns to be tracked separately within a single
 * aggregation.  This is faster and cleaner than
 * SUM(col * CAST(example = foo AS INT)). Also very useful for computing percentages,
 * for example SUM_WHERE(col_x, thing='blah') / SUM(col_x).
 *
 * If either the conditional or the item to sum is null, we return null
 * because we don't know what the sum is.  This seems extreme, and is
 * slightly inconsistent with SUM, but it can be easily escaped by adding
 * the appropriate IS NULL clause into the conditional.
 */
public final class SumWhere extends UDAF {
    public static class UDAFCountWhereEvaluator implements UDAFEvaluator {
        Double s = null;

        public UDAFCountWhereEvaluator() {
            super();
            init();
        }
		// initialize
        public void init() {
            s = null;
        }

        // map
        public boolean iterate(Double item, Boolean ThisBool) {
            if (item != null && ThisBool != null && ThisBool) {
                if (s == null) {
                    s = item;
                } else {
                    s += item;
                }
            }
            return true;
        }

        // map output
        public Double terminatePartial() {
            return s;
        }

        // combine
        public boolean merge(Double s2) {
            if (s2 != null) {
                if (s == null) {
                    s = s2;
                } else {
                    s += s2;
                }
            }
            return true;
        }

        // reduce
        public Double terminate() {
            return s;
        }

    }
}
