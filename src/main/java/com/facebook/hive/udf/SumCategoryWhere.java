package com.facebook.hive.udf;

/**
 * Created by dash wang on 12/19/16.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.Map;
import java.util.TreeMap;

/**
 * Compute the SUM of row items for which a condition is true;
 * "true" means "not false and also not null."
 * <p>
 * This sounds like it is the same as SUM(col) with a WHERE or GROUP BY, but
 * it allows for multiple columns to be tracked separately within a single
 * aggregation.  This is faster and cleaner than
 * SUM(col * CAST(example = foo AS INT)). Also very useful for computing percentages,
 * for example SUM_WHERE(col_x, thing='blah', col_category) / SUM(col_x).
 * <p>
 * If either the conditional or the item to sum is null, we return null
 * because we don't know what the sum is.  This seems extreme, and is
 * slightly inconsistent with SUM, but it can be easily escaped by adding
 * the appropriate IS NULL clause into the conditional.
 */
public final class SumCategoryWhere extends UDAF {

    public static class UDAFCountWhereEvaluator implements UDAFEvaluator {

        // does not have category
        Double s;

        // has category
        Map<String, Double> state;


        public UDAFCountWhereEvaluator() {
            super();
            state = new TreeMap<>();
            init();
        }

        public void init() {
            state.clear();
        }

        public boolean iterate(Double item, Boolean ThisBool, String category) {
            if (item != null && ThisBool != null && ThisBool && category != null) {
                double newSum = item;
                if (state.containsKey(category)) {
                    newSum += state.get(category);
                }
                state.put(category, newSum);
            }
            return true;
        }

        public Map<String, Double> terminatePartial() {
            return state;
        }

        public boolean merge(Map<String, Double> o) {
            for (Map.Entry<String, Double> entry : o.entrySet()) {
                double cnt = entry.getValue();
                if (state.containsKey(entry.getKey()) && 0 < state.get(entry.getKey()))
                    cnt += state.get(entry.getKey());
                state.put(entry.getKey(), cnt);
            }
            return true;
        }

        /*
        public Map<String, Double> terminate() {
            return state;
        }
        */

        public String terminate() {
            String result = "";
            for (Map.Entry<String, Double> entry : state.entrySet()) {
                result += String.format(";%s:%f", entry.getKey(), entry.getValue());
            }
            return StringUtils.stripStart(result, ";");
        }

    }
}
