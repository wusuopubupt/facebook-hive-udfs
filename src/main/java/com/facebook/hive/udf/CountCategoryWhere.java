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
 * Compute a COUNT of row items in which a condition is true;
 * "true" means "not false and also not null."
 *
 * This sounds like it is the same as COUNT(1) with a WHERE or GROUP BY, but
 * it allows for multiple columns to be tracked separately within a single
 * aggregation.  This is faster and cleaner than
 * SUM(CAST(example = foo AS INT)), and also appropriately returns zero
 * when the item in question is NULL, unlike COUNT(1) which doesn't know what
 * is and isn't NULL.
 */
public final class CountCategoryWhere extends UDAF {

    /**
     * The actual class for doing the aggregation. Hive will automatically
     * look for all internal classes of the UDAF that implements
     * UDAFEvaluator.
     */
    public static class UDAFCountWhereEvaluator implements UDAFEvaluator {

        Map<String, Integer> state;

        public UDAFCountWhereEvaluator() {
            super();
            state = new TreeMap<>();
            init();
        }

        /**
         * Reset the state of the aggregation.
         */
        public void init() {
            state.clear();
        }

        /**
         * Iterate through one row of original data.
         *
         * The number and type of arguments need to the same as we call this UDAF
         * from Hive command line.
         *
         * This function should always return true.
         */
        public boolean iterate(String cate, Boolean ThisBool) {
            if (cate != null && ThisBool != null && ThisBool) {
                int cnt = 0;
                if(state.containsKey(cate)) {
                    cnt = state.get(cate) + 1;
                } else {
                    cnt = 1;
                }
                state.put(cate, cnt);
            }
            return true;
        }

        /**
         * Terminate a partial aggregation and return the state. If the state is a
         * primitive, just return primitive Java classes like Integer or String.
         */
        public Map<String, Integer> terminatePartial() {
            return state;
        }

        /**
         * Merge with a partial aggregation.
         *
         * This function should always have a single argument which has the same
         * type as the return value of terminatePartial().
         */
        public boolean merge(Map<String, Integer> o) {
            if(o != null) {
                for (Map.Entry<String, Integer> entry : o.entrySet()) {
                    String cate = entry.getKey();
                    int cnt = o.get(cate);
                    if (state.containsKey(cate)) {
                        cnt += state.get(cate);
                    }
                    state.put(cate, cnt);
                }
            }
            return true;
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        public String terminate() {
            String result = "";
            for(Map.Entry<String, Integer> entry: state.entrySet()) {
                result += String.format(";%s:%d", entry.getKey(), entry.getValue());
            }
            return StringUtils.stripStart(result, ";");
        }
    }
}
