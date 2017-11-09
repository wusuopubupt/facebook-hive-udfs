package com.facebook.hive.udf;

/**
 * Created by dash wang on 12/19/16.
 */

import com._4paradigm.hiveudafs.lib.MapUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;
import java.util.Map;

/**
 * Compute the top N SUM of row items for which a condition is true;
 * "true" means "not false and also not null."
 * <p>
 * for example TOP_N_CATE_SUM_WHERE(col_x, thing='blah', col_category, n)
 */
public final class TopNSumCategoryWhere extends UDAF {

    public static class UDAFCountWhereEvaluator implements UDAFEvaluator {

        // top n
        int n;

        boolean initialized = false;

        // does not have category
        Double s;

        // has category
        Map<String, Double> state;

        public UDAFCountWhereEvaluator() {
            super();
            state = new HashMap<>();
            init();
        }

        public void init() {
            state.clear();
        }

        public boolean iterate(Double item, Boolean ThisBool, String category, int n) {
            if (!initialized) {
                this.n = n;
                initialized = true;
            }

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
                double sum = entry.getValue();
                if (state.containsKey(entry.getKey()) && 0 < state.get(entry.getKey()))
                    sum += state.get(entry.getKey());
                state.put(entry.getKey(), sum);
            }
            return true;
        }

        // reduce: find top n <k,v> sorted by v
        /*
        public Map<String, Double> terminate() {
            return MapUtil.sortByValueDescAndGetTopN(state, n);
        }
        */

        public String terminate() {
            Map<String, Double> m = MapUtil.sortByValueDescAndGetTopN(state, n);
            String result = "";
            for (Map.Entry<String, Double> entry : m.entrySet()) {
                result += String.format(";%s:%f", entry.getKey(), entry.getValue());
            }
            return StringUtils.stripStart(result, ";");
        }

    }
}
