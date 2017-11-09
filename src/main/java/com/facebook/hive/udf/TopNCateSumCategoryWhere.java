package com.facebook.hive.udf;

/**
 * Created by dash wang on 12/19/16.
 */

import com._4paradigm.hiveudafs.lib.MapUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compute the top N SUM of row items for which a condition is true;
 * "true" means "not false and also not null."
 * <p>
 * for example TOP_N_KEY_CATE_SUM_WHERE(col_x, thing='blah', col_category, n)
 */
public final class TopNCateSumCategoryWhere extends UDAF {

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
                double newSum = entry.getValue();
                if (state.containsKey(entry.getKey()) && 0 < state.get(entry.getKey()))
                    newSum += state.get(entry.getKey());
                state.put(entry.getKey(), newSum);
            }
            return true;
        }

        // reduce: find top n k sorted by v
        /*
        public List<String> terminate() {
            return MapUtil.sortByValueDescAndGetTopNKey(state, n);
        }
        */

        public String terminate() {
            List<String> sorted_list = MapUtil.sortByValueDescAndGetTopNKey(state, n);
            String result = "";
            for (String item : sorted_list) {
                result += String.format(";%s", item);
            }
            return StringUtils.stripStart(result, ";");
        }

    }
}
