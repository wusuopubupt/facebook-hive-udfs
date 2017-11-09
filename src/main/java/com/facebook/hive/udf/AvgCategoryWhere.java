package com.facebook.hive.udf;

/**
 * Created by dash wang on 12/19/16.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.Map;
import java.util.TreeMap;

/**
 * Compute the AVG of row items for which a condition is true;
 * "true" means "not false and also not null."
 * <p>
 * Usage: AVG_WHERE(col_x, thing='blah', col_cate)
 */
@Description(name = "AVG_CATE_WHERE",
        value = "_FUNC_(col, thing='blah', col_cate) - Compute the AVG of row items for which a condition is true group by col_cate")
public final class AvgCategoryWhere extends UDAF {

    public static class UDAFAvgState {
        private double mSum;
        private long mCount;
    }

    public static class UDAFAvgWhereEvaluator implements UDAFEvaluator {
        public Map<String, UDAFAvgState> statMap;

        public UDAFAvgWhereEvaluator() {
            super();
            statMap = new TreeMap<>();
            init();
        }

        public void init() {
            statMap.clear();
        }

        // map
        public boolean iterate(Double item, Boolean ThisBool, String category) {
            if (item != null && ThisBool != null && ThisBool && category != null) {
                UDAFAvgState s;
                if (statMap.containsKey(category)) {
                    s = statMap.get(category);
                } else {
                    s = new UDAFAvgState();
                }
                s.mSum += item;
                s.mCount++;

                statMap.put(category, s);
            }
            return true;
        }

        // map output
        public Map<String, UDAFAvgState> terminatePartial() {
            return statMap;
        }

        // combine
        public boolean merge(Map<String, UDAFAvgState> o) {
            if (o != null) {
                for (Map.Entry<String, UDAFAvgState> entry : o.entrySet()) {
                    String key = entry.getKey();
                    UDAFAvgState state = entry.getValue();
                    UDAFAvgState newState = new UDAFAvgState();

                    if (statMap.containsKey(key)) {
                        newState.mCount += state.mCount;
                        newState.mSum += state.mSum;
                    } else {
                        newState = state;
                    }

                    statMap.put(key, newState);

                }
            }

            return true;
        }

        // reduce into String
        public String terminate() {
            String result = "";

            for (Map.Entry<String, UDAFAvgState> entry : statMap.entrySet()) {
                String key = entry.getKey();
                UDAFAvgState state = entry.getValue();

                // This is SQL standard - average of zero items should be null.
                Double value = state.mCount == 0 ? null : Double.valueOf(state.mSum / state.mCount);
                result += String.format(";%s:%f", key, value);
            }

            return StringUtils.stripStart(result, ";");
        }
    }
}
