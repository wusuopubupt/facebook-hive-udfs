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

public final class TopNCntCategoryWhere extends UDAF {

    public static class UDAFCountWhereEvaluator implements UDAFEvaluator {

        // top n
        int n;

        boolean initialized = false;

        // does not have category
        Double s;

        // has category
        Map<String, Long> state;

        public UDAFCountWhereEvaluator() {
            super();
            state = new HashMap<>();
            init();
        }

        public void init() {
            state.clear();
        }

        public boolean iterate(Double item, Boolean ThisBool, String category, int n) {
            if(!initialized) {
                this.n = n;
                initialized = true;
            }

            if (item != null && ThisBool != null && ThisBool && category != null) {
                long newCnt = 1;
                if (state.containsKey(category)) {
                    newCnt += state.get(category);
                }
                state.put(category, newCnt);
            }
            return true;
        }

        public Map<String, Long> terminatePartial() {
            return state;
        }

        public boolean merge(Map<String, Long> o) {
            for (Map.Entry<String, Long> entry : o.entrySet()) {
                long cnt = entry.getValue();
                if (state.containsKey(entry.getKey()) && 0 < state.get(entry.getKey()))
                    cnt += state.get(entry.getKey());
                state.put(entry.getKey(), cnt);
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
            Map<String, Long> m = MapUtil.sortByValueDescAndGetTopN(state, n);
            String result = "";
            for(Map.Entry<String, Long> entry: m.entrySet()) {
                result += String.format(";%s:%d", entry.getKey(), entry.getValue());
            }
            return StringUtils.stripStart(result, ";");
        }

    }
}
