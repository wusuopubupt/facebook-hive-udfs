package com.facebook.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.Map;
import java.util.TreeMap;

public final class LogCountCategoryWhere extends UDAF {

    /**
     * The internal state of an aggregation for average.
     * <p>
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     * <p>
     * The internal state can also contains fields with types like
     * ArrayList<String> and HashMap<String,Double> if needed.
     */
    public static class UDAFAvgState {
        private long mCount;
        private double mSum;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class AocEvaluator implements UDAFEvaluator {

        Map<String, Double> state;


        public AocEvaluator() {
            super();
            state = new TreeMap<>();
            init();
        }

        /**
         * Reset the state of the aggregation.
         */
        public void init() {
        }

        /**
         * Iterate through one row of original data.
         * <p>
         * The number and type of arguments need to the same as we call this UDAF
         * from Hive command line.
         * <p>
         * This function should always return true.
         */
        public boolean iterate(Boolean ThisBool, String cate) {
            if (ThisBool != null && ThisBool && null != cate) {
                double newCnt = 1;
                if (state.containsKey(cate))
                    newCnt += state.get(cate);
                state.put(cate, newCnt);
            }
            return true;
        }

        /**
         * Terminate a partial aggregation and return the state. If the state is a
         * primitive, just return primitive Java classes like Integer or String.
         */
        public Map<String, Double> terminatePartial() {
            return state;
        }

        /**
         * Merge with a partial aggregation.
         * <p>
         * This function should always have a single argument which has the same
         * type as the return value of terminatePartial().
         */
        public boolean merge(Map<String, Integer> o) {
            for (Map.Entry<String, Integer> entry : o.entrySet()) {
                double cnt = entry.getValue();
                if (state.containsKey(entry.getKey()) && 0 < state.get(entry.getKey()))
                    cnt += state.get(entry.getKey());
                state.put(entry.getKey(), cnt);
            }
            return true;
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        /*
        public Map<String, Double> terminate() {
            for(Map.Entry<String, Double> entry: state.entrySet()) {
               state.put(entry.getKey(), Math.log(entry.getValue()));
            }

            return state;
        }
        */

        // return String instead of Map, since prophet does not support Map data type for now
        public String terminate() {
            String result = "";

            for (Map.Entry<String, Double> entry : state.entrySet()) {
                // reduce into string
                result += String.format(";%s:%f", entry.getKey(), Math.log(entry.getValue()));
            }

            return StringUtils.stripStart(result, ";");
        }
    }

}