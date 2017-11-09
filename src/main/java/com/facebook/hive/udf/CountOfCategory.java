package com.facebook.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by baiyang on 16/11/24.
 */

/**
 * This is a simple UDAF that calculates categorical count.
 *
 * It should be very easy to follow and can be used as an example for writing
 * new UDAFs.
 *
 * Note that Hive internally uses a different mechanism (called GenericUDAF) to
 * implement built-in aggregation functions, which are harder to program but
 * more efficient.
 *
 */
@Description(name = "example_avg",
        value = "_FUNC_(col) - Example UDAF to compute average")
public final class CountOfCategory extends UDAF {

    /**
     * The internal state of an aggregation for average.
     *
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     *
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

        Map<String, Integer> state;


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
         *
         * The number and type of arguments need to the same as we call this UDAF
         * from Hive command line.
         *
         * This function should always return true.
         */
        public boolean iterate(String cate, int cnt) {
            if (null != cate && cnt > 0) {
                int newCnt = cnt;
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
            for (Map.Entry<String, Integer> entry : o.entrySet()) {
                int cnt = entry.getValue();
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
        public Map<String, Integer> terminate() {
            return state;
        }
        */

        // reduce into String
        public String terminate() {
            String result = "";

            for (Map.Entry<String, Integer> entry : state.entrySet()) {
                result += String.format(";%s:%d", entry.getKey(), entry.getValue());
            }

            return StringUtils.stripStart(result, ";");
        }
    }

}