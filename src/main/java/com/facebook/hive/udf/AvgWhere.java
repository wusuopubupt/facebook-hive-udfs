package com.facebook.hive.udf;

/**
 * Created by dash wang on 12/19/16.
 */
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Compute the AVG of row items for which a condition is true;
 * "true" means "not false and also not null."
 *
 * Usage: AVG_WHERE(col_x, thing='blah')
 *
 */
@Description(name = "AVG_WHERE",
        value = "_FUNC_(col) - Compute the AVG of row items for which a condition is true")
public final class AvgWhere extends UDAF {

	public static class UDAFAvgState {
		private double mSum;
		private long mCount;	
	}

    public static class UDAFAvgWhereEvaluator implements UDAFEvaluator {
	
		UDAFAvgState state;

        public UDAFAvgWhereEvaluator() {
            super();
			state = new UDAFAvgState();
            init();
        }

        public void init() {
			state.mSum = 0;
			state.mCount = 0;
        }

        // map
        public boolean iterate(Double item, Boolean ThisBool) {
            if (item != null && ThisBool != null && ThisBool) {
				state.mSum += item;
				state.mCount++;
            }
            return true;
        }

        // map output
        public UDAFAvgState terminatePartial() {
            return state.mCount == 0 ? null : state;
        }

        // combine
        public boolean merge(UDAFAvgState s) {
			if(s != null)	{
				state.mSum += s.mSum;
				state.mCount += s.mCount;			
			}

            return true;
        }

        // reduce
        public Double terminate() {
			// This is SQL standard - average of zero items should be null.
            return state.mCount == 0 ? null : Double.valueOf(state.mSum / state.mCount);
        }
    }
}
