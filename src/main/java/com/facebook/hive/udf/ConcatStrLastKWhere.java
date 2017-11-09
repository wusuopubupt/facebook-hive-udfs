package com.facebook.hive.udf;

/**
 * Created by dash wang on 12/20/16.
 */

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;
import java.util.List;


@Description(name = "concat_str_last_k_where",
        value = "_FUNC_(col, n, condition) - An UDAF that concatenates all last n char of col content from different rows into a single string")
public final class ConcatStrLastKWhere extends UDAF {

    public static class UDAFConcatStrLastKEvaluator implements UDAFEvaluator {

        private List<String> data;

        public UDAFConcatStrLastKEvaluator() {
            super();
			data = new ArrayList<String>();
            init();
        }

        public void init() {
			data.clear();
        }

        // map
        public boolean iterate(String item, int k, Boolean ThisBool) {
            if (ThisBool != null && ThisBool) {
                if (null == item)
                    data.add(null);
                else {
                    // if item.length <= k, return item; else return last k char of item
                    String s = item.length() > k ? item.substring(item.length()-k) : item;
                    data.add(s);
                }
            }
            return true;
        }

        // map output
        public List<String> terminatePartial() {
            return data;
        }

        // combine
        public boolean merge(List<String> otherItems) {
			if(otherItems != null)	{
				data.addAll(otherItems);
			}
            return true;
        }

        // reduce
        public String terminate() {
            return Joiner.on(";").useForNull("").join(data);
        }

        public List<String> getData() {
            return data;
        }
    }
}
