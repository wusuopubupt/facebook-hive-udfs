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


@Description(name = "concat_str_where",
        value = "_FUNC_(col, condition) - An UDAF that concatenates all col content from different rows into a single string")
public class ConcatStrWhere extends UDAF {

    public static class UDAFConcatStrEvaluator implements UDAFEvaluator {

        private List<String> data;

        public UDAFConcatStrEvaluator() {
            super();
			data = new ArrayList<String>();
            init();
        }

        public void init() {
			data.clear();
        }

        // map
        //public boolean iterate(String[] items) {
        public boolean iterate(String item, Boolean ThisBool) {
            if (ThisBool != null && ThisBool) {
				data.add(item);
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


