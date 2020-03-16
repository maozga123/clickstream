package com.clickstream.stat.function;

import com.clickstream.stat.pojo.ClickStreamRow;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Spark function class. It returns the key by which we want to run "groupBy" on our dataset.
 */
public class GroupByFunction implements MapFunction<ClickStreamRow, GroupByActivity>
{
	
	public GroupByActivity call(ClickStreamRow raw)
	{
		return new GroupByActivity(raw.getModule(), raw.getActivity());
	}
}
