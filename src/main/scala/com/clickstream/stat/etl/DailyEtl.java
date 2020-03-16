package com.clickstream.stat.etl;

import com.clickstream.stat.function.GroupByActivity;
import com.clickstream.stat.function.GroupByFunction;
import com.clickstream.stat.pojo.ClickStreamRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DailyEtl
{
	public static void main(String[] args)
	{
		SparkSession spark = SparkSession
				.builder()
				.appName("Click Stream Statistics")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/temp")
				.getOrCreate();
		DailyEtl dailyEtl = new DailyEtl();
		dailyEtl.calculateStatistics(spark);
	}

	private void calculateStatistics(SparkSession spark)
	{
		Dataset<ClickStreamRow> events = spark.read().parquet("clickstream/client_id=1234/date=20200310").as(Encoders.bean(ClickStreamRow.class));

	}
}
