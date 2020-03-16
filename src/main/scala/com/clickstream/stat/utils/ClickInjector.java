package com.clickstream.stat.utils;

import com.clickstream.stat.pojo.ClickStreamRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ClickInjector
{
	private static Random r = new Random();
	public static void main(String[] args)
	{
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/temp")
				.getOrCreate();

		List<ClickStreamRow> click = new ArrayList<ClickStreamRow>();
		List <String> activities = fillValues("activity",7);
		System.out.println(activities);
		List <String> modules = fillValues("module",5);
		List <String> users = fillValues("user",50);
		List <Integer> clients = new ArrayList<Integer>();
		clients.add(1234);
		List <Integer> accounts = new ArrayList<Integer>();
		accounts.add(100);
		accounts.add(101);
		accounts.add(102);
		for (int i = 0 ; i < 1440 ; i++)
		{
			ClickStreamRow row = new ClickStreamRow();
			row.setAccount_id(accounts.get(getRandom(0,3)));
			row.setActivity(activities.get(getRandom(0,7)));
			row.setModule(modules.get(getRandom(0,5)));
			row.setUser_id(users.get(getRandom(0,50)));
			row.setClient_id(1234);
			click.add(row);
		}

		Dataset<ClickStreamRow> clickDs = spark.createDataset(click, Encoders.bean(ClickStreamRow.class));
		clickDs.coalesce(4)
				.write()
				.mode(SaveMode.Append)
				.format("parquet")
				.parquet("clickstream/client_id=1234/date=20200310");
	}

	private static List<String> fillValues(String name, int numValues)
	{
		List <String> values = new ArrayList<String>();
		for (int i = 1 ; i <= numValues ; i++)
		{
			values.add(name + "_" + i);
		}
		return values;
	}

	private static Integer getRandom(int start, int end)
	{

		return r.nextInt(end-start) + start;
	}
}
