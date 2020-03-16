package com.clickstream.stat

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


object AggregationManager
{
  case class ClickRow(account_id:Int, activity:String, client_id:Int, module:String, user_id:String)

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    import spark.implicits._
    val clickStream = spark.read.parquet("clickstream/client_id=1234/date=20200310").as[ClickRow]

    println("Number of click stream events" + clickStream.count());

    val format = new SimpleDateFormat("yMd")
    println(format.format(Calendar.getInstance().getTime()))

    clickStream.createOrReplaceTempView("click_stream")

    println("calculate activities per user")
    val numUsersActvities = spark.sql("Select user_id, module, activity , count (*) as activities_count from click_stream " +
      "group by user_id, module, activity")

    numUsersActvities.show()
    numUsersActvities.coalesce(4).write.mode(SaveMode.Append).parquet("batchZone/client_id=1234/date=20200310/statistics/activities_per_user/")

    println("calculate activities per account")
    val numAccountActivities = spark.sql("Select account_id, module,activity , count (*) as activities_count from click_stream " +
      "group by  account_id , module, activity")
    numAccountActivities.show()
    numAccountActivities.coalesce(4).write.mode(SaveMode.Append).parquet("batchZone/client_id=1234/date=20200310/statistics/activities_per_account/")

    println("calculate Modules per user")
    val numUsersModules = spark.sql("Select user_id, module, count (*) as modules_count from click_stream " +
      "group by user_id, module")

    numUsersModules.show()
    numUsersModules.coalesce(4).write.mode(SaveMode.Append).parquet("batchZone/client_id=1234/date=20200310/statistics/modules_per_user/")

    println("calculate Modules per account")
    val numAccountModules = spark.sql("Select account_id, module,count (*) as modules_count from click_stream " +
      "group by  account_id , module")
    numAccountModules.show()
    numAccountModules.coalesce(4).write.mode(SaveMode.Append).parquet("batchZone/client_id=1234/date=20200310/statistics/modules_per_account/")

    val numUniqueUsers = spark.sql("Select account_id , user_id  from click_stream " +
      "group by  account_id,user_id")
    numUniqueUsers.show()

/*

    val numUniqueUsers = spark.sql("Select account_id , count(DISTINCT (user_id))  from click_stream " +
      "group by  account_id")
    numUniqueUsers.show()
*/

    //clickStream.write.mode(SaveMode.Append).parquet("clickstream/client_id=1234/date=20191223")

  }
}
