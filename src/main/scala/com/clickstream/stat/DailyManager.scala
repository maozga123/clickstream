package com.clickstream.stat

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Calendar

import com.clickstream.stat.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object DailyManager
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
    val clickStream = spark.read.option("basePath", "clickstream/").parquet("clickstream/client_id=1234/date=20200310").as[ClickRow]
    clickStream.show();

    println("Number of click stream events" + clickStream.count());

    clickStream.createOrReplaceTempView("click_stream")

    val dailyData = spark.sql("select client_id, date, user_id, account_id, module, activity, count(*) " +
      "as activities from click_stream group by client_id,  account_id, date, user_id, module, activity")
    dailyData.show();

    dailyData.write.format("parquet").option("path","/batchZone/clicksData/").partitionBy("client_id", "date").
      saveAsTable("daily_data")


    val format = new SimpleDateFormat("yMd")
    println(format.format(Calendar.getInstance().getTime()))
    val day1 = format.format(DateUtils.asDate(LocalDateTime.now().minusDays(6)))
    //val day3 = format.format(LocalDateTime.now().minusDays(3))

    println(day1)

    // UF
    println("calculate activities per user")
    val numUsersActvities = spark.sql(String.format("Select user_id, module, account_id, sum(activities) as activities " +
      "from daily_data where date >= (%s) group by user_id, module, account_id", day1))

    numUsersActvities.show()

    println("calculate Modules per user")
    val numUsersModules = spark.sql(String.format("Select user_id, account_id, count (module) as modules_count from click_stream" +
      " where date >= (%s) group by user_id, account_id",day1))

    numUsersModules.show()

  }
}
