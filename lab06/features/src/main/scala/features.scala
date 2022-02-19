import org.apache.spark.sql.{Column, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

object features {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lab06")
      .getOrCreate()

    import spark.implicits._

    val webLogs = spark.read.json("hdfs:///labs/laba03/weblogs.json")
      .select(col("uid"), explode(col("visits")).alias("visits"))
      .withColumn("host", lower(callUDF("parse_url", $"visits.url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .withColumn("day_of_week", dayofweek(col("visits.timestamp").cast("timestamp")))
      .withColumn("hour", hour(col("visits.timestamp").cast("timestamp")))
      .withColumn("web_day_mon", (col("day_of_week") === 1).cast("int"))
      .withColumn("web_day_tue", (col("day_of_week") === 2).cast("int"))
      .withColumn("web_day_wed", (col("day_of_week") === 3).cast("int"))
      .withColumn("web_day_thu", (col("day_of_week") === 4).cast("int"))
      .withColumn("web_day_fri", (col("day_of_week") === 5).cast("int"))
      .withColumn("web_day_sat", (col("day_of_week") === 6).cast("int"))
      .withColumn("web_day_sun", (col("day_of_week") === 7).cast("int"))
      .withColumn("web_hour_0", (col("hour") === 0).cast("int"))
      .withColumn("web_hour_1", (col("hour") === 1).cast("int"))
      .withColumn("web_hour_2", (col("hour") === 2).cast("int"))
      .withColumn("web_hour_3", (col("hour") === 3).cast("int"))
      .withColumn("web_hour_4", (col("hour") === 4).cast("int"))
      .withColumn("web_hour_5", (col("hour") === 5).cast("int"))
      .withColumn("web_hour_6", (col("hour") === 6).cast("int"))
      .withColumn("web_hour_7", (col("hour") === 7).cast("int"))
      .withColumn("web_hour_8", (col("hour") === 8).cast("int"))
      .withColumn("web_hour_9", (col("hour") === 9).cast("int"))
      .withColumn("web_hour_10", (col("hour") === 10).cast("int"))
      .withColumn("web_hour_11", (col("hour") === 11).cast("int"))
      .withColumn("web_hour_12", (col("hour") === 12).cast("int"))
      .withColumn("web_hour_13", (col("hour") === 13).cast("int"))
      .withColumn("web_hour_14", (col("hour") === 14).cast("int"))
      .withColumn("web_hour_15", (col("hour") === 15).cast("int"))
      .withColumn("web_hour_16", (col("hour") === 16).cast("int"))
      .withColumn("web_hour_17", (col("hour") === 17).cast("int"))
      .withColumn("web_hour_18", (col("hour") === 18).cast("int"))
      .withColumn("web_hour_19", (col("hour") === 19).cast("int"))
      .withColumn("web_hour_20", (col("hour") === 20).cast("int"))
      .withColumn("web_hour_21", (col("hour") === 21).cast("int"))
      .withColumn("web_hour_22", (col("hour") === 22).cast("int"))
      .withColumn("web_hour_23", (col("hour") === 23).cast("int"))
      .withColumn("web_fraction_work_hours", (col("hour") >= 9 && col("hour") < 18).cast("int"))
      .withColumn("web_fraction_evening_hours", (col("hour") >= 18).cast("int"))
      .drop("visits", "host")
      .persist()

    webLogs.count()

    val topDomains = webLogs.groupBy(col("domain"))
      .count()
      .orderBy(col("count").desc_nulls_last)
      .select("domain")
      .filter(col("domain").isNotNull)
      .take(1000)
      .map(row => row.getAs[String]("domain"))
      .sorted

    val domainColumns: Array[Column] = topDomains.map((str: String) => col("`" + str + "`"))

    val isTopDomain = udf((s: String) => if (topDomains.contains(s)) s else null)

    val timeColumnNames: List[String] = List("web_day_mon", "web_day_tue", "web_day_wed",
      "web_day_thu", "web_day_fri", "web_day_sat", "web_day_sun", "web_hour_0", "web_hour_1",
      "web_hour_2", "web_hour_3", "web_hour_4", "web_hour_5", "web_hour_6", "web_hour_7", "web_hour_8",
      "web_hour_9", "web_hour_10", "web_hour_11", "web_hour_12", "web_hour_13", "web_hour_14", "web_hour_15",
      "web_hour_16", "web_hour_17", "web_hour_18", "web_hour_19", "web_hour_20", "web_hour_21", "web_hour_22",
      "web_hour_23", "web_fraction_work_hours", "web_fraction_evening_hours")

    val dfDomains = webLogs
      .withColumn("top_domains", isTopDomain(col("domain")))
      .groupBy("uid")
      .pivot("top_domains")
      .agg(count("uid"))
      .drop("null")
      .na.fill(0)
      .withColumn("features", array(domainColumns: _*))
      .select(col("uid"), col("features"))

    val dfTime = webLogs
      .groupBy("uid")
      .agg(sum("web_day_mon").as("web_day_mon"),
        sum("web_day_tue").as("web_day_tue"),
        sum("web_day_wed").as("web_day_wed"),
        sum("web_day_thu").as("web_day_thu"),
        sum("web_day_fri").as("web_day_fri"),
        sum("web_day_sat").as("web_day_sat"),
        sum("web_day_sun").as("web_day_sun"),
        sum("web_hour_0").as("web_hour_0"),
        sum("web_hour_1").as("web_hour_1"),
        sum("web_hour_2").as("web_hour_2"),
        sum("web_hour_3").as("web_hour_3"),
        sum("web_hour_4").as("web_hour_4"),
        sum("web_hour_5").as("web_hour_5"),
        sum("web_hour_6").as("web_hour_6"),
        sum("web_hour_7").as("web_hour_7"),
        sum("web_hour_8").as("web_hour_8"),
        sum("web_hour_9").as("web_hour_9"),
        sum("web_hour_10").as("web_hour_10"),
        sum("web_hour_11").as("web_hour_11"),
        sum("web_hour_12").as("web_hour_12"),
        sum("web_hour_13").as("web_hour_13"),
        sum("web_hour_14").as("web_hour_14"),
        sum("web_hour_15").as("web_hour_15"),
        sum("web_hour_16").as("web_hour_16"),
        sum("web_hour_17").as("web_hour_17"),
        sum("web_hour_18").as("web_hour_18"),
        sum("web_hour_19").as("web_hour_19"),
        sum("web_hour_20").as("web_hour_20"),
        sum("web_hour_21").as("web_hour_21"),
        sum("web_hour_22").as("web_hour_22"),
        sum("web_hour_23").as("web_hour_23"),
        sum("web_fraction_work_hours").as("web_fraction_work_hours"),
        sum("web_fraction_evening_hours").as("web_fraction_evening_hours"))

    webLogs.unpersist()

    val dfDomainTime = dfDomains.join(dfTime, "uid")

    val usersItemsDF = spark.read
      .parquet("/user/svetlana.lapina/users-items/20200429")


    val result = dfDomainTime.join(usersItemsDF, dfDomainTime.col("uid") === usersItemsDF.col("uid"), "full")
      .drop(usersItemsDF.col("uid"))

        result.write
          .mode("overwrite")
          .format("parquet")
          .option("path", "/user/svetlana.lapina/features")
          .save()
  }
}
