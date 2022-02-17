import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset}

import java.sql.Timestamp

object agg {
  val spark = SparkSession.builder()
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5, org.apache.kafka:kafka-clients:0.10.1.0")
    .appName("lab04b")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "svetlana_lapina",
      //"subscribe" -> "lab04_input_data",
      "startingOffsets" -> "earliest"
    )

    val df = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .load
    //

    val schema = new StructType()
      .add("event_type", StringType, true)
      .add("category", StringType, true)
      .add("item_id", StringType, true)
      .add("item_price", StringType, true)
      .add("uid", StringType, true)
      .add("timestamp", LongType, true)


    val parsedDf = df.select(from_json('value.cast("string"), schema).alias("value"))
      .select(col("value.category").alias("category"),
        col("value.event_type").alias("event_type"),
        col("value.item_id").alias("item_id"),
        col("value.item_price").cast("long").alias("item_price"),
        (col("value.timestamp") / 1000).cast("timestamp").alias("timestamp"),
        col("value.uid").alias("uid")
      )
      .withColumn("visitors_uid", col("uid").isNotNull.cast("int"))
      .withColumn("buy_price", col("uid").isNotNull.cast("int") * col("item_price"))
      .withColumn("buy_count", (col("event_type") === lit("buy")).cast("int"))
      .withWatermark("timestamp", "1 hours")
      .groupBy(window($"timestamp", "1 hours"))
      .agg(sum("visitors_uid").as("visitors"),
        sum("buy_price").as("revenue"),
        sum("buy_count").as("purchases"))

    val getStartTs = udf((str: String) => str.substring(1, 20))

    val resultDf = parsedDf
      .withColumn("start_ts", to_timestamp(getStartTs(col("window").cast("string"))).cast("long"))
      .withColumn("end_ts", (col("start_ts") + 3600).cast("long"))
      .withColumn("aov", col("revenue") / col("purchases"))
      .drop(col("window"))
      .as[Count]


    createKafkaSink(resultDf).start().awaitTermination()
  }

  def createConsoleSink(df: Dataset[Count]) = {
    df.selectExpr("1 AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("truncate", "false")
      .option("numRows", "20")
  }

  def createKafkaSink(df: Dataset[Count]) = {
        df.selectExpr("CAST(start_ts AS STRING) AS key", "to_json(struct(*)) AS value")
          .writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers" , "spark-master-1:6667")
          .option("topic", "svetlana_lapina_lab04b_out")
          .trigger(Trigger.ProcessingTime("5 seconds"))
          .option("checkpointLocation", "/user/svetlana.lapina/tmp/chk")
          //.option("checkpointLocation", s"/tmp/chk/$chkName")
  }

  case class Count(start_ts: Long, end_ts: Long, revenue: Long, visitors: Long, purchases: Long, aov: Double)

}
