import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, from_json, from_unixtime, schema_of_json, udf, lit}
import org.apache.spark.sql.types.{StringType, StructType, LongType}

object filter {
  val spark = SparkSession.builder()
    .appName("lab04a")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> spark.conf.get("spark.filter.topic_name"),
      "startingOffsets" -> spark.conf.get("spark.filter.offset")
    )

    val df = spark.read
      .format("kafka")
      .options(kafkaParams)
      //.option("checkpointLocation", "/user/svetlana.lapina/tmp/chk")
      .load


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
        col("value.item_price").alias("item_price"),
        col("value.timestamp").alias("timestamp"),
        col("value.uid").alias("uid")
      )
      .withColumn("date", from_unixtime(col("timestamp") / 1000, "yyyyMMdd"))
      .withColumn("p_date", col("date"))
      .persist()

    val viewDf = parsedDf.filter(col("event_type") === lit("view"))
    val buyDf = parsedDf.filter(col("event_type") === lit("buy"))

    viewDf.write
      .partitionBy("p_date")
      .json(spark.conf.get("spark.filter.output_dir_prefix") + "/view")

    buyDf.write
      .partitionBy("p_date")
      .json(spark.conf.get("spark.filter.output_dir_prefix") + "/buy")

    parsedDf.unpersist()
    spark.close()
  }

}