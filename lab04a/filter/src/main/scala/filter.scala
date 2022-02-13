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

    val sdf = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .load

    val schema =  new StructType()
      .add("event_type", StringType, true)
      .add("category", StringType, true)
      .add("item_id", StringType, true)
      .add("item_price", StringType, true)
      .add("uid", StringType, true)
      .add("timestamp", LongType, true)

    val parsedSdf = sdf.select(from_json('value.cast("string"), schema).alias("value"))
      .select(col("value.event_type").alias("event_type"),
        col("value.category").alias("category"),
        col("value.item_id").alias("item_id"),
        col("value.item_price").alias("item_price"),
        col("value.uid").alias("uid"),
        col("value.timestamp").alias("timestamp")
      )
      .withColumn("date", from_unixtime(col("timestamp") / 1000, "yyyyMMdd"))
      .withColumn("p_date", col("date"))

    val viewDf = parsedSdf.filter(col("event_type") === lit("view"))
    val buyDf = parsedSdf.filter(col("event_type") === lit("buy"))

    val sink1 = createViewSink(viewDf)
    val sink2 = createBuySink(buyDf)

    val sq1 = sink1.start
    val sq2 = sink2.start

    sq1.awaitTermination()
    sq2.awaitTermination()
  }

  def createConsoleSink(df: DataFrame): DataStreamWriter[Row] = {
    df.writeStream
      //.partitionBy("p_event_type", "p_date")
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch((batch, id) => batch.show(20, false))
  }

  def createViewSink(df: DataFrame): DataStreamWriter[Row] = {
    df.writeStream
      .partitionBy("p_date")
      .format("parquet")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", "/user/svetlana.lapina/tmp/chk/view")
      .option("path", spark.conf.get("spark.filter.output_dir_prefix")+ "/view")
  }

  def createBuySink(df: DataFrame): DataStreamWriter[Row] = {
    df.writeStream
      .partitionBy("p_date")
      .format("parquet")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", "/user/svetlana.lapina/tmp/chk/buy")
      .option("path", spark.conf.get("spark.filter.output_dir_prefix")+ "/buy")
  }
}