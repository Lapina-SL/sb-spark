import com.sun.jdi
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object test {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5, org.apache.kafka:kafka-clients:0.10.1.0")
      .appName("test")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val model_path = spark.conf.get("spark.model.path")
    val topic_in = spark.conf.get("spark.topic.in")
    val topic_out = spark.conf.get("spark.topic.out")

    val kafkaParamsIn = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic_in,
      "startingOffsets" -> "earliest"
    )

    val df = spark.readStream
      .format("kafka")
      .options(kafkaParamsIn)
      .load

    val getDomains = udf((arr: Seq[String]) => arr.map(s => s.replaceAll("https?://(www\\.)?", "").split("/")(0)))

    val schemaVisit = new StructType()
      .add("timestamp", LongType, true)
      .add("url", StringType, true)

    val schema = new StructType()
      .add("uid", StringType, true)
      .add("visits", ArrayType(schemaVisit, true), true)


    val parsedDf = df.select(from_json('value.cast("string"), schema).alias("v"))
      .withColumn("domains", getDomains(map_values(map_from_entries(col("v.visits")))))
      .select(col("v.uid"), col("domains"))

    val model = PipelineModel.load(model_path)
    val trainDf = model.transform(parsedDf)
      .select(col("uid"), col("label_name").alias("gender_age"))

    val cq: StreamingQuery = createKafkaSink(trainDf, topic_out).start()
    //val cq: StreamingQuery = createConsoleSink(trainDf).start()
    cq.awaitTermination()
  }

  def createKafkaSink(df: DataFrame, topic_out: String) = {
    df.selectExpr("CAST(uid AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", topic_out)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", "/user/svetlana.lapina/tmp/chk")
  }

  def createConsoleSink(df: DataFrame) = {
    df
      .writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("truncate", "50")
      .option("numRows", "20")
  }


  case class Model(uid: String, domains: List[String], gender_age: String)
}
