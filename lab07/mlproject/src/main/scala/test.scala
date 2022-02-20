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
      .getOrCreate()

    import spark.implicits._

    val model_path = spark.conf.get("model.path")
    val topic_in = spark.conf.get("topic.in")
    val topic_out = spark.conf.get("topic.out")

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

    val json = "{\"uid\": \"bd7a30e1-a25d-4cbf-a03f-61748cbe540e\",\"visits\": [{\"url\": \"http://www.interfax.ru/business/414668\",\"timestamp\": 1419775945781\n    },{\"url\": \"http://amerikan-gruzovik.ru/zapchasti-dlya-amerikanskikh-gruzovikov.html\",\"timestamp\": 1419679865088}]}"

    val parsedDf = df.select(from_json('value.cast("string"), schema_of_json(lit(json))))
      .withColumn("domains", getDomains(map_values(map_from_entries(col("visits")))))
      .select(col("uid"), col("domains"), col("gender_age"))

    val model = PipelineModel.load(model_path)
    val trainDf = model.transform(parsedDf)

    val cq: StreamingQuery = createKafkaSink(trainDf, topic_in).start()
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


  case class Model(uid: String, domains: List[String], gender_age: String)
}
