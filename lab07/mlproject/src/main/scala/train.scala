import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object train {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("train")
      .getOrCreate()

    import spark.implicits._

    val model_path = spark.conf.get("model.path")
    val json_path = spark.conf.get("json.path")

    val getDomains = udf((arr: Seq[String]) => arr.map(s => s.replaceAll("https?://(www\\.)?", "").split("/")(0)))

    val training = spark.read
      .json(json_path)
      .withColumn("domains", getDomains(map_values(map_from_entries(col("visits")))))
      .select(col("uid"), col("domains"), col("gender_age"))
      .as[Model]

    training.show(100, 50)

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer: StringIndexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr))

    val model = pipeline.fit(training)

    model.write.overwrite().save(model_path)
  }

  case class Model(uid: String, domains: List[String], gender_age: String)

}
