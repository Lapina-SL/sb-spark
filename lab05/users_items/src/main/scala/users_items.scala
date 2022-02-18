import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object users_items {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("lab05")
      .getOrCreate()

    import spark.implicits._

    val mode = spark.conf.get("spark.users_items.update")
    val outputDir = spark.conf.get("spark.users_items.output_dir")
    val inputDir = spark.conf.get("spark.users_items.input_dir")

    val df = spark.read
      .json(inputDir + "/*/*/*")
      .filter('uid.isNotNull)
      .withColumn("item", concat('event_type, lit("_"), 'item_id))
      .select('uid, regexp_replace('item, " |-", "_").alias("event"))
      .groupBy('uid)
      .pivot('event)
      .agg(count('uid))
      .na.fill(0)



    mode match {
      case "0" => writeCreate(df, outputDir)
      case "1" => writeUpdate(df, outputDir, spark)
    }
  }

  def writeCreate(df: DataFrame, outputDir: String) = {
    df.write
      .format("parquet")
      .mode("overwrite")
      .option("path", outputDir + "/20200429")
      .save()
  }

  def writeUpdate(df: DataFrame, outputDir: String, spark: SparkSession) = {

    val oldDf = spark.read.parquet(outputDir + "/20200429")
    val newDf = oldDf.union(df)
    newDf.write
      .mode("overwrite")
      .format("parquet")
      .option("path", outputDir + "/20200430")
      .save()
  }
}
