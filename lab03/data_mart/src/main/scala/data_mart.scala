import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{col, count, explode, udf}

import java.net.{URL, URLDecoder}

object data_mart {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("lab03")
      .getOrCreate()

    val users = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("table", "clients")
      .option("keyspace", "labdata")
      .option("spark.cassandra.connection.host", "10.0.0.31")
      .option("spark.cassandra.connection.port", "9042")
      .load()

    def getShopCategory = udf((a: String) => "shop_".concat(a.toLowerCase().replaceAll(" |-", "_")))

    val logsMarket = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.index", "visits")
      .option("es.nodes", "10.0.0.31")
      .option("es.net.http.auth.user", "svetlana.lapina")
      .option("es.net.http.auth.pass", "")
      .load("visits")
      .select(getShopCategory(col("category")).alias("category"), col("uid"))
      .na.drop("any")

    def getUrl = udf((a: String) => a.replaceAll("https?://(www\\.)?", "").split("/")(0))

    val logsWeb = spark.read.json("hdfs:///labs/laba03/weblogs.json")
      .select(col("uid"), explode(col("visits")).alias("visits"))
      .select(col("uid"), col("visits.url").alias("url"))
      .withColumn("domain", getUrl(col("url")))

    val categoryWeb = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "svetlana_lapina")
      .option("password", "")
      .option("driver", "org.postgresql.Driver")
      .load()

    def getWebCategory = udf((a: String) => "web_".concat(a.toLowerCase().replaceAll(" |-", "_")))

    val webCat = logsWeb.alias("left").join(categoryWeb.alias("right"))
      .where(col("left.domain") === col("right.domain"))
      .select(getWebCategory(col("right.category")), col("left.uid"))

    val categories = logsMarket.union(webCat)

    def getAgeCategory = udf((a: Integer) => {
      if (a <= 24) "18-24"
      else if (a <= 34) "25-34"
      else if (a <= 44) "35-44"
      else if (a <= 54) "45-54"
      else ">=55"
    })

    val usersCategories = users.join(categories, users("uid") === categories("uid"), "left")
      .select(users("uid"),
        col("gender"),
        getAgeCategory(col("age")).alias("age_cat"),
        col("category"))


    val result = usersCategories
      .groupBy(col("uid"), col("gender"), col("age_cat"))
      .pivot(col("category"))
      .agg(count(col("uid")))
      .drop(col("null"))
      .na.fill(0)

    result.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/svetlana_lapina")
      .option("dbtable", "clients")
      .option("user", "svetlana_lapina")
      .option("password", "")
      .option("driver", "org.postgresql.Driver")
      .save()

  }
}
