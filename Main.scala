import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import java.sql
import scala.util.Random
object Main extends App {
  def generateDataFrame (datsFrameSize: Int):DataFrame = {
    val pagesTagsSchema = new StructType ()
    .add ("userId", IntegerType) //  уникальный идентификатор посетителя сайта.
    .add ("timestamp", TimestampType) // дата и время события в формате unix timestamp
    .add ("action", StringType) // тип события
    .add ("pageId", IntegerType) // id текущей страницы
    .add ("tag", StringType) //  список тематик
    .add ("sign", BooleanType) // наличие у пользователя личного кабинета
    var pagesTagsData = Seq (
    Row (12345, new sql.Timestamp (System.currentTimeMillis () ), "click", 101, "Sport", false)
    )
    val usersSchema = new StructType ()
    .add ("id", IntegerType)
    .add ("name", StringType)
    val userData = Seq (
      Row (12345, "Фамилия 1"),
      Row (2, "Фамилия 2"),
      Row (3, "Фамилия 3"),
      Row (4, "Фамилия 4"),
      Row (5, "Фамилия 5"),
      Row (6, "Фамилия 6"),
      Row (7, "Фамилия 7"),
      Row (8, "Фамилия 8"),
      Row (9, "Фамилия 9"),
      Row (10, "Фамилия 10")
    )
    var dfUsers = spark.createDataFrame (spark.sparkContext.parallelize (userData), usersSchema)
    val tagsSchema = new StructType ()
    .add ("id", IntegerType)
    .add ("name", StringType)
    val tagsData = Seq (
    Row (1, "Sport"),
    Row (2, "News"),
    Row (3, "Fun")
    )
    val actionsSchema = new StructType ()
    .add ("id", IntegerType)
    .add ("name", StringType)
    val actionsData = Seq (
    Row (1, "click"),
    Row (2, "view"),
    )
    var dfAction = spark.createDataFrame (spark.sparkContext.parallelize (actionsData), actionsSchema)
    var dfTags = spark.createDataFrame (spark.sparkContext.parallelize (tagsData), tagsSchema)
    for (a <- 1 to datsFrameSize) {
      pagesTagsData = pagesTagsData ++ Seq (Row (userData.lift (Random.nextInt (userData.length) ).get (0), new sql.Timestamp ("1668799353153".toLong - Random.nextInt ().abs.toLong * 1000), actionsData.lift (Random.nextInt (actionsData.length) ).get (1), Random.nextInt (101), tagsData.lift (Random.nextInt (tagsData.length) ).get (1), Random.nextInt (2) == 1) )
    }
    return spark.createDataFrame (spark.sparkContext.parallelize (pagesTagsData), pagesTagsSchema)
  }


    val spark = SparkSession.builder
      .master("spark://192.168.251.107:7077")
      .appName("Spark Task 3.3")
      .config("spark.driver.host","192.168.251.106")
      .config("spark.driver.extraClassPath","C:/Users/nevzorov.KB/IdeaProjects/Spark_3_3/out/artifacts/Spark_3_3_jar/Spark_3_3.jar")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    var dfPages = generateDataFrame (100)
    dfPages.show()
  //  dfPages.coalesce(1).write.mode("overwrite").option("header","true").csv("hdfs://192.168.251.105/df/df.csv")
  println("Топ 5 активных пользователей")
   dfPages.groupBy("userId").count().orderBy(col("count").desc).show(5)


  //продолжение следует ...
}
