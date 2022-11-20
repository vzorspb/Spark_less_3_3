import org.apache.spark.sql.functions.{col, date_format, regexp_extract, regexp_replace}
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
    for (a <- 1 to datsFrameSize-1) {
      pagesTagsData = pagesTagsData ++ Seq (Row (userData.lift (Random.nextInt (userData.length) ).get (0), new sql.Timestamp ("1668799353153".toLong - Random.nextInt ().abs.toLong * 1000), actionsData.lift (Random.nextInt (actionsData.length) ).get (1), Random.nextInt (10), tagsData.lift (Random.nextInt (tagsData.length) ).get (1), Random.nextInt (2) == 1) )
    }
    return spark.createDataFrame (spark.sparkContext.parallelize (pagesTagsData), pagesTagsSchema)
  }

    val lines = 200 //количество генерируемых строк датафрейма
    val spark = SparkSession.builder
      .master("spark://192.168.251.107:7077")
      .appName("Spark Task 3.3")
      .config("spark.driver.host","192.168.251.106")
      .config("spark.driver.extraClassPath","C:/Users/nevzorov.KB/IdeaProjects/Spark_3_3/out/artifacts/Spark_3_3_jar/Spark_3_3.jar")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    var dfPages = generateDataFrame (200)
    dfPages.show()
  //  dfPages.coalesce(1).write.mode("overwrite").option("header","true").csv("hdfs://192.168.251.105/df/df.csv")
  println("Топ 5 активных пользователей")
   dfPages.groupBy("userId").count().orderBy(col("count").desc).show(5)
   println("Процент посетителей, у которых есть ЛК")
  dfPages.groupBy("sign").count().withColumn("procents",col("count")/lines*100).select("procents").show(1)
   println("Временной промежуток, в течение которого было больше всего активностей на сайте")
//  timeWindow1 ("00","01","02","03")
//  timeWindow2 ("04","05","06","07")
//  timeWindow3 ("08","09","10","11")
//  timeWindow4 ("12","13","14","15")
//  timeWindow5 ("16","17","18","19")
//  timeWindow6 set("20","21","22","23")
  dfPages.withColumn("timeWindow",regexp_replace(date_format(col("timestamp"), "HH"),"0[0-3]","timeWindow1"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"0[4-7]", "timeWindow2"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"0[8-9]", "timeWindow3"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"1[0,1]", "timeWindow3"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"1[2-5]", "timeWindow4"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"1[6-9]", "timeWindow5"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"2[0-3]", "timeWindow6"))
    .groupBy("timeWindow").count()
    .orderBy(col("count").desc)
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"timeWindow1", "00:00 to 03:59"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"timeWindow2", "04:00 to 07:59"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"timeWindow3", "08:00 to 11:59"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"timeWindow4", "12:00 to 15:59"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"timeWindow5", "16:00 to 10:59"))
    .withColumn("timeWindow",regexp_replace(col("timeWindow"),"timeWindow6", "20:00 to 23:59"))
    .show(1)

  //продолжение следует ...
}
