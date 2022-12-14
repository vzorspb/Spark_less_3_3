import org.apache.spark.sql.functions.{col, date_format, regexp_extract, regexp_replace, round, to_date, to_timestamp}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.sql
import scala.util.Random
object Main extends App {
  def generateDataFrame (datsFrameSize: Int, dfUsers:DataFrame):DataFrame = {
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
      pagesTagsData = pagesTagsData ++ Seq (Row (userData.lift (Random.nextInt (userData.length) ).get (0), new sql.Timestamp ("1668799353153".toLong - Random.nextInt (44640000).abs.toLong * 1000), actionsData.lift (Random.nextInt (actionsData.length) ).get (1), Random.nextInt (10), tagsData.lift (Random.nextInt (tagsData.length) ).get (1), Random.nextInt (2) == 1) )
    }
    return spark.createDataFrame (spark.sparkContext.parallelize (pagesTagsData), pagesTagsSchema)
  }

    val lines = 50 //количество генерируемых строк датафрейма
    val spark = SparkSession.builder
      .master("spark://192.168.251.107:7077")
      .appName("Spark Task 3.3")
      .config("spark.driver.host","192.168.251.105")
      .config("spark.driver.extraClassPath","C:/Users/nevzorov.KB/IdeaProjects/Spark_3_3/out/artifacts/Spark_3_3_jar/Spark_3_3.jar")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

  val usersSchema = new StructType()
    .add("id", IntegerType)       // Идентификатор пользователя
    .add("name", StringType)      // Фамилия
    .add("lkId", IntegerType)    // Идентификационный номер личного кабинета
    .add("createDate", StringType)  // Дата создания
  val userData = Seq(
    Row(12345, "Иванов Сергей Андреевич", 1, "11/20/2022"),
    Row(2, "Петрова Ольга Николаевна", 2, "11/20/2022"),
    Row(3, "Сидорова Оксана Сергеевна", 3, "11/20/2022"),
    Row(4, "Филимонова Ксения Николаевна", null , null),
    Row(5, "Веселов Алексей Борисович", 5, "11/20/2022"),
    Row(6, "Крылов Илья Владимирович", 6, "11/20/2022"),
    Row(7, "Алексеев Николай Васильевич", null , null),
    Row(8, "Васильева Людмила Борисовна", 8, "11/20/2022"),
    Row(9, "Серов Дмитрий Александрович", 9, "11/20/2022"),
    Row(10, "Быков Василий Альбертович", 10, "11/20/2022")
  )
  var dfUsers = spark.createDataFrame(spark.sparkContext.parallelize(userData), usersSchema)

    var dfPages = generateDataFrame (lines, dfUsers)
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
  println("фамилии посетителей, которые читали хотя бы одну новость про спорт.")
  dfPages.join(dfUsers,dfUsers("id")===dfPages("userId"))
    .select("name","tag").filter(col("tag")==="Sport" && col("action")==="click")
    .groupBy(col("name")).count()
    .orderBy(col("name"))
    .select(col("name")).withColumn("family",functions.split(col("name")," ").getItem(0)).select("family")
    .show()

  println ("10% ЛК, у которых максимальная разница между датой создания ЛК и датой последнего посещения")
 // val numLk = ((dfUsers.groupBy("lkId").count().count()-1).toFloat/10+0.5)
  dfPages.join(dfUsers, dfUsers("id") === dfPages("userId"))
    .withColumn("dateDelta", to_timestamp(col("CreateDate"),"MM/dd/yyyy").cast(LongType)-col("timestamp").cast(LongType))
    .groupBy("lkId").max("dateDelta").orderBy(col("max(dateDelta)").desc).select(col="lkId")
    .show(((dfUsers.groupBy("lkId").count().count()-1).toFloat/10+0.5).round.toInt)

  //продолжение следует ...
}
