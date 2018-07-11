import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter


/**
  * Created by onkar on 4/4/17.
  */
object SparkMain {
  def main(args: Array[String]): Unit = {
    dataFrameSample
  }


  private def sparkConf = {
    val file = "/home/onkar/Downloads/SFPD_Incidents_-_from_1_January_2003.csv"
    val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(file).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    var tokens = logData.flatMap(s => s.split(","))
    var tokens_1 = tokens.map(s => (s, 1))
    var sum_each = tokens_1.reduceByKey((a, b) => a + b)
    sum_each.saveAsTextFile("/home/onkar/Temp/output")
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println(s"\n\nWord Count - $sum_each")
    sc.stop()
  }

  def dataFrameSample: Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.S")

    var dataFrame = spark.read.option("header", "true").option("inferSchema", "true").
      csv("/home/onkar/Downloads/SFPD_Incidents_-_from_1_January_2003.csv").cache()

    println(Instant.now())
    println("Sample records")
    dataFrame.limit(5).show(10, false)

    //println(Instant.now())
    //println("Distinct Category")
    //dataFrame.select("Category").distinct.count()

    //println(Instant.now())
    //println("Distinct Category with Order")
    //dataFrame.select("Category").distinct.orderBy("Category").show(40, false)

    //println(Instant.now())
    //println("Category wise Count")
    //dataFrame.select("Category").groupBy("Category").count().orderBy("count").show(40, false)

    val from_pattern = "MM/dd/yyyy"

    println(Instant.now())
    println("Original Schema")
    dataFrame.printSchema()

    dataFrame = dataFrame.withColumn("DateTS", unix_timestamp(dataFrame.col("Date"), from_pattern)
      .cast("timestamp")).drop("Date")

    println(Instant.now())
    println("Schema with timestamp")
    dataFrame.printSchema()
    //spark.stop()

    println(Instant.now())
    println("Year wise count")
    dataFrame.select(year(dataFrame.col("DateTS")).as("year")).groupBy("year").count().orderBy("year").show(40, false)

    println(Instant.now())
    println("Current Partitions")
    println(dataFrame.rdd.getNumPartitions)

    dataFrame.repartition(6).createOrReplaceTempView("dataServiceView")

    println(Instant.now())
    println("After repartitioning")
    println(dataFrame.rdd.getNumPartitions)

    spark.catalog.cacheTable("dataServiceView")

    println(Instant.now())
    println("Reading from cached table")
    spark.table("dataServiceView").count()

    dataFrame = spark.table("dataServiceView")

    println(Instant.now())
    println("Count")
    dataFrame.count()

    println(Instant.now())
    println("Spark SQL : Top 20 category wise count for 2015 ")
    spark.sql("SELECT Category, count(Category) as cnt FROM dataServiceView WHERE year(DateTS) == 2015 " +
      "GROUP BY Category ORDER BY cnt DESC LIMIT 20 ").show()

    println(Instant.now())
    println("Cached table show ")
    dataFrame.select(year(dataFrame.col("DateTS")).as("year")).groupBy("year").count().orderBy("year").show(40, false)

    dataFrame.write.format("parquet").save("/home/onkar/Projects/Sample/Spark-Demo/csv-parquet")

    dataFrame = spark.read.parquet("/home/onkar/Projects/Sample/Spark-Demo/csv-parquet")

    println(Instant.now())
    println("Parquet read ")
    dataFrame.limit(10).show(false)


  }
}