import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Streaming basic example")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._


//  val myTopic = args(0)
  val myTopic = "sparkkafka"


  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", ":9092")
    .option("subscribe", myTopic)
    .load()

  df.printSchema()


  df.writeStream
    .format("console")
    .option("kafka.bootstrap.servers", ":9092")
    .option("truncate", false)
    .start()
    .awaitTermination()
}