import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

import java.util.Properties
import java.util.Calendar
import java.sql.DriverManager
import java.sql.Connection

//Scala Libraries
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

//Kafka Libraries
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

//Spark Libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

//Play Libraries for Json
import play.api.libs.json._

object KafkaConsumerSubscribeApp2 extends App {
  val spark = SparkSession.builder.master("local[*]").appName("SparkKafkaConsumer").getOrCreate()
  import spark.implicits._

  var database="data_test";
  var table = "teamb"
  var username="postgres"
  var password="wagle"
  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer(props)
  val topics = List("test_topic")
  try {
    consumer.subscribe(topics.asJava)
    var jsonSeq = new ListBuffer[String]
    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        var record_string: String = record.value()
        var jsonD: JsValue = Json.parse(record_string)
        jsonSeq += record_string

      }

      if(jsonSeq.nonEmpty && jsonSeq.size > 100) {
        var test = jsonSeq.toList
        var jsonDS = test.toDS()
        var df = spark.read.json(jsonDS)

        //df = df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd"))
        //df = df.withColumn("datetime", date_format(to_date(col("datetime"),"dd-MM-yyyy"),"yyyy-MM-dd"))
        //df = df.withColumn("datetime", col("datetime").cast())

        val df1 = df.withColumn("datetime", to_date(col("datetime"), "yyyy-MM-dd"))



        df1.show()
        df1.printSchema()


        println("before temp table")
        df1.createOrReplaceTempView("orders")
        var df2 = spark.sql("select * from orders")
        var props2 = new Properties()
        props2.put("driver", "org.postgresql.Driver")
        props2.put("user", username)
        props2.put("password", password)

        df2.write.mode("append").jdbc("jdbc:postgresql://localhost:5432/"+database, table, props2)
        println("df2 write")
        df1.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/"+database)
          .option("dbtable", table)
          .option("user", username)
          .option("password", password)
          .mode("append").save()

        println("finished writing to database")

        jsonSeq.clear()
      }

    }
  }catch{
    case e:Exception => e.printStackTrace()
  }
  finally {
    consumer.close()
  }
}









