//Java Libraries
import java.util.Properties

//Scala Libraries
import scala.collection.JavaConvertors._
import scala.collection.mutable.ListBuffer

//Kafka Libraries
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

//Spark Libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField}

//Play Libraries for Json
import play.api.libs.json._

object Main extends App {
  
  ConsumerExample.consumer()
  //ToDatabase.PSQL(database="project3", username="user", password="pass", topic="topicName")

}

object ConsumerExample {
  val spark = SparkSession.builder.master("local[*]").appName("SparkKafkaConsumer").getOrCreate()
  import spark.implicits._

  val topicName = "test_topic"

  def consumer():Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apahce.kafka.common.serialization.StringDeserializer")
    props.put("group.id", topicName)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")

    val consumer = new KafkaConsumer(props)
    val topics = List(topicName)
    var subscribed = true

    try {

      consumer.subscribe(topics.asJava)

      while(subscribed) {

        val records = consumer.poll(100)
        for(record <- records.asScala) {

          val recordString:String = record.value()
          println(s"| key = ${record.key()} | value = ${record.value()} | offset = ${record.offset()} |")
          val jsonD:JsValue = Json.parse(recordString)
          //println("Key():" + jsonD("order_id"))
          //println(jsonD)

        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
