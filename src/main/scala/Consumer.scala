//Java Libraries
import java.util.Properties

//Scala Libraries
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

//Kafka Libraries
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

//Spark Libraries
import org.apache.spark.sql._

//Play Libraries for Json
import play.api.libs.json._

object Consumer extends App {

  val topicName = "test_topic"   //I DON'T KNOW WHAT THE TOPIC NAME IS GOING TO BE SO MAKE SURE TO CHANGE IT

  val props = new Properties()
  props.put("boostrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", topicName)
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer(props)
  val topics = List(topicName)
  var jsonSeq = new ListBuffer[String]()
  val subscribed = true

  consumer.subscribe(topics.asJava)

  while(subscribed) {
    val records = consumer.poll(100)
    for(record <- records.asScala){
      val recordString:String = record.value()
      val jsonD:JsValue = Json.parse(recordString)
      println("Key():" + jsonD("order_id"))
      println(jsonD)

      println("condition: "+(record.offset() == records.count()-1) + "offset, count: "+record.offset() + " " + records.count())
      jsonSeq += recordString
    }
  }

}
