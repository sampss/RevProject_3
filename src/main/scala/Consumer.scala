//Java Libraries
import java.util.Properties
import java.util.Calendar
import java.sql.DriverManager
import java.sql.Connection

//Scala Libraries
import scala.collection.JavaConvertors._
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

object Main extends App {
  
  Consumer.consumer("project3","postgres","password")
  //ToDatabase.PSQL(database="project3", username="user", password="pass", topic="topicName")

}

object Consumer {

  val spark = SparkSession.builder.master("local[*]").appName("SparkKafkaConsumer").getOrCreate()
  import spark.implicits._

  def consumer(database:String, username:String, password:String, topic:String = "test_topic"):Unit = {
    import spark.SqlContext.implicits._

    val props = new Properties()
    props.put("boostrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", topic)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")

    val cons = new KafkaConsumer(props)
    val topics = List(topic)

    cons.subscribe(topics.asJava)

    while(true) {
      
      var records = cons.poll(100)
      var jsonSeq = new ListBuffer[String]()

      for(record <- records.asScala) {

        var recordString:String = record.value()
        println(s"| Key: ${record.key()} | Value: ${record.value()} | Offset: ${record.offset()} |")
        var jsonRow:JsValue = Json.parse(recordString)
        println(jsonRow)

        jsonSeq += recordString
      }

      if(jsonSeq.nonEmpty && jsonSeq.size > 100) {

        var jsonDS = jsonSeq.toDS()
        var df = spark.read.json(jsonDS)

        df = df.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")).drop("datetime")

        df.createOrReplaceTempView("orders")
        var df2 = spark.sql("select * from orders")
        var props2 = new Properties()
        props2.put("driver", "org.postgresql.Driver")
        props2.put("user", username)
        props2.put("password", password)
        df2.write.mode("append").jdbc("jdbc:postgresql://localhost:5432/"+database, "orders", props2)
        df.write.format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432"+database)
          .option("dbtable", "orders")
          .option("user", username)
          .option("password", password)
          .mode("append").save()
      }
    }

    cons.close()

    consumer(topic)

  }
}

