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


import scala.io.StdIn.readLine

//Play Libraries for Json
import play.api.libs.json._

object KafkaConsumerSubscribeApp1 extends App {
    val spark =    SparkSession.builder.master("local[*]").appName("SparkKafkaConsumer").getOrCreate()
import spark.implicits._

    var database="project3";
    var username="postgres"
    var password="972260Renzo"
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
           while (true) {
                //val records = consumer.poll(10)
		 var records = consumer.poll(100)
	  	 var jsonSeq = Seq[String]()
                for (record <- records.asScala) {
			var recordString:String = record.value()
			//println(s"| Key: ${record.key()} | Value: ${record.value()} | Offset: ${record.offset()} |")
			//println(recordString)
			//var jsonRow:JsValue = Json.parse(recordString)
			//println(jsonRow)
			//println(recordString)
			//println("----------------------------------")
			jsonSeq=jsonSeq :+ recordString
			//println(jsonSeq)
			//println("------------")
			println("Number of records : "+jsonSeq.size)
			//readLine()
           	}
	      if(jsonSeq.nonEmpty && jsonSeq.size > 100) {
	      	println("-------------------------------------------------------------------------")
		var jsonDS = jsonSeq.toDS()
		var df = spark.read.json(jsonDS)
		df=df.drop("_corrupt_record")
		
//		df = df.withColumn("datetime", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")).drop("datetime")
		df = df.withColumn("datetime", to_timestamp(col("time"))).drop("time")
		df.createOrReplaceTempView("orders")
		var df2 = spark.sql("select * from orders")
		var props2 = new Properties()
		props2.put("driver", "org.postgresql.Driver")
		props2.put("user", username)
		props2.put("password", password)
		df2.write.mode("append").jdbc("jdbc:postgresql://127.0.0.1:5432/"+database, "orders", props2)
		df.write.format("jdbc")
		  .option("url", "jdbc:postgresql://127.0.0.1:5432/"+database)
		  .option("dbtable", "orders")
		  .option("user", username)
		  .option("password", password)
		  .mode("append").save()
//		println("finale -----------------")
		jsonSeq = Seq[String]()
	      }
           	
     	   }
     }catch{
          case e:Exception => e.printStackTrace()
     }
     finally {
        consumer.close()
     }
}
