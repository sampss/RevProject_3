
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
object KafkaProducerApp1 extends App {

    def producerFunc(stringToSend:String)
    {
	  val props:Properties = new Properties()
	  props.put("bootstrap.servers","localhost:9092")
	  props.put("key.serializer",
		 "org.apache.kafka.common.serialization.StringSerializer")
	  props.put("value.serializer",
		 "org.apache.kafka.common.serialization.StringSerializer")
	  props.put("acks","all")
	  val producer = new KafkaProducer[String, String](props)
	  val topic = "topictext"

	  try {
	    val record1 = new ProducerRecord[String, String](topic,stringToSend)
            val metadata1 = producer.send(record1)
	  }catch{
	    case e:Exception => e.printStackTrace()
	  }finally {
	    producer.close()
	  }
    }
    
    for(i<-0 to 10)
    {
    	val a="Message "+i
    	producerFunc(a)
    }
    
}

