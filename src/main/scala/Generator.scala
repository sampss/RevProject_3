import java.sql.{Connection, DriverManager}
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}
import java.util.Properties
//import scala.collection._
import scala.io._
import scala.language.postfixOps
import scala.util.matching.Regex
import java.sql.ResultSetMetaData
import java.text.{DateFormat, FieldPosition, ParsePosition, SimpleDateFormat}
import java.util.Date
import scala.collection.mutable._
import java.time._
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Generator {

  def main(args: Array[String]): Unit = {

    var presets = Map(
      "minNumSales" -> "1000", // minimum number of records allowed
      "startDate" -> "1980,1,1", // start date of sales
      "endDate" -> "2000,1,1", // end date of sales
      "minRecordsPerDay" -> "" // min records per day - result of days / numberSales -- Auto adjusted based on minNumSales and Dates
    )

    // some default values
    // default path to current csv file
    val path = "C:\\Users\\samps\\IdeaProjects\\RevProject_3\\src\\resources\\"
    val path1 = path + "customer.csv"
    val path2 = path + "product.csv"

    getCSVFiles(presets, path1, path2)

  }

  def getCSVFiles(presets: Map[String, String], filePath1: String = "", filePath2: String = ""): Unit = {

    val customerFile = Source.fromFile(filePath1)
    val productFile = Source.fromFile(filePath2)
    // create column header for customers
    val customerColumnsSrc = customerFile.getLines.take(1).toList
    val customerColumns = customerColumnsSrc(0).split(",").toList // create list of customer columns
    // create column header for products
    val productColumnsSrc = productFile.getLines.take(1).toList
    val productColumns = productColumnsSrc(0).split(",").toList // create list of product columns
    // get remaining lines from csv to create data table
    val customerData = customerFile.getLines.toList
    val productData = productFile.getLines.toList
    // Build a list of lists as a table structure so I can randomly select information
    val customerValuesTable = buildList(customerData)
    val productValuesTable = buildList(productData)

    // handoff to build sales event function to build the sales event and pass each built sale to the producer
    createSaleEvent(presets, customerValuesTable, productValuesTable, customerColumns, productColumns) // may change order of this in the future, to send to producer then producer func calls create sales event?

  }

  def buildList(initList: List[String]): List[List[String]] = {

    val tempList = new ListBuffer[List[String]]()

    for (x <- 0 until initList.length) {
      val tempSubList = initList(x).split(",").toList
      tempList.append(tempSubList)
    }
    return tempList.toList
  }

  def createSaleEvent(presets: Map[String, String], customerData: List[List[String]], productData: List[List[String]], customerColumnHeader: List[String], productColumnHeader: List[String]): Unit = {

    // setup default payment types
    val paymentTypes = List("debit card", "credit card", "internet banking", "upi", "wallet")
    // setup default failure reasons
    val failureReasons = List("Insufficient Funds", "Invalid Input", "Connection Error")
    // setup default websites
    val websites = Map(
      "gogogrocery.com" -> "Fruits, Beverages, Baby Food, Vegetables, Cereal, Meat, Snacks ",
      "thehardstore.net" -> "Hardware, Household, Office Supplies, Clothes",
      "EGad-get.com" -> "Tech_Hardware",
      "allgoods.net" -> "",
      "NailsNNails.com" -> "Hardware, Cosmetics, Personal Care, Clothes",
      "memawmall.net" -> "Clothes, Cosmetics, Household, Snacks, Personal Care",
      "onfice.com" -> "Tech_Hardware, Office Supplies",
      "discodiscount.com" -> "Cosmetics, Clothes, Entertainment_Technology",
      "petesbigbutcher.net" -> "Meat, Clothes, Snacks",
      "vis4vegan.com" -> "Fruits, Beverages, Baby Food, Vegetables, Snacks"
    )
    val websiteNames = websites.keys.toList
    // set the start date to preset date if set, otherwise set date to current date
    val startDate = {
      if (presets("startDate") != "") {
        dateWorker(presets, "startDate")
      } else {
        LocalDate.now()
      }
    }
    // set end date value, ' need to make if equal to or greater than start iggy and keep running '
    val endDate = {
      if (presets("endDate") != "") {
        dateWorker(presets, "endDate")
      } else {
        LocalDate.now()
      }
    }
    // set a current pointer to update the date of each sale
    var currentDatePointer = startDate
    // calculate number of days to run sales events
    var salesPeriod = (endDate.toEpochDay - startDate.toEpochDay).toInt
    //  set up sales id variable to track sales Id number that is generated
    var orderId = 0
    //  set min records per day to meet the criteria if minNumSales is set
    if (presets("minNumSales") != "") {
      presets("minRecordsPerDay") = math.ceil(presets("minNumSales").toFloat / salesPeriod.toFloat).toInt.toString
    }
    // setup default string variable to
    var thisSale = new StringBuilder("")


    // set a time and count for records to see how long and how many records produced.
    val startTime = LocalDateTime.now()
    var count = 0
    // temporary loop to see all moving parts just to create a list of 20 years data 3 sales per day


    for (x <- 0 to salesPeriod) {
      for (y <- 0 to presets("minRecordsPerDay").toInt) {
        // to text how many sales records are created
        count = count + 1
        // ---------------------    This section may be moved to other function     -----------------------------

        // generate random customer
        val customer = customerData(Random.nextInt(customerData.length))
        // generate random product
        val product = productData(Random.nextInt(productData.length))

        // payment_type
        val paymentType = paymentTypes(Random.nextInt(paymentTypes.length))
        // select quantity between 1 and 100 - default 1-30, 50% - 31-65, 35% - 66-100, 15%
        val qty = selectQty() // can add logic to update or change the range later format List(List(percentChance Int, highestValueWithThisPercent Int))
        // set price of product to price of product plus generated qty
        val price = (product(3) * qty).toString.replaceAll("(?<=\\d\\.\\d{2}).*", "")
        // set current dateTime
        val dateTime = currentDatePointer
        // generate website ordered from at random
        val website = websiteNames(Random.nextInt(websiteNames.length)).toString // store as string so can be used to call other info
        // setup transaction Id -- to be unique, used orderId and Date
        val paymentTxnId = s"${orderId}${currentDatePointer.toEpochDay}"
        // generate random failure rate - set failure rate to 20% in this example
        val paymentTxnSuccess = if (Random.nextInt(100) > 20) {
          "pass"
        } else {
          "fail"
        }
        // setup basic failure reason
        val failureReason = if (paymentTxnSuccess == "fail") {
          failureReasons(Random.nextInt(failureReasons.length))
        } else {
          ""
        }

        // ---------- Build JSON Section -----------//
        thisSale ++= "{\"order_id\":\"" + orderId + "\", "
        thisSale ++= "\"" + customerColumnHeader(0) + "\":\"" + customer(0) + "\", "
        thisSale ++= "\"" + customerColumnHeader(1) + "\":\"" + customer(1) + "\", "
        thisSale ++= "\"" + productColumnHeader(0) + "\":\"" + product(0) + "\", "
        thisSale ++= "\"" + productColumnHeader(1) + "\":\"" + product(1) + "\", "
        thisSale ++= "\"" + productColumnHeader(2) + "\":\"" + product(2) + "\", "
        thisSale ++= "\"payment_type\":\"" + paymentType + "\", "
        thisSale ++= "\"qty\":\"" + qty.toString + "\", "
        thisSale ++= "\"price\":\"" + price + "\", "
        thisSale ++= "\"datetime\":\"" + dateTime.toString + "\", "
        thisSale ++= "\"" + customerColumnHeader(2) + "\":\"" + customer(2) + "\", "
        thisSale ++= "\"" + customerColumnHeader(3) + "\":\"" + customer(3) + "\", "
        thisSale ++= "\"ecommerce_website_name\":\"" + website + "\", "
        thisSale ++= "\"payment_txn_id\":\"" + paymentTxnId + "\", "
        thisSale ++= "\"payment_txn_success\":\"" + paymentTxnSuccess + "\", "
        thisSale ++= "\"failure_reason\":\"" + failureReason + "\"}"

        println("//-------------------------------------------------------------------------------------------------")
        println(thisSale.toString)
        //producerFunc(thisSale.toString)


        // Update Necessary Items
        orderId += 1
        currentDatePointer = currentDatePointer.plusDays(1)
      }
    }
    val endTime = LocalDateTime.now()
    println(count)
    //println(startTime +" : " + endTime)
    //println(presets("minRecordsPerDay"))
  }

  def trendCreator(): Unit = { // run a function to determine what trends need applied to the sale that is generated.
    //val trendList = List(List("customerData", "productData", "percentage1", "percentage 2",))

  }

  def selectQty(rangeList: List[List[Int]] = List(List(50, 30), List(35, 65), List(15, 100))): Int = {

    // set a top temp range to account for someone adding more than 100% in the list
    var topTmpRng = 0
    for(x <- 0 until rangeList.length){ topTmpRng = topTmpRng + rangeList(x)(0) }
    topTmpRng = topTmpRng + 1

    val tmpRng = Random.between(1,topTmpRng)
    var lowQty = 0
    var highQty = 0
    var rngStep = 0

    //println("//----------BREAK--------------//")

    for (x <- 0 until rangeList.length) {

      // preset range to initial value
      if ( x == 0 ) { rngStep = rangeList(x)(0)}

      //println(x + " : " + tmpRng + " : " + rangeList(x)(0) + " : " + highQty + " : " + rngStep )
      if (x == 0 && tmpRng <= rangeList(x)(0) ) {
        lowQty = 1
        highQty = rangeList(x)(1) + 1  // add one on high qty to make it include the top number
        //println(lowQty + " : " + highQty)
      }
      if (x != 0 && tmpRng <= rngStep && highQty == 0 ) {
        lowQty = rangeList(x-1)(1) + 1  // inclusive add one to step 1 past previous top number
        highQty = rangeList(x)(1) + 1 // exclusive top range, add one to include final top number
        //println(lowQty + " : " + highQty)
      }

      // if not the end of the list add next rngStep to previous
      if (x != rangeList.length -1 ){ rngStep = rngStep + rangeList(x+1)(0) }

    }

    //println(Random.between(lowQty,highQty))
    return Random.between(lowQty,highQty)

  }


// worker to set dates if defined
  def dateWorker(presets:Map[String,String], varName:String ):LocalDate ={
    if ( varName == "startDate" ) {
      val startDateList = presets("startDate").split(",").toList
      val startDate = LocalDate.of(startDateList(0).toInt, startDateList(1).toInt, startDateList(2).toInt)
      startDate
    }else if ( varName == "endDate"){
      val endDateList = presets("endDate").split(",").toList
      val endDate = LocalDate.of(endDateList(0).toInt, endDateList(1).toInt, endDateList(2).toInt)
      endDate
    }else {
      val startDateList = presets("startDate").split(",").toList
      val startDate = LocalDate.of(startDateList(0).toInt, startDateList(1).toInt, startDateList(2).toInt)
      val currentDate = startDate
      currentDate
    }
  }


  def producerFunc(stringToSend:String)
  {
    println("producer Ran")
    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "test_topic"

    try {
      val record1 = new ProducerRecord[String, String](topic,stringToSend)
      val metadata1 = producer.send(record1)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }
  }



}




