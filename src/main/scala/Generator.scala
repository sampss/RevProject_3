import java.sql.{Connection, DriverManager}
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}
//import scala.collection._
import scala.io._
import scala.language.postfixOps
import scala.util.matching.Regex
import java.sql.ResultSetMetaData
import java.text.{DateFormat, FieldPosition, ParsePosition, SimpleDateFormat}
import java.util.Date
import scala.collection.mutable._
import java.time._

object Generator {

  def main(args: Array[String]):Unit = {

    var presets = Map(
      "minNumSales" -> "122000",  // minimum number of records allowed
      "startDate" -> "1980,1,1", // start date of sales
      "endDate" -> "2000,1,1",   // end date of sales
      "minRecordsPerDay" -> ""   // min records per day - result of days / numberSales -- Auto adjusted based on minNumSales and Dates
    )

    // some default values
    // default path to current csv file
    val path = "C:\\Users\\samps\\IdeaProjects\\RevProject_3\\src\\resources\\"
    val path1 = path + "testCustomers.csv"
    val path2 = path + "testProducts.csv"

    getCSVFiles(presets, path1, path2)

  }

  def getCSVFiles(presets:Map[String,String], filePath1:String = "", filePath2:String = ""): Unit = {

    val customerFile = Source.fromFile(filePath1)
    val productFile = Source.fromFile(filePath2)
    // create column header for customers
    val customerColumnsSrc = customerFile.getLines.take(1).toList
    val customerColumns = customerColumnsSrc(0).split(",").toList  // create list of customer columns
    // create column header for products
    val productColumnsSrc = productFile.getLines.take(1).toList
    val productColumns = productColumnsSrc(0).split(",").toList  // create list of product columns
    // get remaining lines from csv to create data table
    val customerData = customerFile.getLines.toList
    val productData = productFile.getLines.toList
    // Build a list of lists as a table structure so I can randomly select information
    val customerValuesTable = buildList(customerData)
    val productValuesTable = buildList(productData)

    // handoff to build sales event function to build the sales event and pass each built sale to the producer
    createSaleEvent(presets, customerValuesTable, productValuesTable, customerColumns, productColumns ) // may change order of this in the future, to send to producer then producer func calls create sales event?

  }

  def buildList( initList:List[String] ):List[List[String]] = {

    val tempList = new ListBuffer[List[String]]()

    for ( x <- 0 until initList.length ) {
      val tempSubList = initList(x).split(",").toList
      tempList.append( tempSubList )
    }
    return tempList.toList
  }

  def createSaleEvent( presets:Map[String,String], customerData:List[List[String]], productData:List[List[String]], customerColumnHeader:List[String], productColumnHeader:List[String] ): Unit = {

    // setup default payment types
    val paymentTypes = List( "debit card", "credit card", "internet banking", "upi", "wallet" )
    // setup default failure reasons
    val failureReason = List( "Insufficient Funds", "Invalid Input", "Connection Error" )
    // setup default websites
    val websites = Map(
      "gogogrocery.com" -> "Fruits, Beverages, Baby Food, Vegetables, Cereal, Meat, Snacks ",
      "thehardstore.net" -> "Hardware, Household, Office Supplies, Clothes",
      "EGad-get.com" -> "Entertainment_Technology, Tech_Hardware, Software",
      "allgoods.net" -> "",
      "NailsNNails.com" -> "Hardware, Cosmetics, Personal Care, Clothes",
      "memawmall.net" -> "Clothes, Cosmetics, Household, Snacks, Personal Care",
      "onfice.com" -> "Tech_Hardware, Software, Office Supplies",
      "discodiscount.com" -> "Cosmetics, Clothes, Entertainment_Technology",
      "petesbigbutcher.net" -> "Meat, Clothes, Snacks",
      "vis4vegan.com" ->"Fruits, Beverages, Baby Food, Vegetables, Snacks"
    )
    val websiteNames = websites.keys.toList
    // set the start date to preset date if set, otherwise set date to current date
    val startDate = {
      if (presets("startDate") != ""){ dateWorker(presets, "startDate" ) } else { LocalDate.now() }
    }
    // set end date value, ' need to make if equal to or greater than start iggy and keep running '
    val endDate = {
      if (presets("endDate") != ""){ dateWorker(presets, "endDate" ) } else { LocalDate.now() }
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

    // temporary loop to see all moving parts just to create a list of 20 years data 3 sales per day
    for ( x <- 0 to salesPeriod ) {

      thisSale ++= "{\"order_id\":\"" + orderId + "\", "

      // ---------------------    This section may be moved to other function     -----------------------------

      // setup scala random utility
      val r = scala.util.Random
      // generate random customer
      val customer = customerData(r.nextInt(customerData.length))
      // generate random product
      val product = productData(r.nextInt(productData.length))
      // generate random failure rate - set failure rate to 20% in this example
      val failPass = if (r.nextInt(100) > 20){ "pass" }else{ "fail" }
      // setup basic failure reason
      val reasonFail = if (failPass == "fail"){failureReason(r.nextInt(failureReason.length))}else{ "" }
      // generate website ordered from at random
      val website = websiteNames(r.nextInt(websiteNames.length)).toString // store as string so can be used to call other info
      // setup transaction Id -- to be unique, used orderId and Date
      val paymentTxnId = s"${orderId}${currentDatePointer.toEpochDay}"

      println(website)
      //println(thisSale.toString)
      orderId += 1
      currentDatePointer = currentDatePointer.plusDays(1)
      //println(orderId)
    }


    //  Here I need to set up
    //  A math random select for customer
    //  A math random for product -- may vary based off circumstances.
    //  A math random for payment type -- also declare payment type variables
    //  A math random for fail or pass  -- may add higher percent based on country, customer, etc
    //  A math random with limitations on bad data for any field
    //  Math.random for records for a date... has to be at least a minimum of x can be more





  }

  def trendCreator():Unit = {  // run a function to determine what trends need applied to the sale that is generated.
      //val trendList = List(List("customerData", "productData", "percentage1", "percentage 2",))



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




}




