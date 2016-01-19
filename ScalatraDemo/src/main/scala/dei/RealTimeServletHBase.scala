package dei

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.filter.FuzzyRowFilter
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Pair
import CompareFilter.CompareOp
import collection.JavaConverters._

import org.json4s.{DefaultFormats, Formats, JValue}
import org.scalatra.ScalatraServlet
import org.scalatra.json._

import scala.collection.mutable.ListBuffer
import scala.util.Random

class RealTimeServletHBase extends ScalatraServlet with JacksonJsonSupport {
  protected implicit val jsonFormats: Formats = DefaultFormats

  case class DataPoint(dataKey:String,dataValue:Int)

  before() {
    contentType = formats("json")
  }

  def parseDouble(value:Option[String],defaultValue:Double):Double = {
    try {
      value.get.toDouble
    }
    catch {
      case e:Exception => defaultValue
    }
  }

  def parseInt(value:Option[String],defaultValue:Int):Int = {
    parseDouble(value, defaultValue).toInt
  }

  get("/data/data1.json") {
    getHbaseData()
  }

  def getHbaseData() = {
    queryHBase()
  }

  def getData3() = {
    val keyCount = parseInt(params.get("keyCount"), 20)
    val minValue = parseDouble(params.get("minValue"), 0)
    val maxValue = parseDouble(params.get("maxValue"), 100)

    Range(1, keyCount + 1).
      map(_.toString()).
      map(k => {
        val dataKey = k
        val dataValue = minValue + (Random.nextDouble) * (maxValue - minValue)
        //DataPoint(dataKey, dataValue)
      })
  }

  def getData1() = {

    val dataWords = Array("depression", "weather","alcohol", "sourbread", "cobol","job","trump","guns","sickness","sunlight")
    val max = dataWords.length
    var index:Int = 0
    val minValue = 20
    val maxValue = 999
    Range(1,max).
      map(key => {

        index = Random.nextInt(max)

        println("index : " + index + "datawords.length: " + max)

        dataWords(index)}).map(k=>
        {
          val value = minValue + Random.nextInt(maxValue - minValue);
          DataPoint(k, value)
        }
        )
  }

  def getData2() = {

  }


  //def getHbaseConfig(): Configuration = ???

  def getHbaseConfig():Configuration = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf
  }


  def queryHBase(): ListBuffer[DataPoint] = {
    val hbaseTableName: String = getServletContext.getAttribute("htable").toString()
    println("The table name : " + hbaseTableName)
    var hashtagstats = new ListBuffer[DataPoint]()
    val CF = Bytes.toBytes("htt")
    //Configuration config = HBaseConfiguration.create();
    //Connection connection = ConnectionFactory.createConnection(config);
    val hbaseconf: Configuration = getHbaseConfig()
    val connection = ConnectionFactory.createConnection(hbaseconf)
    try {

      //val table = connection.getTable(TableName.valueOf("ht_tweets"));
      val table = connection.getTable(TableName.valueOf(hbaseTableName));
      try {
        // Sometimes, you won't know the row you're looking for. In this case, you
        // use a Scanner. This will give you cursor-like interface to the contents
        // of the table.  To set up a Scanner, do like you did above making a Put
        // and a Get, create a Scan.  Adorn it with column names, etc.
        val s: Scan = new Scan();
        s.setCaching(10)
        val (prk: String, col: String) = getTsForRowKey()
        val rowkey = prk + ":"
        s.setRowPrefixFilter(Bytes.toBytes(rowkey))

        //s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
        val scanner: ResultScanner  = table.getScanner(s);
        try {
          hashtagstats = printtable(table,CF,scanner)

        } finally {

          scanner.close();
        }
        // Close your table and cluster connection.
      } finally {

        if (table != null) table.close();
      }
    } finally {

      connection.close();
    }



    hashtagstats

  }

  def printtagTrend(table: Table, CF: Array[Byte],scanner: ResultScanner): ListBuffer[DataPoint] = {
    import scala.collection.JavaConversions._
    val hashTrend: ListBuffer[DataPoint] = new ListBuffer[DataPoint]()
    var countPerTag: Int = 0
    val wordTagged = ""
    for (result <- scanner) {
      val wordTagged: String = Bytes.toString(result.getRow)
      val valueAdj: util.NavigableMap[Array[Byte], Array[Byte]] = result.getFamilyMap(CF)
      var hashTag: String = ""
      for ((key, value) <- valueAdj) {
        countPerTag = countPerTag + Bytes.toInt(value)
      }
    }
    hashTrend += DataPoint(wordTagged.split(":")(0).split("/")(2), countPerTag)
    hashTrend
  }




   def printtable(table: Table, CF: Array[Byte],scanner: ResultScanner): ListBuffer[DataPoint] = {
    import scala.collection.JavaConversions._
     val hashCount: ListBuffer[DataPoint] = new ListBuffer[DataPoint]()
    for (result <- scanner) {
      val symbol: String = Bytes.toString(result.getRow)
      val valueAdj: util.NavigableMap[Array[Byte], Array[Byte]] = result.getFamilyMap(CF)
      var countPerTag: Int = 0
      var hashTag: String = ""
      for ((key, value) <- valueAdj) {
        countPerTag = countPerTag + Bytes.toInt(value)
        hashTag = Bytes.toString(key)
        //println(symbol + ":" + Bytes.toString(key) +"-->" + Bytes.toInt(value))}
      }
      hashCount += DataPoint(symbol.split(":")(2), countPerTag)
    }
    hashCount
  }

  def getTsForRowKey() = {
    val curDate = new Date()
    val format1 = new SimpleDateFormat("yyyy/MM/dd:H")
    val format2 = new SimpleDateFormat("m")
    (format1.format(curDate).toString(),format2.format(curDate).toString())
  }

  override def render(value: JValue)(implicit formats: Formats): JValue =
    value.camelizeKeys
}

