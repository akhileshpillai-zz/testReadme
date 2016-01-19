package kafkaStream

/**
  * Created by akhileshpillai on 1/4/16.
  */

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import kafka.serializer.{DefaultDecoder, StringDecoder}
import net.liftweb.json.{JValue, parse}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable.ListBuffer

object JsonHBaseAppWithArgs {

  object Util extends Logging {
    def setStreamingLogLevels() {
      // Set reasonable logging levels for streaming if the user has not configured log4j.
      val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4jInitialized) {
        // Force initialization.
        logInfo("Setting log level to [WARN] for streaming example." +
          " To override add a custom log4j.properties to the classpath.")
        Logger.getRootLogger.setLevel(Level.WARN)
      }
    }
  }

  def getHbaseConfig():Configuration = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf
  }

  def createTable(conf: Configuration, tableName: String, columnFamilies: String*) {
    val conn: Connection =ConnectionFactory.createConnection(conf);
    val admin: Admin = conn.getAdmin()
    // val admin: HBaseAdmin = new HBaseAdmin(conf)
    val descriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    for (columnFamily <- columnFamilies) {
      descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(columnFamily)))
    }
    if (!admin.tableExists(TableName.valueOf(tableName))) admin.createTable(descriptor)
    admin.close

  }

  def deleteTable(conf: Configuration, tableName: String) {
    val conn: Connection =ConnectionFactory.createConnection(conf);
    val admin: Admin = conn.getAdmin()
    admin.disableTable(TableName.valueOf(tableName))
    val isDisabled: Boolean = admin.isTableDisabled(TableName.valueOf(tableName))
    if (true == isDisabled) {
      admin.deleteTable(TableName.valueOf(tableName))
    }
    else {
      throw new RuntimeException("Disable failed")
    }
    admin.close
  }

  def queryHBase(htable: String) = {

    val CF = Bytes.toBytes("htt")
    val hbaseconf: Configuration = getHbaseConfig()
    val connection = ConnectionFactory.createConnection(hbaseconf)
    try {

      val table = connection.getTable(TableName.valueOf(htable));
      try {
        val s: Scan = new Scan();
        s.setCaching(10)
        val (prk: String, col: String) = getTsForRowKey()
        val rowkey = prk + ":"
        s.setRowPrefixFilter(Bytes.toBytes(rowkey))
        val scanner: ResultScanner  = table.getScanner(s);
        try {
          printtable(table,CF,scanner)

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

  }

  def putHBase(toplist: Array[(Int,String)], htable: String) = {

    // Set the column family name
    val CF = Bytes.toBytes("htt")
    val hbaseconf: Configuration = getHbaseConfig()
    val connection = ConnectionFactory.createConnection(hbaseconf)

    //Create Hbase table
    createTable(hbaseconf, htable, "htt")
    try {

      val table = connection.getTable(TableName.valueOf(htable));
      try {
        toplist.foreach { case (count, tag) => println("%s (%s)".format(tag, count))
          val (prk: String, col: String) = getTsForRowKey()
          val rowkey = prk + ":" + tag
          val p: Put = new Put(Bytes.toBytes(rowkey))
          p.addColumn(CF, Bytes.toBytes(col), Bytes.toBytes(count))
          table.put(p)
        }
        // Close your table and cluster connection.
      } finally {

        if (table != null) table.close();
      }
    } finally {

      connection.close();
    }

  }





  def printtable(table: Table, CF: Array[Byte],scanner: ResultScanner) = {
    import scala.collection.JavaConversions._
    var aggregate: Float = 0
    for (result <- scanner) {
      val symbol: String = Bytes.toString(result.getRow)
      val valueAdj: util.NavigableMap[Array[Byte], Array[Byte]] = result.getFamilyMap(CF)
      var countPerTag: Int = 0
      var hashTag: String = ""
      for ((key, value) <- valueAdj) {
        countPerTag = countPerTag + Bytes.toInt(value)
        hashTag = Bytes.toString(key)
        println(symbol + ":" + Bytes.toString(key) + "-->" + Bytes.toInt(value))
      }
    }
  }



  def getTsForRowKey() = {
    val curDate = new Date()
    val format1 = new SimpleDateFormat("yyyy/MM/dd:H")
    val format2 = new SimpleDateFormat("m")
    (format1.format(curDate).toString(),format2.format(curDate).toString())
  }

  def main(args:Array[String]) = {


    if (args.length < 1) {
      System.err.println("Usage: TwitterPopularTags <HbaseTable>")
      System.exit(1)
    }

    // Disable chatty logging.
    Util.setStreamingLogLevels()

    // Create Spark context.
    val conf = new SparkConf().
      setAppName("MFStream")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext

    // Brokers.
    val brokers = Set("ip-10-11-191-179.ec2.internal:6667")
    val kafkaParams = Map("metadata.broker.list" -> brokers.mkString(","))

    // Topics.
    val topics = Set("tweettopic")

    // Create DStream.
    val kafkaStream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    // Parse the tweet (Json) and extract required field
    val hashTags = kafkaStream.map{ case (_, json) => {
      val json1:net.liftweb.json.JValue = parse(json);
      val elements:net.liftweb.json.JsonAST.JValue = (json1 \\ "entities" \\ "hashtags" \\"text");
      var htags = new ListBuffer[String]();
      for (i<-elements.children) {
        val st = (i.asInstanceOf[net.liftweb.json.JValue] \\ "text") ;
        htags += st.asInstanceOf[JValue].values.toString;
      }
      htags.toSet.toList
    }
    }


    val hashCount = hashTags.flatMap(x=>x.filter(_.length > 0)).map(x=>(x.toLowerCase,1))

    // Create and reduce by sliding window of 2min duration
    val hcByWindow = hashCount.reduceByKeyAndWindow({ (a: Int, b: Int) => a + b }, Seconds(120), Seconds(120))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    hcByWindow.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      putHBase(topList,args(0))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
