package com.cloudera.demo.fed_highway_admin.basic_safety_message

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.cloudera.demo.common.SolrSupport
import com.cloudera.demo.fed_highway_admin.basic_safety_message.pojo._
import kafka.serializer.StringDecoder
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.kududb.client.KuduClient
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.local.KuduContext
import org.kududb.spark.local.KuduDStreamFunctions._

class p1Streaming {

  val dayOfWeekMap = Map(0 -> "Sun",
    1 -> "Mon",
    2 -> "Tues",
    3 -> "Weds",
    4 -> "Thurs",
    5 -> "Fri",
    6 -> "Sat")

  def main(args:Array[String]):Unit = {


    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<zooKeeper> " +
        "<kafkaTopicList> " +
        "<kuduMaster> " +
        "<tripSummaryTable> " +
        "<tripSummerySolrCollection> " +
        "<numberOfSeconds> " +
        "<pushToKudu> " +
        "<pushToSolR>")
      return
    }

    val kafkaBrokerList = args(0)
    val zk = args(1)
    val kafkaTopicList = args(2)
    val kuduMaster = args(3)
    val tripSummaryTableName = args(4)
    val solrCollection = args(6)
    val numberOfSeconds = args(7).toInt
    val pushToKudu = args(8).equalsIgnoreCase("t")
    val pushToSolR = args(9).equalsIgnoreCase("t")
    val checkpointDirectory = args(10)

    val sparkConf = new SparkConf().setAppName("NyTaxiYellowTripStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))
    val kuduContext = new KuduContext(sc, kuduMaster)
    val sqlContext = new SQLContext(sc)

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val pojoDStream:DStream[P1] = messageStream.map(r => P1Builder.build(r._2.split(",")))

    if (pushToKudu) {
      pojoDStream.kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
        sendTripSummaryToKudu(tripSummaryTableName, it, kuduClient)
      })
    }

    if (pushToSolR) {
      val solrDocumentDStream = pojoDStream.map(convertToSolRDocuments)

      // Commit the SolrInputDocument DStream
      SolrSupport.indexDStreamOfDocs(zk,
        solrCollection,
        100,
        JavaDStream.fromDStream(solrDocumentDStream))
    }
  }

  def convertToSolRDocuments(p1: P1): SolrInputDocument = {
    //2014-10-01T00:00:00Z
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    val doc: SolrInputDocument = new SolrInputDocument
    doc.addField("id", p1.rx_device + " " + p1.file_id + " " + p1.gen_time)
    doc.addField("rx_device", p1.rx_device)
    doc.addField("file_id", p1.file_id)
    doc.addField("tx_device", p1.tx_device)
    doc.addField("gen_time", dateFormat.format(new Date(p1.gen_time)))
    val startTime = Calendar.getInstance()
    startTime.setTimeInMillis(p1.gen_time)
    doc.addField("day_of_week", dayOfWeekMap.getOrElse(startTime.get(Calendar.DAY_OF_WEEK), "N/A"))
    doc.addField("hour_of_day", startTime.get(Calendar.HOUR_OF_DAY))
    doc.addField("tx_random", p1.tx_random)
    doc.addField("msg_count", p1.msg_count)
    doc.addField("d_second", p1.d_second)
    doc.addField("latitude", p1.latitude)
    doc.addField("longitude", p1.longitude)
    doc.addField("elevation", p1.elevation)
    doc.addField("heading", p1.heading)
    doc.addField("a_x", p1.a_x)
    doc.addField("a_y", p1.a_y)
    doc.addField("a_z", p1.a_z)
    doc.addField("a_a", p1.a_a)
    doc.addField("path_count", p1.path_count)
    doc.addField("radius_of_curve", p1.radius_of_curve)
    doc.addField("confidence", p1.confidence)

    doc
  }

  def sendTripSummaryToKudu(tripSummaryTableName: String, it: Iterator[P1], kuduClient: KuduClient): Unit = {
    val table = kuduClient.openTable(tripSummaryTableName)
    val session = kuduClient.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    it.foreach(p1 => {

      val operation = table.newInsert()
      val row = operation.getRow()

      row.addLong("rx_device", p1.rx_device)
      row.addLong("file_id", p1.file_id)
      row.addLong("tx_device", p1.tx_device)
      row.addLong("gen_time", p1.gen_time)
      val startTime = Calendar.getInstance()
      startTime.setTimeInMillis(p1.gen_time)
      row.addString("day_of_week", dayOfWeekMap.getOrElse(startTime.get(Calendar.DAY_OF_WEEK), "N/A"))
      row.addInt("hour_of_day", startTime.get(Calendar.HOUR_OF_DAY))
      row.addLong("tx_random", p1.tx_random)
      row.addLong("msg_count", p1.msg_count)
      row.addInt("d_second", p1.d_second)
      row.addDouble("latitude", p1.latitude)
      row.addDouble("longitude", p1.longitude)
      row.addDouble("elevation", p1.elevation)
      row.addDouble("heading", p1.heading)
      row.addDouble("a_x", p1.a_x)
      row.addDouble("a_y", p1.a_y)
      row.addDouble("a_z", p1.a_z)
      row.addDouble("a_a", p1.a_a)
      row.addInt("path_count", p1.path_count)
      row.addDouble("radius_of_curve", p1.radius_of_curve)
      row.addInt("confidence", p1.confidence)

      try {
        session.apply(operation)
      } catch {
        case e: Exception => {
          //nothing
        }
      }
    })
    try {
      session.flush()
      session.close()
    } catch {
      case e: Exception => {
        //nothing
      }
    }
  }
}
