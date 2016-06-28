package com.cloudera.demo.fed_highway_admin.basic_safety_message

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.cloudera.demo.common.SolrSupport
import com.cloudera.demo.fed_highway_admin.basic_safety_message.pojo.{TripSummary, TripSummaryBuilder}
import kafka.serializer.StringDecoder
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.cloud.ZooKeeperException
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

object TripSummaryStreaming {

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

    val pojoDStream:DStream[TripSummary] = messageStream.map(r => TripSummaryBuilder.build(r._2.split(",")))

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

  def convertToSolRDocuments(trip: TripSummary): SolrInputDocument = {
    //2014-10-01T00:00:00Z
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    val doc: SolrInputDocument = new SolrInputDocument
    doc.addField("id", trip.device_id + "," + trip.trip_id + "," + trip.epoch_start_time)
    doc.addField("device_id", trip.device_id)
    doc.addField("trip_id", trip.trip_id)
    doc.addField("epoch_start_time", dateFormat.format(new Date(trip.epoch_start_time)))
    val startTime = Calendar.getInstance()
    startTime.setTimeInMillis(trip.epoch_start_time)
    doc.addField("start_day_of_the_week", dayOfWeekMap.getOrElse(startTime.get(Calendar.DAY_OF_WEEK), "N/A"))
    doc.addField("start_hour_of_day", startTime.get(Calendar.HOUR_OF_DAY))
    doc.addField("epoch_end_time", dateFormat.format(new Date(trip.epoch_start_time)))
    doc.addField("total_trip_distance", trip.total_trip_distance)
    doc.addField("dis_travelled_plus_25_mph", trip.dis_travelled_plus_25_mph)
    doc.addField("trip_duration", trip.trip_duration)
    doc.addField("avg_speed", trip.avg_speed)
    doc.addField("max_speed", trip.max_speed)
    doc.addField("break_count", trip.break_count)
    doc.addField("wider_activity", trip.wiper_activity)

    doc
  }

  def sendTripSummaryToKudu(tripSummaryTableName: String, it: Iterator[TripSummary], kuduClient: KuduClient): Unit = {
    val table = kuduClient.openTable(tripSummaryTableName)
    val session = kuduClient.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    it.foreach(trip => {

      val operation = table.newInsert()
      val row = operation.getRow()

      row.addLong("device_id", trip.device_id)
      row.addString("trip_id", trip.trip_id)
      row.addLong("epoch_start_time", trip.epoch_start_time)
      val startTime = Calendar.getInstance()
      startTime.setTimeInMillis(trip.epoch_start_time)
      row.addString("start_day_of_week", dayOfWeekMap.getOrElse(startTime.get(Calendar.DAY_OF_WEEK), "N/A"))
      row.addInt("start_hour_or_day", startTime.get(Calendar.HOUR_OF_DAY))
      row.addLong("epoch_end_time", trip.epoch_end_time)
      row.addDouble("total_trip_distance", trip.total_trip_distance)
      row.addDouble("dis_travelled_plus_25_mph", trip.dis_travelled_plus_25_mph)
      row.addDouble("trip_duration", trip.trip_duration)
      row.addDouble("avg_speed", trip.avg_speed)
      row.addDouble("max_speed", trip.max_speed)
      row.addInt("break_count", trip.break_count)
      row.addString("wiper_activity", trip.wiper_activity)
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
