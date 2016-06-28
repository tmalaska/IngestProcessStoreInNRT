package com.cloudera.demo.ny_taxi

import java.text.SimpleDateFormat
import java.util.Date
import com.cloudera.demo.common.SolrSupport
import com.cloudera.demo.ny_taxi.pojo.{NyTaxiYellowTripBuilder, NyTaxiYellowTrip, NyTaxiYellowEntityBuilder, NyTaxiYellowEntity}
import kafka.serializer.StringDecoder
import NyTaxiYellowEntityBuilder.NyTaxiYellowEntityStateWrapper
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.kududb.client.{Operation, KuduClient}
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.local.KuduContext
import org.kududb.spark.local.KuduDStreamFunctions._

object NyTaxiYellowTripStreaming {

  val paymentTypeMap = Map("1" -> "Credit card",
  "2" -> "Cash",
  "3" -> "No charge",
  "4" -> "Dispute",
  "5" -> "Unknown",
  "6" -> "Voided trip")

  val rateCodeId = Map("1" -> "Standard rate",
  "2" -> "JFK",
  "3" -> "Newark",
  "4" -> "Nassau or Westchester",
  "5" -> "Negotiated fare",
  "6" -> "Group ride")

  def main(args:Array[String]):Unit = {

    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    val v:ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<zooKeper> " +
        "<kafkaTopicList> " +
        "<kuduMaster> " +
        "<TaxiEntityTableName> " +
        "<TaxiTripTableName> " +
        "<solrCollection> " +
        "<numberOfSeconds> " +
        "<pushToKudu> " +
        "<pushToSolR>" +
        "<checkpointDir>" +
        "<runLocal>")
      return
    }

    val kafkaBrokerList = args(0)
    val zk = args(1)
    val kafkaTopicList = args(2)
    val kuduMaster = args(3)
    val taxiEntityTableName = args(4)
    val taxiTripTableName = args(5)
    val solrCollection = args(6)
    val numberOfSeconds = args(7).toInt
    val pushToKudu = args(8).equalsIgnoreCase("t")
    val pushToSolR = args(9).equalsIgnoreCase("t")
    val checkpointDirectory = args(10)
    val runLocal = args(11).equals("l")

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("zk:" + zk)
    println("kafkaTopicList:" + kafkaTopicList)
    println("kuduMaster:" + kuduMaster)
    println("taxiEntityTableName:" + taxiEntityTableName)
    println("taxiTripTableName:" + taxiTripTableName)
    println("solrCollection:" + solrCollection)
    println("numberOfSeconds:" + numberOfSeconds)
    println("pushToKudu:" + pushToKudu)
    println("pushToSolR:" + pushToSolR)
    println("checkpointDirectory:" + checkpointDirectory)
    println("runLocal:" + runLocal)

    val sc:SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("NyTaxiYellowTripStreaming")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))
    val kuduContext = new KuduContext(sc, kuduMaster)
    val sqlContext = new SQLContext(sc)

    println("--Loading Entities from Kudu")

    val starterEntities = loadOriginalKuduProfilesData(taxiEntityTableName,
      kuduMaster,
      ssc,
      sqlContext)


    // Get new readings data from Kafka
    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val tripDStream = messageStream.map(r => {
      (r._1, r._2.split(","))
    }).filter(r => r._2.size > 3).map(r => {
      (r._1, NyTaxiYellowTripBuilder.build(r._2))
    })

    tripDStream.print(10)

    // Push trip information to trip table
    if (pushToKudu) {
      println("--Adding Kudu Push trip")
      tripDStream.kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
        sendTripToKudu(taxiTripTableName, it, kuduClient)
      })
    }

    val snapshotTripEntityDStream = tripDStream.map(r => {
      (r._1, NyTaxiYellowEntityBuilder.build(r._2))
    })

    // Compute aggregates of taxi vender to kudu
    if (pushToKudu) {
      println("--Adding Kudu Push entity")
      tripDStream.updateStateByKey[NyTaxiYellowEntityStateWrapper](
        (newTrips: Seq[NyTaxiYellowTrip],
         taxiEntityWrapperOp: Option[NyTaxiYellowEntityStateWrapper]) => {

        val taxiEntityWrapper = taxiEntityWrapperOp.getOrElse(new NyTaxiYellowEntityStateWrapper)
        var newTaxiEntity = taxiEntityWrapper.entity
        newTrips.foreach(trip => {
          newTaxiEntity = newTaxiEntity + trip
        })

        val newState = if (taxiEntityWrapper.state.equals("Blank")) {
          "New"
        } else if (newTrips.length > 0) {
          "Modified"
        } else {
          "Untouched"
        }

        Option(new NyTaxiYellowEntityStateWrapper(newState, newTaxiEntity))
      }).kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
        sendEntityToKudu(taxiEntityTableName, it, kuduClient)
      })
    }

    // Push trip information to SolR
    if (pushToSolR) {
      val solrDocumentDStream = tripDStream.map(convertToSolRDocuments)

      // Commit the SolrInputDocument DStream
      SolrSupport.indexDStreamOfDocs(zk,
        solrCollection,
        100,
        JavaDStream.fromDStream(solrDocumentDStream))
      //TODO make batch size configurable
    }



    println("--Starting Spark Streaming")
    ssc.checkpoint(checkpointDirectory)
    ssc.start()
    ssc.awaitTermination()
  }



  def convertToSolRDocuments(tripTuple: (String, NyTaxiYellowTrip)): SolrInputDocument = {
    val trip = tripTuple._2

    //2014-10-01T00:00:00Z
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    val doc: SolrInputDocument = new SolrInputDocument
    doc.addField("id", trip.vender_id + "," + trip.tpep_pickup_datetime)
    doc.addField("vender_id", trip.vender_id)
    doc.addField("tpep_pickup_datetime", dateFormat.format(new Date(trip.tpep_pickup_datetime)))
    doc.addField("tpep_dropoff_datetime", dateFormat.format(new Date(trip.tpep_dropoff_datetime)))
    doc.addField("passenger_count", trip.passenger_count)
    doc.addField("trip_distance", trip.trip_distance)
    doc.addField("pickup_longitude", trip.pickup_longitude)
    doc.addField("pickup_latitude", trip.pickup_latitude)
    doc.addField("dropoff_longitude", trip.dropoff_longitude)
    doc.addField("dropoff_latitude", trip.dropoff_latitude)
    doc.addField("payment_type", trip.payment_type)
    doc.addField("fare_amount", trip.fare_amount)
    doc.addField("extra", trip.extra)
    doc.addField("mta_tax", trip.mta_tax)
    doc.addField("improvement_surcharge", trip.improvement_surcharge)
    doc.addField("tip_amount", trip.tip_amount)
    doc.addField("tolls_amount", trip.tolls_amount)
    doc.addField("total_amount", trip.total_amount)

    doc
  }

  def sendEntityToKudu(taxiEntityTableName: String, it: Iterator[(String, NyTaxiYellowEntityStateWrapper)], kuduClient: KuduClient): Unit = {
    val table = kuduClient.openTable(taxiEntityTableName)
    val session = kuduClient.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    it.foreach(r => {
      val state = r._2.state
      val entity = r._2.entity

      val operation: Operation = if (state.equals("New")) {
        table.newInsert()
      } else if (state.equals("Modified")) {
        table.newUpdate()
      } else {
        null
      }

      if (operation != null) {
        val row = operation.getRow()

        row.addString("vender_id", entity.vender_id)
        row.addInt("total_trips", entity.total_trips)
        row.addInt("total_passengers", entity.total_passengers)
        row.addDouble("total_distance_of_trips", entity.total_distance_of_trips)
        row.addDouble("max_distance_of_trip", entity.max_distance_of_trip)
        row.addDouble("total_credit_card_fare_amount", entity.total_credit_card_fare_amount)
        row.addDouble("total_create_card_extra", entity.total_create_card_extra)
        row.addDouble("total_credit_card_mta_tax", entity.total_credit_card_mta_tax)
        row.addDouble("total_credit_card_impr_surcharge", entity.total_credit_card_impr_surcharge)
        row.addDouble("total_credit_card_tip_amount", entity.total_credit_card_tip_amount)
        row.addDouble("total_credit_card_tolls_amount", entity.total_credit_card_tolls_amount)
        row.addDouble("total_credit_card_total_amount", entity.total_credit_card_total_amount)
        row.addDouble("total_cash_fare_amount", entity.total_cash_fare_amount)
        row.addDouble("total_cash_extra", entity.total_cash_extra)
        row.addDouble("total_cash_mta_tax", entity.total_cash_mta_tax)
        row.addDouble("total_cash_impr_surcharge", entity.total_cash_impr_surcharge)
        row.addDouble("total_cash_tip_amount", entity.total_cash_tip_amount)
        row.addDouble("total_cash_tolls_amount", entity.total_cash_tolls_amount)
        row.addDouble("total_cash_total_amount", entity.total_cash_total_amount)
        row.addInt("total_credit_card_trips", entity.total_credit_card_trips)
        row.addInt("total_cash_trips", entity.total_cash_trips)
        row.addInt("total_no_charge_trips", entity.total_no_charge_trips)
        row.addInt("total_dispute_trips", entity.total_dispute_trips)
        row.addInt("total_unknown_trips", entity.total_unknown_trips)
        row.addInt("total_voided_trips", entity.total_voided_trips)

        session.apply(operation)
      }

    })
    session.flush()
    session.close()
  }

  def sendTripToKudu(taxiTripTableName: String, it: Iterator[(String, NyTaxiYellowTrip)], kuduClient: KuduClient): Unit = {
    val table = kuduClient.openTable(taxiTripTableName)
    val session = kuduClient.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    it.foreach(r => {
      val trip = r._2
      val operation = table.newInsert()
      val row = operation.getRow()

      row.addString("vender_id", trip.vender_id)
      row.addLong("tpep_pickup_datetime", trip.tpep_pickup_datetime)
      row.addLong("tpep_dropoff_datetime", trip.tpep_dropoff_datetime)
      row.addInt("passenger_count", trip.passenger_count)
      row.addDouble("trip_distance", trip.trip_distance)
      row.addDouble("pickup_longitude", trip.pickup_longitude)
      row.addDouble("pickup_latitude", trip.pickup_latitude)
      row.addString("rate_code_id", rateCodeId.getOrElse(trip.rate_code_id, "N/A"))
      row.addString("store_and_fwd_flag", trip.store_and_fwd_flag)
      row.addDouble("dropoff_longitude", trip.dropoff_longitude)
      row.addDouble("dropoff_latitude", trip.dropoff_latitude)
      row.addString("payment_type", trip.payment_type)
      row.addDouble("fare_amount", trip.fare_amount)
      row.addDouble("extra", trip.extra)
      row.addDouble("mta_tax", trip.mta_tax)
      row.addDouble("improvement_surcharge", trip.improvement_surcharge)
      row.addDouble("tip_amount", trip.tip_amount)
      row.addDouble("tolls_amount", trip.tolls_amount)
      row.addDouble("total_amount", trip.total_amount)

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

  def loadOriginalKuduProfilesData(tableName:String,
                                   kuduMaster:String,
                                   ssc:StreamingContext,
                                   sqlContext:SQLContext
                                    ):RDD[(String, NyTaxiYellowEntity)] = {

    val kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> kuduMaster)

    sqlContext.read.options(kuduOptions).format("org.kududb.spark.kudu").load.
      registerTempTable("ny_taxi")

    println("collect from ny_taxi")
    sqlContext.sql("select * from ny_taxi").map( r => {
      val entity = NyTaxiYellowEntityBuilder.build(r)
      (entity.vender_id, entity)
    }).take(100).foreach(println)
    println("Done from ny_taxi")

    return sqlContext.sql("select * from ny_taxi").map( r => {
      val entity = NyTaxiYellowEntityBuilder.build(r)
      (entity.vender_id, entity)
    })
  }
}
