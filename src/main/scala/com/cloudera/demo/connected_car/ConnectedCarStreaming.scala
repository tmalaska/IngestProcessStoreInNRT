package com.cloudera.demo.connected_car

//import com.lucidworks.spark.SolrSupport
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.SolrException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.joda.time.DateTime
import org.joda.time.format._
import org.kududb.client.Operation
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.local.{KuduContext}
import org.kududb.spark.local.KuduDStreamFunctions._

object ConnectedCarStreaming {
  /*
  Steps:
    1. Load values from Kudu and create an RDD with key=vin, value=ConnectedCarProfile objects
    2. Get DStream of Strings from Kafka with key=vin, value=full connected car event string
    3. Call updateStateByKey on Kafka DStream, passing in the Kudu RDD as the initial value (see:SPARK-3660)
    4. Write results back to Kudu
    5. Filter Kafka events for collisions and hazards, join with vehicle profile data, convert to SolrInputDocuments, and commit to Solr.
  */
  def main(args:Array[String]):Unit = {
    if (args.length == 0) {
      println("Args: <Kafka broker list> <kudu master> <zk> <kafka topic name to read from>")
      return
    }

    val kafkaBrokerList = args(0)
    //val kafkaTopicName = "connected_car_streaming"
    //val kafkaTopicName = "connected_car_readings"
    val kafkaTopicName = args(3)
    val kuduProfilesTableName = "connected_car_profiles"
    val kuduMaster = args(1)
    val zk = args(2)
    val solrCollection = "car-event-collection"

    val sparkConf = new SparkConf().setAppName("ConnectedCarStreaming")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    val kuduContext = new KuduContext(streamingContext.sparkContext, kuduMaster)

    // Get existing profiles from Kudu
    val originalKuduProfilesDStream = loadOriginalKuduProfilesData(kuduProfilesTableName, kuduContext, streamingContext)

    // Get new readings data from Kafka
    val kafkaDStream = getDataFromKafka(kafkaTopicName, kafkaBrokerList, streamingContext)

    // Update state with new readings data
    val currentStateDStream = kafkaDStream.updateStateByKey[ConnectedCarProfile](
      (newReadings:Seq[String], profile:Option[ConnectedCarProfile]) => {
        val it = newReadings.iterator
        if (!it.hasNext) {
          if (!profile.isEmpty) {
            val existingProfile = profile.get

            existingProfile.hasChanged = false

            Some(existingProfile)
          } else {
            None
          }
        } else {
          if (profile.isEmpty) { // We don't yet have a profile for this vin, so create one and add the readings
          val newProfile = new ConnectedCarProfile()

            while (it.hasNext) {
              val newReading = it.next()
              newProfile.addReading(newReading)
            }

            newProfile.isInsert = true
            newProfile.hasChanged = true

            Some(newProfile)
          } else { // We already have a profile for this vin, so add the new readings to that
          val existingProfile = profile.get

            while (it.hasNext) {
              val newReading = it.next()
              existingProfile.addReading(newReading)
            }

            existingProfile.isInsert = false
            existingProfile.hasChanged = true

            Some(existingProfile)
          }
        }
      }, new HashPartitioner(streamingContext.sparkContext.defaultParallelism), originalKuduProfilesDStream)

    // Write data to Kudu
    currentStateDStream.kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
      val table = kuduClient.openTable(kuduProfilesTableName)

      // TODO: This can be made to be faster?
      val session = kuduClient.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

      var operation:Operation = null

      while (it.hasNext) {
        val connectedCarProfileTuple = it.next()

        if (connectedCarProfileTuple._2.hasChanged) {
          if (connectedCarProfileTuple._2.isInsert) {
            operation = table.newInsert()
          } else {
            operation = table.newUpdate()
          }

          val row = operation.getRow()

          row.addString("vin", connectedCarProfileTuple._2.vin)
          row.addDouble("acceleration_aggression_score", connectedCarProfileTuple._2.accelerationAggressionScore)
          row.addDouble("braking_aggression_score", connectedCarProfileTuple._2.brakingAggressionScore)
          row.addDouble("handling_aggression_score", connectedCarProfileTuple._2.handlingAggressionScore)
          row.addDouble("overall_aggression_score", connectedCarProfileTuple._2.overallAggressionScore)
          row.addInt("oil_replacement_period", connectedCarProfileTuple._2.oilReplacementPeriod)
          row.addInt("brake_replacement_period", connectedCarProfileTuple._2.brakeReplacementPeriod)
          row.addInt("tire_replacement_period", connectedCarProfileTuple._2.tireReplacementPeriod)
          row.addLong("miles_count", connectedCarProfileTuple._2.milesCount)
          row.addLong("brakes_applied_count", connectedCarProfileTuple._2.brakesAppliedCount)
          row.addInt("average_speed", connectedCarProfileTuple._2.averageSpeed)
          row.addInt("illegal_lane_departure_plus_minus", connectedCarProfileTuple._2.illegalLaneDeparturePlusMinus)
          row.addInt("collisions_count", connectedCarProfileTuple._2.collisionsCount)
          row.addInt("hazards_detected_count", connectedCarProfileTuple._2.hazardsDetectedCount)
          row.addLong("readings_count", connectedCarProfileTuple._2.readingsCount)
          row.addLong("accelerating_readings_count", connectedCarProfileTuple._2.acceleratingReadingsCount)
          row.addLong("decelerating_readings_count", connectedCarProfileTuple._2.deceleratingReadingsCount)
          row.addLong("last_updated", connectedCarProfileTuple._2.lastUpdated)

          session.apply(operation)
        }
      }

      session.close()
    })
    /*
        // Create an RDD of SolrInputDocuments for every event with a collision or hazard detected
        val solrDocumentDStream:DStream[SolrInputDocument] = kafkaDStream
                                        .filter(readingString => {
                                          val values = readingString._2.split(",")
                                          values(10).equals("true") || values(11).equals("true") || (values(9).equals("true") && values(8).equals("false"))
                                        })
                                        .map(readingString => {
                                          val values = readingString._2.split(",")
                                          val doc:SolrInputDocument = new SolrInputDocument
                                          doc.addField("id", values(1) + "-" + values(0))
                                          doc.addField("vin", values(1))
                                          doc.addField("speed", values(6).toInt)
                                          val dt = new DateTime(values(0).toLong)
                                          //val dtf = ISODateTimeFormat.dateTime()
                                          val dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                                          doc.addField("datetime", dtf.print(dt)) // String in Solr
                                          doc.addField("timestamp", dtf.print(dt)) // conveted to tdate in Solr
                                          doc.addField("collisionDetected", values(10).toBoolean)
                                          doc.addField("hazardDetected", values(11).toBoolean)
                                          // Determine event type TODO: enable multivalued
                                          var eventType:String = null
                                          if (values(10).equals("true")) {
                                            eventType = "collision"
                                          } else if (values(11).equals("true")) {
                                            eventType = "hazard"
                                          } else if (values(9).equals("true") && values(8).equals("false")) {
                                            eventType = "illegal_lane_departure"
                                          }
                                          doc.addField("eventType", eventType)
                                          doc.addField("latitude", values(12).toDouble)
                                          doc.addField("longitude", values(13).toDouble)
                                          // TODO: how to enrich with below values? sparksql?
                                          /*doc.addField("owner", "")
                                          doc.addField("year", 1900)
                                          doc.addField("make", "")
                                          doc.addField("model", "")
                                          doc.addField("color", "")*/

                                          doc
                                        })

        // Convert the DStream to a JavaDStream
        val solrDocumentJavaDStream:JavaDStream[SolrInputDocument] = JavaDStream.fromDStream(solrDocumentDStream)

        // Commit the SolrInputDocument DStream
        SolrSupport.indexDStreamOfDocs(zk, solrCollection, 1, solrDocumentJavaDStream) // batch size here is 1 to ensure NRT...not performant
    */
    streamingContext.checkpoint("/tmp/checkpoint")
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getDataFromKafka(topics:String,
                       brokerList:String,
                       streamingContext:StreamingContext
                        ):DStream[(String,String)] = {

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokerList)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topicsSet)

    return messages.map(r => {(r._1, r._2)})
  }

  def loadOriginalKuduProfilesData(tableName:String,
                                   kuduContext:KuduContext,
                                   ssc:StreamingContext
                                    ):RDD[(String, ConnectedCarProfile)] = {

    val originalKuduProfileRDD =
      kuduContext
        .kuduRDD(tableName,"vin,acceleration_aggression_score,braking_aggression_score,handling_aggression_score,overall_aggression_score,average_speed,illegal_lane_departure_plus_minus,collisions_count,hazards_detected_count,readings_count,accelerating_readings_count,decelerating_readings_count,oil_replacement_period,brake_replacement_period,tire_replacement_period,miles_count,brakes_applied_count")
        .map(r => {
        val row = r._2

        val vin = row.getString(0)
        val accelerationAggressionScore = row.getDouble(1)
        val brakingAggressionScore = row.getDouble(2)
        val handlingAggressionScore = row.getDouble(3)
        val overallAggressionScore = row.getDouble(4)
        val averageSpeed = row.getInt(5)
        val illegalLaneDeparturePlusMinus = row.getInt(6)
        val collisionsCount = row.getInt(7)
        val hazardsDetectedCount = row.getInt(8)
        val readingsCount = row.getLong(9)
        val acceleratingReadingsCount = row.getLong(10)
        val deceleratingReadingsCount = row.getLong(11)
        val oilReplacementPeriod = row.getInt(12)
        val brakeReplacementPeriod = row.getInt(13)
        val tireReplacementPeriod = row.getInt(14)
        val milesCount = row.getLong(15)
        val brakesAppliedCount = row.getLong(16)
        val lastUpdated = row.getLong(16)

        val originalConnectedCarProfile = new ConnectedCarProfile(
          vin,
          accelerationAggressionScore,
          brakingAggressionScore,
          handlingAggressionScore,
          overallAggressionScore,
          averageSpeed,
          illegalLaneDeparturePlusMinus,
          collisionsCount,
          hazardsDetectedCount,
          readingsCount,
          acceleratingReadingsCount,
          deceleratingReadingsCount,
          oilReplacementPeriod,
          brakeReplacementPeriod,
          tireReplacementPeriod,
          milesCount,
          brakesAppliedCount,
          lastUpdated,
          false,
          false
        )

        (vin, originalConnectedCarProfile)
      })

    return originalKuduProfileRDD
  }

}