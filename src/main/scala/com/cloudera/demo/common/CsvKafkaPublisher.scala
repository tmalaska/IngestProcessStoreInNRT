package com.cloudera.demo.common

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object CsvKafkaPublisher {

  var counter = 0

  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<brokerList> <topicName> <dataFolderOrFile> <sleepPerRecord> <acks> <linger.ms> <producer.type> <batch.size>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicName = args(1)
    val nyTaxiDataFolder = args(2)
    val sleepPerRecord = args(3).toInt
    val acks = args(4).toInt
    val lingerMs = args(5).toInt
    val producerType = args(6) //"async"
    val batchSize = args(7).toInt

    val kafkaProducer = getNewProducer(kafkaBrokerList, acks, lingerMs, producerType, batchSize)

    println("--Input:" + nyTaxiDataFolder)

    val dataFolder = new File(nyTaxiDataFolder)
    if (dataFolder.isDirectory) {
      val files = dataFolder.listFiles().iterator
      files.foreach(f => {
        println("--Input:" + f)
        processFile(f, kafkaTopicName, kafkaProducer, sleepPerRecord)
      })
    } else {
      println("--Input:" + dataFolder)
      processFile(dataFolder, kafkaTopicName, kafkaProducer, sleepPerRecord)
    }
    println("---Done")
  }

  def processFile(file:File, kafkaTopicName:String,
                  kafkaProducer: KafkaProducer[String, String], sleepPerRecord:Int): Unit = {
    var counter = 0
    println("-Starting Reading")
    Source.fromFile(file).getLines().foreach(l => {
      counter += 1
      if (counter % 1000 == 0) {
        println("{Sent:" + counter + "}")
      }
      if (counter % 20 == 0) {
        print(".")
      }
      Thread.sleep(sleepPerRecord)
      publishTaxiRecord(l, kafkaTopicName, kafkaProducer)
    })
  }

  def publishTaxiRecord(line:String, kafkaTopicName:String, kafkaProducer: KafkaProducer[String, String]): Unit = {

    if (line.startsWith("vendor_name") || line.length < 10) {
      println("skip")
    } else {
      val message = new ProducerRecord[String, String](kafkaTopicName, line.hashCode.toString, line)
      kafkaProducer.send(message)
    }
  }

  def getNewProducer(brokerList:String,
                      acks:Int,
                      lingerMs:Int,
                      producerType:String,
                      batchSize:Int): KafkaProducer[String, String] = {
    val kafkaProps = new Properties
    kafkaProps.put("bootstrap.servers", brokerList)
    kafkaProps.put("metadata.broker.list", brokerList)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("acks", acks.toString)
    kafkaProps.put("retries", "3")
    kafkaProps.put("producer.type", producerType)
    kafkaProps.put("linger.ms", lingerMs.toString)
    kafkaProps.put("batch.size", batchSize.toString)

    println("brokerList:" + brokerList)
    println("acks:" + acks)
    println("lingerMs:" + lingerMs)
    println("batchSize:" + batchSize)
    println("producerType:" + producerType)
    println(kafkaProps)

    return new KafkaProducer[String,String](kafkaProps)
  }
}
