package com.cloudera.demo.ny_taxi

import com.cloudera.demo.ny_taxi.pojo.NyTaxiYellowTripBuilder
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext

import org.apache.spark.{SparkConf, SparkContext}

object TripKMeansExample {
  def main(args:Array[String]): Unit = {


    if (args.length == 0) {
      println("Args: <runLocal> <kuduMaster> " +
        "<TaxiTripTableName> " +
        "<centers> " +
        "<iterations> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val kuduMaster = args(1)
    val taxiTripTableName = args(2)
    val centers = args(3).toInt
    val iterations = args(4).toInt

    val sc:SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain")
      new SparkContext(sparkConfig)
    }

    val sqlContext = new SQLContext(sc)

    val kuduOptions = Map(
      "kudu.table" -> taxiTripTableName,
      "kudu.master" -> kuduMaster)

    sqlContext.read.options(kuduOptions).format("org.kududb.spark.kudu").load.
      registerTempTable("ny_taxi_trip")

    sqlContext.sql("select * from ny_taxi_trip").registerTempTable()

    val vectorRDD = sqlContext.sql("select * from ny_taxi_trip").map( r => {
      val trip = NyTaxiYellowTripBuilder.build(r)
      val array = Array(trip.dropoff_latitude,
        trip.dropoff_longitude,
        trip.pickup_latitude,
        trip.pickup_longitude,
        trip.total_amount,
        trip.tip_amount,
        trip.tolls_amount,
        trip.passenger_count,
        trip.trip_distance)
      Vectors.dense(array)
    })

    val clusters = KMeans.train(vectorRDD, centers, iterations)
    println("Vector Centers:")
    clusters.clusterCenters.foreach(v => println(" >> " + v))
  }
}
