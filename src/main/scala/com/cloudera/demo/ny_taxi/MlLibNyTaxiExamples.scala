package com.cloudera.demo.ny_taxi

import com.cloudera.demo.ny_taxi.pojo.NyTaxiYellowTripBuilder
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object MlLibNyTaxiExamples {

  /*
  Remember the following link reference
    https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/mllib
   */
  def main(args: Array[String]): Unit = {
    
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

    val sc: SparkContext = if (runLocal) {
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

    //Vector
    val vectorRDD = sqlContext.sql("select * from ny_taxi_trip").map(r => {
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

    println("--Running KMeans")
    val clusters = KMeans.train(vectorRDD, centers, iterations)
    println(" > vector centers:")
    clusters.clusterCenters.foreach(v => println(" >> " + v))

    println("--Running corr")
    val correlMatrix: Matrix = Statistics.corr(vectorRDD, "pearson")
    println(" > corr: " + correlMatrix.toString)

    println("--Running colStats")
    val colStats = Statistics.colStats(vectorRDD)
    println(" > max: " + colStats.max)
    println(" > count: " + colStats.count)
    println(" > mean: " + colStats.mean)
    println(" > min: " + colStats.min)
    println(" > normL1: " + colStats.normL1)
    println(" > normL2: " + colStats.normL2)
    println(" > numNonZeros: " + colStats.numNonzeros)
    println(" > variance: " + colStats.variance)


    //Labeled Points
    val labeledPointRDD = sqlContext.sql("select * from ny_taxi_trip").map(r => {
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

      val payTypeBool = if (trip.payment_type.equalsIgnoreCase("cash")) 1 else 0

      new LabeledPoint(payTypeBool, Vectors.dense(array))
    })

    val splits = labeledPointRDD.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //TODO
    //Sampleing by Key: http://spark.apache.org/docs/latest/mllib-statistics.html
  }
}
