package com.cloudera.demo.connected_car

import java.lang.StringBuilder
import java.util.{Properties, Random}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.io.Source

object ConnectedCarReadingGenerator {

	val random = new Random()
	val startTime = System.currentTimeMillis

	var coordinatesList:Vector[String] = null

	var generateErrors = false

	def main(args:Array[String]):Unit = {
		if (args.length == 0) {
			println("Args: <Kafka broker list> <car properties file> <latitude,longitude csv file> [-generateErrors]")
			return
		}

		val kafkaBrokerList = args(0)
		val kafkaTopicName = "connected_car_readings"
		val carPropertiesFile = args(1)
		val coordinatesListFile = args(2)

		generateErrors = {
			if (args.length == 4) {
				args(3).equals("-generateErrors")
			} else {
				false
			}
		}

		val kafkaProducer = getNewProducer(kafkaBrokerList)

		val carPropertiesList = Source.fromFile(carPropertiesFile).getLines.toList

		coordinatesList = Source.fromFile(coordinatesListFile).getLines.toVector

		while (true) {
			for (carProperties <- carPropertiesList) {
				// TODO: why am I generating a string to build a pojo to convert it back to a string? probaby should just lose the pojo...
				//val connectedCarReading = ConnectedCarReadingBuilder.build(generateConnectedCarReading(carProperties))
				//val message = new ProducerRecord[String, String](kafkaTopicName, connectedCarReading.vin, connectedCarReading.toString())

				val message = new ProducerRecord[String, String](kafkaTopicName, carProperties.split(",")(0), generateConnectedCarReading(carProperties))
				kafkaProducer.send(message)
			}
			Thread.sleep(1000)
			print(".")
		}
	}

	def getNewProducer(brokerList:String): KafkaProducer[String, String] = {
		val kafkaProps = new Properties
		kafkaProps.put("bootstrap.servers", brokerList)
		kafkaProps.put("metadata.broker.list", brokerList)
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		kafkaProps.put("acks", "0")
		kafkaProps.put("retries", "3")
		kafkaProps.put("producer.type", "async")
		kafkaProps.put("linger.ms", "500")
		kafkaProps.put("batch.size", "1000")

		return new KafkaProducer[String,String](kafkaProps)
	}

	def generateConnectedCarReading(carProperties:String):String = {
		/*
			0 = vin
			1 = starting miles
			2 = avg speed
			3 = average accel
			4 = average decel
			5 = average handling
			6 = illegal lane departures per 100
			7 = collision probability per 100000
		*/
		val props = carProperties.split(",")

		val sb = new StringBuilder

		val time = System.currentTimeMillis

		val vin = props(0)

		val miles = calculateMiles(props(1).toInt, props(2).toInt)

		val xAccel = props(5).toDouble + (random.nextGaussian() * .5) // standard deviation of .5

		var yAccel = random.nextGaussian() * .5 // standard deviation of .5
		if (random.nextBoolean()) { // TODO: this is a naive way to decide if we're accelerating or decelerating, and depending on the gaussian may not even produce the correct result...do I care?
			yAccel = yAccel + props(3).toDouble
		} else {
			yAccel = yAccel - props(4).toDouble
		}

		val zAccel = 0

		var speed = (props(2).toInt + (random.nextGaussian() * 5)).toInt // standard deviation of 5
		if (speed < 0) { // make sure speed is never negative
			speed = 0
		}

		if (generateErrors && random.nextInt(1000) < 1) {
			speed = -speed
		}

		var brakesOn = false
		if (random.nextInt(10) < 1) { // brakes are on 10% of the time...TODO: should this actually be based on negative yAccel?
			brakesOn = true
		}

		var laneDeparted = false
		if (random.nextInt(50) < 1) { // lane departure is 2% of the time
			laneDeparted = true
		}

		var signalOn = false
		if (laneDeparted) {
			if (random.nextInt(100) > props(6).toInt) {
				signalOn = true
			}
		}

		var collisionDetected = false
		/*if (random.nextInt(100000) < props(7).toInt) {
			collisionDetected = true
		}*/
		// sum all acceleration scores, multiply that by 100
		val collisionThreshold = ((props(3).toDouble + props(4).toDouble + props(5).toDouble) * 10000).toInt
		if (random.nextInt(10000000) < collisionThreshold) {
			collisionDetected = true
		}

		var hazardDetected = false
		if (random.nextInt(1000) < 1) {
			hazardDetected = true
		}

		/* These map to like Kazakhstan or something in leafletjs...using the ones that work in US
		// US boundaries are roughly 25-49 lon and 67-124 lat
		val latitude = 49 - (random.nextDouble() * 24)
		val longitude = 124 - (random.nextDouble() * 57)
		*/
		//val latitude = 83.126102 - (random.nextDouble() * 77.626552) // between 5.499550 and 86.162102
		//val longitude = -52.233040 - (random.nextDouble() * 115.043373) // between -167.276413 and -52.233040
		var latitude = "0"
		var longitude = "0"

		if (!generateErrors || random.nextInt(1000) > 5) {
			val latlon = coordinatesList(random.nextInt(coordinatesList.length)).split(",")
			latitude = latlon(0)
			longitude = latlon(1)
		}

		sb.append(time)
		sb.append(",")
		// Somewhat arbitrary, but generate a missing VIN for .1% of records
		if (!generateErrors || random.nextInt(1000) > 1) {
			sb.append(vin)
		}
		sb.append(",")
		sb.append(miles)
		sb.append(",")
		sb.append(xAccel)
		sb.append(",")
		sb.append(yAccel)
		sb.append(",")
		sb.append(zAccel)
		sb.append(",")
		sb.append(speed)
		sb.append(",")
		sb.append(brakesOn)
		sb.append(",")
		sb.append(signalOn)
		sb.append(",")
		sb.append(laneDeparted)
		sb.append(",")
		sb.append(collisionDetected)
		sb.append(",")
		sb.append(hazardDetected)
		sb.append(",")
		sb.append(latitude)
		sb.append(",")
		sb.append(longitude)

		return sb.toString
	}

	def calculateMiles(startingMiles:Int, avgSpeed:Int):Int = {
		val elapsedSeconds = (System.currentTimeMillis - startTime) / 1000 // how many seconds has this generator been running?

		return startingMiles + (avgSpeed.toDouble / 3600.0 * elapsedSeconds.toDouble).toInt
	}
}
