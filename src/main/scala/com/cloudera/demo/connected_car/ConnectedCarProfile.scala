package com.cloudera.demo.connected_car

/* TODO:
 * 	How to handle cars moslty on highway? Do we include 0 accel readings in average?
 *	How to calculate replacement periods based on averages?
 */

class ConnectedCarProfile(
	var vin:String = "",
	var accelerationAggressionScore:Double = 0, //anything over 1 or 2 is prob aggressive...avg is prob btwn 3 and 4 m/s^2 really trying 0-60 (drag racer = 30m/2^2 over1/4 mile, 1983 Buick Century = 1.9 m/s^2 over 0-60)
	var brakingAggressionScore:Double = 0, // ill go same as accelleration? anything over 2 is aggressive?
	var handlingAggressionScore:Double = 0, // gonna say anything over .5 is aggressive and should cut the replacement period in half
	var overallAggressionScore:Double = 0,
	var averageSpeed:Int = 0,
	var illegalLaneDeparturePlusMinus:Int = 0,
	var collisionsCount:Int = 0,
	var hazardsDetectedCount:Int = 0,
	var readingsCount:Long = 0,
	var acceleratingReadingsCount:Long = 0,
	var deceleratingReadingsCount:Long = 0,
	var oilReplacementPeriod:Int = 5000, 
	var brakeReplacementPeriod:Int = 30000,
	var tireReplacementPeriod:Int = 40000,
	var milesCount:Long = 0,
	var brakesAppliedCount:Long = 0,
	var lastUpdated:Long = 0,
	var isInsert:Boolean = false, // for streaming job, not stored in Kudu
	var hasChanged:Boolean = false // for streaming job, not stored in Kudu
	) extends Serializable {

	override def toString():String = {
		vin + "," +
		accelerationAggressionScore + "," +
		brakingAggressionScore + "," +
		handlingAggressionScore + "," +
		overallAggressionScore + "," +
		averageSpeed + "," +
		illegalLaneDeparturePlusMinus + "," + 
		collisionsCount + "," + 
		hazardsDetectedCount + "," +
		readingsCount + "," + 
		acceleratingReadingsCount + "," + 
		deceleratingReadingsCount + "," + 
		oilReplacementPeriod + "," +
		brakeReplacementPeriod + "," +
		tireReplacementPeriod + "," +
		milesCount + "," +
		brakesAppliedCount + "," +
		lastUpdated + "," +
		isInsert + "," + 
		hasChanged
	}
/*
	def += (connectedCarProfile:ConnectedCarProfile):Unit = {
		vin = connectedCarProfile.vin
		accelerationAggressionScore = combineAverages(accelerationAggressionScore, readingsCount, connectedCarProfile.accelerationAggressionScore, connectedCarProfile.readingsCount)
	}
*/

	def addReading(reading:String) = {
		val values = reading.split(",")

		lastUpdated = values(0).toLong

		// If this is a new profile, we need to set the vin
		if (vin.equals("")) {
			vin = values(1)
		}

		val miles = values(2).toLong
		val xAccel = values(3).toDouble
		val yAccel = values(4).toDouble
		val zAccel = values(5).toDouble
		val speed = values(6).toInt
		val brakesOn = values(7).equals("true")
		val signalOn = values(8).equals("true")
		val laneDeparted = values(9).equals("true")
		val collisionDetected = values(10).equals("true")
		val hazardDetected = values(11).equals("true")

		if (miles > milesCount) {
			milesCount = miles
		}

		if (yAccel > 0) { // the vehicle is accelerating
			accelerationAggressionScore = combineAverages(accelerationAggressionScore, acceleratingReadingsCount, yAccel, 1)
			acceleratingReadingsCount += 1
		} else { // vehicle is braking/not accelerating...zero is included with this, which it probably shouldn't be
			brakingAggressionScore = combineAverages(brakingAggressionScore, deceleratingReadingsCount, math.abs(yAccel), 1)
			deceleratingReadingsCount += 1
		}

		handlingAggressionScore = combineAverages(handlingAggressionScore, readingsCount, Math.abs(xAccel), 1)

		overallAggressionScore = accelerationAggressionScore + brakingAggressionScore + handlingAggressionScore // overall aggression score is just the sum of all other aggression scores

		//averageSpeed = combineAverages(averageSpeed, readingsCount, speed, 1)
		averageSpeed = (((averageSpeed * readingsCount) + speed) / (readingsCount + 1)).toInt

		if (laneDeparted) {
			if (signalOn) {
				illegalLaneDeparturePlusMinus -= 1 // this was a lane departure with a turn signal, so subtract one from the illegal lane departure plus/minus counter
			} else {
				illegalLaneDeparturePlusMinus += 1 // this was a lane departure without a turn signal, so add one to the illegal lane departure plus/minus counter
			}
		}

		if (collisionDetected) {
			collisionsCount += 1
		}

		if (hazardDetected) {
			hazardsDetectedCount += 1
		}

		if (brakesOn) {
			brakesAppliedCount += 1
		}

		// recalculate maintenance periods
		oilReplacementPeriod = 5000 - ((accelerationAggressionScore - 2) * 5000).toInt
		brakeReplacementPeriod = 30000 - ((brakingAggressionScore - 1) * 15000).toInt
		tireReplacementPeriod = 30000 - ((handlingAggressionScore - .5) * 25000).toInt

		// enforce minimum/maximum maintenance periods
		if (oilReplacementPeriod > 10000) oilReplacementPeriod = 10000
		if (oilReplacementPeriod < 500) oilReplacementPeriod = 500
		if (brakeReplacementPeriod > 50000) brakeReplacementPeriod = 50000
		if (brakeReplacementPeriod < 1000) brakeReplacementPeriod = 1000
		if (tireReplacementPeriod > 50000) tireReplacementPeriod = 50000
		if (tireReplacementPeriod < 1000) tireReplacementPeriod = 1000

		readingsCount += 1
	}

	private def combineAverages(scoreA:Double, countA:Long, scoreB:Double, countB:Long):Double = {
		return (((scoreA * countA) + (scoreB * countB)) / (countA + countB)).toDouble
	}

	private def combineAverages(scoreA:Int, countA:Long, scoreB:Int, countB:Long):Int = {
		return (((scoreA * countA) + (scoreB * countB)) / (countA + countB)).toInt
	}
}