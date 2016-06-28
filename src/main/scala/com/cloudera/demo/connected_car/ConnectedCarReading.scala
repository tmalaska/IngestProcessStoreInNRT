package com.cloudera.demo.connected_car

class ConnectedCarReading(
	val time:Long,
	val vin:String, 
	val miles:Long,
	val xAccel:Double, 
	val yAccel:Double,
	val zAccel:Double, 
	val speed:Int, 
	val brakesOn:Boolean, 
	val signalOn:Boolean, 
	val laneDeparted:Boolean, 
	val collisionDetected:Boolean, 
	val hazardDetected:Boolean,
	val latitude:Double,
	val longitude:Double
	) extends Serializable {

	override def toString():String = {
		time + "," +
		vin + "," +
		miles + "," +
		xAccel + "," +
		yAccel + "," +
		zAccel + "," +
		speed + "," +
		brakesOn + "," +
		signalOn + "," +
		laneDeparted + "," +
		collisionDetected + "," +
		hazardDetected + "," + 
		latitude + "," + 
		longitude
	}
}

object ConnectedCarReadingBuilder extends Serializable {
	def build(arg:String):ConnectedCarReading = {
		val args = arg.split(",")

		new ConnectedCarReading(
			args(0).toLong,
			args(1), 
			args(2).toLong,
			args(3).toDouble, 
			args(4).toDouble, 
			args(5).toDouble, 
			args(6).toInt,
			args(7).equals("true"),
			args(8).equals("true"),
			args(9).equals("true"),
			args(10).equals("true"),
			args(11).equals("true"), 
			args(12).toDouble, 
			args(13).toDouble
			)
	}
}