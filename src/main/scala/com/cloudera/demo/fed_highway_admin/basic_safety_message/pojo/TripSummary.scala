package com.cloudera.demo.fed_highway_admin.basic_safety_message.pojo

class TripSummary (val device_id:Long,
                   val trip_id:String,
                   val epoch_start_time:Long,
                   val start_date:String,
                   val start_time:String,
                   val epoch_end_time:Long,
                   val end_date:String,
                   val end_time:String,
                   val total_trip_distance:Double,
                   val dis_travelled_plus_25_mph:Double,
                   val trip_duration:Double,
                   val avg_speed:Double,
                   val max_speed:Double,
                   val break_count:Int,
                   val wiper_activity:String){

}

object TripSummaryBuilder {
  def build(cells:Array[String]): TripSummary = {
    new TripSummary(cells(0).toLong,
    cells(1),
    cells(2).toLong,
    cells(3),
    cells(4),
    cells(5).toLong,
    cells(6),
    cells(7),
    cells(8).toDouble,
    cells(9).toDouble,
    cells(10).toDouble,
    cells(11).toDouble,
    cells(12).toDouble,
    cells(13).toInt,
    cells(14))
  }
}