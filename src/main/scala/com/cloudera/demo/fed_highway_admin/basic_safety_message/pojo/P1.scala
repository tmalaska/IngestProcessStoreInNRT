package com.cloudera.demo.fed_highway_admin.basic_safety_message.pojo

class P1 (val rx_device: Long,
          val file_id:Long,
           val tx_device: Long,
           val gen_time: Long,
           val day_of_week: String,
           val hour_or_day: Int,
           val tx_random: Long,
           val msg_count: Long,
           val d_second: Int,
           val latitude: Double,
           val longitude: Double,
           val elevation: Double,
           val heading: Double,
           val a_x: Double,
           val a_y: Double,
           val a_z: Double,
           val path_count: Int,
           val radius_of_curve: Double,
           val confidence: Int) {
  val a_a = Math.sqrt(Math.pow(Math.sqrt(Math.pow(a_x, 2) + Math.pow(a_y, 2)), 2) +
    Math.pow(a_z, 2))
}

object P1Builder {
  def build(cells:Array[String]): P1 = {

    new P1(
    cells(1).toLong,
    cells(2).toLong,
    cells(3).toLong,
    cells(4).toLong,
    cells(5).toString,
    cells(6).toInt,
    cells(7).toLong,
    cells(8).toLong,
    cells(9).toInt,
    cells(10).toDouble,
    cells(11).toDouble,
    cells(12).toDouble,
    cells(13).toDouble,
    cells(14).toDouble,
    cells(15).toDouble,
    cells(16).toDouble,
    cells(17).toInt,
    cells(18).toDouble,
    cells(19).toInt)
  }
}
