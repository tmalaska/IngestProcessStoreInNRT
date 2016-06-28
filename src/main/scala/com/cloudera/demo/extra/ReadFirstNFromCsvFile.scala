package com.cloudera.demo.extra

import java.io._

import kafka.api.{TopicData, FetchResponsePartitionData}

import scala.io.Source

object ReadFirstNFromCsvFile {
  def main(args:Array[String]): Unit = {
    val inputFile = args(0)
    val outputFile = args(1)
    val limit = args(2).toInt

    println("starting")
    println(" reading:" + inputFile)

    val pw = new PrintWriter(new File(outputFile))

    val newLine = sys.props("line.separator")

    val lines = Source.fromFile(inputFile).getLines()

    for (i <- 0 until limit) {
      val line = lines.next()
      println(line)
      pw.write(line + newLine)
    }
    pw.close
  }
}
