package com.cloudera.demo.extra

import java.io._
import java.util.zip.{ZipFile, ZipInputStream}

import scala.io.Source

object ReadFirstNFromZipFile {
  val newLine = System.getProperties.get("line.separator")

  def main(args:Array[String]): Unit = {
    val inputFile = args(0)
    val outputFolder = args(1)
    val limit = args(2).toInt

    println("starting")
    println(" reading:" + inputFile)

    val zipFile = new ZipFile(inputFile)

    val outputFolderFile = new File(outputFolder)
    if (outputFolderFile.exists()) {
      throw new IllegalArgumentException("Folder already exist:" + outputFolder)
    } else {
      outputFolderFile.mkdirs()
    }

    val entityIt = zipFile.entries()

    while (entityIt.hasMoreElements) {
      val entry = entityIt.nextElement()
      println("Entry: " + entry.getName)
      println(" > Size: " + entry.getCompressedSize + " -> " + entry.getSize)
      println(" > Comment: " + entry.getComment)

      val pw = new PrintWriter(new File(outputFolder + "/" + entry.getName))
      val reader = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry)))

      var line = reader.readLine()
      var counter = 0
      while (line != null && counter < limit) {
        counter += 1
        pw.write(line + newLine)
        line = reader.readLine()
      }
      pw.close()
      println(" > Finished: " + entry.getName)

    }
  }
}
