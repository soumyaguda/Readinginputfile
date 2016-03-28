
package com.knowledgent.bd.ingest

import com.typesafe.config.ConfigFactory

import scala.io.Source


trait ConfigSupport {


  val conf = ConfigFactory.load()
  val IngestMode = "mode"

  val InputDelimiter = "input.delimiter"
  val InputName = "input.name"

  val OutputSeparator = "output.delimiter"
  val OutputName = "output.name"

  val ModeLocal = "local"
  val ModeHdfs = "hdfs"

  val delimiter = conf.getString(InputDelimiter)

  val inputFileName = conf.getString(InputName)
  val outputFileName = conf.getString(OutputName)

  val inputSchema = columnNames(inputFileName)



  def firstLine(s: Source): Option[String] = {
    try {
      s.getLines.find(_ => true)
    } finally {
      s.close()
    }
  }

  def columnNames(fileIn: String, d: String = delimiter, ordinalNames: Boolean = true): List[Symbol] = {
    val l = firstLine(Source.fromFile(fileIn)) match {
      case Some(s) => s.split(d).toList
      case _ => List.empty
    }

    ordinalNames match {
      case true => (0 to l.size - 1).toList.map(n => Symbol(n.toString))
      case false => List.empty
    }
  }

}
