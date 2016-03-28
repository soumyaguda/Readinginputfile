/**
  * Created by Michael.Klatskin on 3/21/2016.
  */
package com.knowledgent.bd.ingest

import cascading.pipe.Pipe
import com.twitter.scalding.Dsl._
import com.twitter.scalding._

trait IngestOperations {

  implicit class OperationsWrapper(val pipe: Pipe) extends IngestOperations with Serializable
  implicit def wrapPipe(rp: RichPipe): OperationsWrapper = new OperationsWrapper(rp.pipe)
  val FieldAction =  "action"
  val FieldUpdated = "updated"

  val ActionInsert = "I"
  val ActionModify = "M"
  val ActionDelete = "D"

  def pipe: Pipe

  val fmt = org.joda.time.format.DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")

  def insertField(s: String, value: Any): Pipe = {
    pipe.map(() ->  Symbol(s)) { _: Unit => value }
  }


  def logsCountVisits: Pipe = pipe
    .groupBy(('day, 'user)) {
      _.size('visits)
    }


}


object IngestionRunner extends ConfigSupport {

  def buildArgs( args: Array[String]) : Args = {
    val a = Args(s" --input $inputFileName --output $outputFileName")

    conf.getString(IngestMode) match {
      case ModeLocal => Mode.putMode(Local(true), a)
      case ModeHdfs => Mode.putMode(Hdfs(true, null), a) // todo pass hadoop conf
      case _ => throw ArgsException("[ERROR] Mode must be one of --local or --hdfs, you provided neither")
    }

  }

  def main(clArgs: Array[String]) {
    val start = System.currentTimeMillis()
    val args: Args = buildArgs(clArgs)

    val x =  Ingester(args)

    x.run
    val stop = System.currentTimeMillis()
    println("\n" + (stop - start))
  }


}
// java -cp target\ingest-1.0-SNAPSHOT-jar-with-dependencies.jar  com.knowledgent.bd.ingest.IngestionRunner  --local


object Ingester {
  def apply(args: Args) = new Ingester(args)
}

class Ingester(args: Args)
  extends Job(args)
    with ConfigSupport
    with IngestOperations  {


  val pipe: Pipe = Csv(p = args("input"), fields = inputSchema, separator = delimiter, skipHeader = true)

    .read
    .debug
    .groupBy( inputSchema(0),inputSchema(5) ) { group:GroupBuilder =>  group.sortBy(inputSchema(1)) }
    .insertField(FieldAction, ActionInsert)
    .insertField(FieldUpdated, System.currentTimeMillis())
    .write(Csv(p = args("output"), separator = "|", skipHeader = true, writeHeader = false))

}

