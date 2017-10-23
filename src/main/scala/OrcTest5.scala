import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{ActorMaterializer, Attributes, Inlet, SinkShape}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, VectorizedRowBatch}
import org.apache.orc.{OrcFile, RecordReader, TypeDescription, Writer}

object OrcTest5 extends App {

  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  // test data
  val r1 = UUID.randomUUID.toString
  val r2 = UUID.randomUUID.toString
  val r3 = UUID.randomUUID.toString

  val source = Source(Array(r1, r2, r3).toVector)
//    val source = Source.single("Hello World!")
  val sink = Sink.fromGraph(OrcSink)
  val runnableGraph = source.to(sink)

  runnableGraph.run()
  system.terminate()
}

object OrcSink extends GraphStage[SinkShape[String]] {

  val in: Inlet[String] = Inlet("flow.in")
  override val shape: SinkShape[String] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // preStart
      override def preStart(): Unit = {
        println("# preStart...")

        pull(in)
      }

      // onPush
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val data: String = grab(in)
          println(s"# data: ${data}")

          OrcFileManager.writeToOrcFile(data)



          pull(in)
        }
      })
    }
}

object OrcFileManager {

  private val BASE_PATH = "/"
  private val FILE_NAME = "orcTest1.orc"
  private val SENSOR_ID = "sensor_id"

  // hadoop namenode connection configuration
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://localhost:9000")

  val schema: TypeDescription = TypeDescription.createStruct().addField(SENSOR_ID, TypeDescription.createString())
  val writer: Writer = OrcFile.createWriter(new Path(BASE_PATH + FILE_NAME), OrcFile.writerOptions(conf).setSchema(schema))
  val batch: VectorizedRowBatch = schema.createRowBatch(3)

  var row = 0
  def writeToOrcFile(data: String) = {
//    val fs = FileSystem.get(conf)
//    val fileStatus = fs.getFileStatus(new Path(BASE_PATH + FILE_NAME))
//    println(s"# file status: ${fileStatus.getLen}")

    row = batch.size
    println(s"# current row: ${row}, current batch size: ${batch.size}")
    batch.size += 1

    val stringVector = batch.cols(0).asInstanceOf[BytesColumnVector]
    stringVector.setVal(row, data.getBytes())

    if (batch.size == batch.getMaxSize) {
      println("# same")
      writer.addRowBatch(batch)
      batch.reset
      writer.close()


      val reader = OrcFile.createReader(new Path(BASE_PATH + FILE_NAME), OrcFile.readerOptions(conf))
      println(s"# reader: ${reader.getSchema.toJson}")
      println(s"# reader: ${reader.getSchema.toString}")
      val batch2 = reader.getSchema.createRowBatch()
      val rows: RecordReader = reader.rows()
      while (rows.nextBatch(batch2)) {
        println(s"# started... batch size: ${batch2.size}")
        val stringVector = batch2.cols(0).asInstanceOf[BytesColumnVector]

        var r = 0
        while ( {
          r < batch2.size
        }) {
          val stringValue = new String(stringVector.vector(r), stringVector.start(r), stringVector.length(r))
          println(s"# value: $stringValue")

          r += 1
        }
      }

      rows.close()
    }
  }

}
