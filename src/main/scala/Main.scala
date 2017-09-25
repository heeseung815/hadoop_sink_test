
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Main extends App {

  private var temp: String = _

//  temp = "ddd"
//  if (temp == null) println("null") else println(s"$temp")
  temp match {
    case _: String => println(s"# $temp")
    case _ => println(s"#null: $temp")
  }

//  implicit val system = ActorSystem()
//  implicit val ec = system.dispatcher
//  implicit val materializer = ActorMaterializer()
//
//  val conf = new Configuration()
//  conf.set("fs.default.name", "hdfs://localhost:9000")
//  System.setProperty("HADOOP_USER_NAME", "hscho")

  /*
          /data/${YYYY}/${MM}/${DD}/${sensorid}_시작시간
  */

//  val outputFile = "/Users/hscho/uangel/projects/test/hadoop_sink_test/resources/outputFile4.txt"
//
//  val source: Source[ByteString, NotUsed] = Source((1 to 10).map(a => ByteString.apply(a.toByte)))
//  val sink: Sink[Int, Future[Done]] = {
//    Sink.foreach(a => println(a))
//  }
//
//
//  val sink2: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(outputFile), Set(CREATE, WRITE, APPEND))
//
//  val graph: RunnableGraph[NotUsed] = source.to(sink2)
//
//  graph.run()
//  system.terminate()

//  val hdfs = FileSystem.get(conf)   // hadoop file system
//  val path = new Path("testFile2.txt")    // save path with filename

//  val inputFile = "/Users/hscho/uangel/projects/test/hadoop_sink_test/resources/inputFile.txt"
//  val outputFile = "/Users/hscho/uangel/projects/test/hadoop_sink_test/resources/outputFile2.txt"
//
//  println("heeseung".getBytes())
//  val source2: Source[ByteString, NotUsed] = Source.single(ByteString("heeseung"))
//  val hdfsSink: Sink[ByteString, NotUsed] = Sink.fromGraph(HdfsPublisher)
//
//  val graph = source2.to(hdfsSink)
//  graph.run()
//  system.terminate()

//  val graph: Future[IOResult] = source2.runWith(sink)
//  graph.onComplete { _ =>
//    println("done")
//    system.terminate()
//  }


//  val sourceTest: Source[Int, NotUsed] = Source(1 to 100)
//  val sinkTest: Sink[String, NotUsed] = Sink.fromGraph(HdfsPublisher)
//  val sinkTest =
//    Flow[String]
//      .map(s => ByteString(s + "\n"))
//      .toMat(FileIO.toPath(Paths.get(outputFile))) (Keep.right)


//  val runnableGraph = sourceTest.to(sinkTest)
//
//
//  runnableGraph.run()

//  sourceTest.map(_.toString).runWith(sinkTest)
//  system.terminate()


//  val source = Source(1 to 10)
//  val flow: Flow[Int, String, NotUsed] = Flow[Int].map(_.toString)
//
//  val fileFlow: Flow[String, IOResult, NotUsed] =
//    Flow[String].mapAsync(parallelism = 4){ s ⇒
//      Source.single(ByteString(s)).runWith(FileIO.toPath(Paths.get(outputFile)))
//    }
//
//  val fileSink: Sink[IOResult, Future[Done]] = Sink.foreach[IOResult]{println(_)}
//  val fileSink2: Sink[Int, Future[Done]] = Sink.foreach{println(_)}

//  val graph = source.via(flow).via(fileFlow).to(fileSink)
//  val graph = source.to(fileSink2)
//  println("##############")
//  graph.run
//
//  system.terminate()
}
