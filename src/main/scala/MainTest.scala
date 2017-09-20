import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.concurrent.Future

object MainTest extends App {

  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val config = ConfigFactory.load()
  val hostname = config.getString("hadoop.hostname")
  val port = config.getString("hadoop.port")
  val basePath = config.getString("hadoop.fs.file.path")

  // for hdfs configuration
//  val conf = new Configuration()
//  conf.set("fs.default.name", "hdfs://localhost:9000")
//  System.setProperty("HADOOP_USER_NAME", "hscho")
//  val fs: FileSystem = FileSystem.get(conf)
//  val os: FSDataOutputStream = fs.create(new Path("/test/test001.txt"))

  println("\nMainTest started...\n")
  println(s"### ${hostname}")
  println(s"### ${port}")
  println(s"### ${basePath}")

//  val source = Source.single(ByteString("Hello World!"))
//  val sink = Sink.foreach(println)
//  val runnableGraph = source.runWith(sink)

//  runnableGraph.onComplete(result => println(s"done"))

  val source = Source(Array("1\n", "2\n", "3\n").toVector)
//  val source = Source.single("Hello World!")
  val sink = Sink.fromGraph(HdfsPublisher)
  val runnableGraph = source.to(sink)

  runnableGraph.run()
  system.terminate()

  println("\nMainTest terminated.\n")
}

