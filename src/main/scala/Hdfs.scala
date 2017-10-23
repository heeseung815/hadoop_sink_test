import java.io.InputStream
import java.net.Socket

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.language.postfixOps

object Hdfs extends App {

  val conf = new Configuration()
  conf.set("fs.default.name", "hdfs://localhost:9000")
  System.setProperty("HADOOP_USER_NAME", "hscho")
  //  conf.set("fs.defaultFS", "hdfs://localhost:9000")
  //  System.setProperty("HADOOP_USER_NAME", "hanyoungtak")

  def write(filePath: String, data: Array[Byte]) = {
    val path = new Path(filePath)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()
  }

  def removeFile(filename: String): Boolean = {
    val fs = FileSystem.get(conf)
    val path = new Path(filename)
    fs.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val fs = FileSystem.get(conf)
    val path = new Path(filename)
    fs.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    val fs = FileSystem.get(conf)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
  }

  def inputToFile(is: java.io.InputStream) {
    val in = scala.io.Source.fromInputStream(is)
    //val out = new java.io.PrintWriter(f)
    //try { in.getLines().foreach(out.println(_)) }
    try { in.getLines().foreach(println) }
    //finally { out.close }
  }

  def checkHDFS = {
    println("==> ???")

    var s: Socket = null
    try {
//      val fs = FileSystem.get(conf)
//      val temp = fs.access(new Path("/"), FsAction.ALL)

//      s = new Socket("192.168.1.72", 8020)
      s = new Socket("localhost", 9000)
      println("running!")
      true
    } catch {
      case e: Exception => println("error!!"); false
    } finally {
      println("finally")
//      if (s != null) s.close()
    }
//    println(s"${fs}")
//    println(s"${fs.toString}")
//    println(s"${fs.getStatus.getUsed}")
  }

  def serverListening: Unit = {
//    val socket: Socket = new Socket("localhost", 9000)
//    if (socket != null) {
//      println(s"${socket.getKeepAlive}")
//      println("hdfs is running")
//    } else {
//      println("hdfs is not running")
//    }

    var s: Socket = null
    try {
      s = new Socket("localhost", 9000)
      println("running")
    } catch {
      case e: Exception =>
        println(e)
        println("++++++++++++++++++++++++++++")
        println("not running")
    } finally {
      if (s != null) {
        try
          s.close
        catch {
          case e: Exception => ???
        }
      }
    }
  }

  def checkHDFSWithFuture(): Future[Any] = {
    println("....")
    var s: Socket = null
    Future {
        try {
          s = new Socket("localhost", 9000)
        } catch {
          case e: Exception => e
        } finally {
          if (s != null)
            s.close()
        }
      }

  }

  val result: Future[Any] = checkHDFSWithFuture()
  println("ees")
  val temp = Await.result(result, 2 seconds)
  println("ddd")
  temp match {
    case _: Socket => println("success..")
    case e: Throwable => println("failure.."); e.printStackTrace()
  }
//  Thread.sleep(2000)
//  result.onComplete {
//    case Success(s) => s.asInstanceOf[Socket].close(); println("success...")
//    case Failure(e) => println(e.toString)
//  }

//  val result: Future[Unit] = checkHDFSWithFuture()
//  Thread.sleep(1000)
//  result.onComplete {
//    case Success(x) => println(s"x: ${x.toString}"); println("success")
//    case Failure(e) => println("......"); println(e)
//    case _ => println("unit.....")
//  }
//  println("terminated...")
//  val
  // temp = checkHDFS
//  println(s"result : ${temp}")
//  serverListening
//  Hdfs.write("/testFile1.txt", "Hello World".getBytes)
//  Hdfs.write("/test1.txt", "Hello World".getBytes)
//  Hdfs.inputToFile(Hdfs.getFile("/testFile1.txt"))
//  Hdfs.removeFile("/test1.txt")
//  Hdfs.createFolder("/testFolder1")
}