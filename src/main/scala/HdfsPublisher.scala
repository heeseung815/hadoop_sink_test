
import java.util.Calendar

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}

object HdfsPublisher extends GraphStage[SinkShape[String]] {

  val cal = Calendar.getInstance()

  // for hdfs
  val conf = new Configuration()
  conf.set("fs.default.name", "hdfs://localhost:9000")
  System.setProperty("HADOOP_USER_NAME", "hscho")
  val fs: FileSystem = FileSystem.get(conf)
//  val os: FSDataOutputStream = fs.create(new Path("/test/test001.txt"))
  private var os: FSDataOutputStream = _

  // for sink
  val in: Inlet[String] = Inlet("flow.in")
  override val shape: SinkShape[String] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // This requests one element at the Sink startup
      override def preStart(): Unit = {
        println("HdfsPublisher started...")
        pull(in)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          // 입력이 들어올 때 마다 직전 파일의 사이즈를 체크하여 기준치를 넘어선 경우 새로운 파일에 저장한다.
          // 저장된 파일의 사이즈를 구해올 수 있는 지 먼저 확인
          if (os == null) {
            val path = new Path("/test/testfile_1505815112782.txt")
            val temp: FileStatus = fs.getFileStatus(path)
            println(s"@ ${temp.getBlockSize}")
            println(s"@ ${temp.getGroup}")
            println(s"@ ${temp.getLen}")
            println(s"@ ${temp.getPath}")

            val limit = 1 * 1024

          }

          val temp: String = grab(in)
          println("###" + temp) // ByteString to String
//          os.write(temp.getBytes())
//          os.close()
          pull(in)
        }
      })
    }

}


//trait HdfsConnector {
//
//  val fs
//}
