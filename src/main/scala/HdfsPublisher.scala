
import java.text.SimpleDateFormat
import java.util.Calendar

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.typesafe.config.ConfigFactory
import common.TimeUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}

object HdfsPublisher extends GraphStage[SinkShape[String]] {

  // for sink
  val in: Inlet[String] = Inlet("flow.in")
  override val shape: SinkShape[String] = SinkShape(in)

  // for application configuration
  val config = ConfigFactory.load()
  val limit = config.getInt("hadoop.fs.file.limit")
  val basePath = config.getString("hadoop.fs.file.path")
  val fileExt = config.getString("hadoop.fs.file.extension")

  // for hadoop configuration
  val conf = new Configuration()
  conf.set("fs.default.name", "hdfs://localhost:9000")
  System.setProperty("HADOOP_USER_NAME", "hscho")
  val fs: FileSystem = FileSystem.get(conf)
  private var os: FSDataOutputStream = _

  // for sensor
  val sensorId = "sensor001"

  // for time
  val fmt = new SimpleDateFormat("yyyy-MM-dd-HHmmss")


  // for file
  var currentFilePath: Path = _

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // This requests one element at the Sink startup
      override def preStart(): Unit = {
        println("HdfsPublisher preStart called...")
        // 입력 값 최초 요청 시에 설정한 이름으로 파일을 생성하고 해당 파일에 쓰기 위한 스트림을 오픈한다.
        currentFilePath = new Path(basePath + "/" + TimeUtil.getCurrentYear + "/" + TimeUtil.getCurrentMonth + "/" + TimeUtil.getCurrentDay + "/" + sensorId + "_" + TimeUtil.getCurrentTime + fileExt)
        println(s"# Current file path: ${currentFilePath.getName}")
        os = fs.create(currentFilePath)
        pull(in)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          // 쓰고자 하는 파일의 사이즈가 기준치를 넘어섰는지 먼저 체크한다.
          val currentFileStatus = fs.getFileStatus(currentFilePath)
          if (currentFileStatus.getLen >= (limit * 1024)) {
            //
          }

          os.close()
          os = null

          pull(in)


          /*
          if (os == null) {
            println(s"os closed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            os = fs.append(currentFilePath)
          }

          println(s"# Current file status: ${currentFileStatus.toString}")
          // 파일 사이즈가 기준치를 넘어서면 새로운 파일을 생성하고 새로운 스트림을 오픈한다.
          // 추가 내용: 스트림이 연결된 상태에서는 변경되는 파일 사이즈를 확인할 수 없다.
          println(s"# File size: ${currentFileStatus.getLen} bytes")
          if (currentFileStatus.getLen > (limit * 1024)) {
            // 기존에 존재하던 스트림을 종료한다.
            println(s"# Existing output stream closed...")
            os.close()

            TimeUtil.resetDate
            currentFilePath = new Path(basePath + "/" + TimeUtil.getCurrentYear + "/" + TimeUtil.getCurrentMonth + "/" + TimeUtil.getCurrentDay + "/" + sensorId + "_" + TimeUtil.getCurrentTime + fileExt)
            println(s"Changed current file path: ${currentFilePath.getName}")
            os = fs.create(currentFilePath)
          }

          val data: String = grab(in)
          os.write(data.getBytes)
          println(s"# Data: $data")
          os.close()
          os = null

          println("----------------------------------------------------------------------------------------------------\n")
          pull(in)
          */

          // 입력이 들어올 때 마다 현재 파일의 사이즈를 체크하여 기준치를 넘어선 경우 새로운 파일에 저장한다.
          // 저장된 파일의 사이즈를 구해올 수 있는 지 먼저 확인
//          if (os == null) {
//            val temp: FileStatus = fs.getFileStatus(currentFilePath)
//            println(s"@ ${temp.getBlockSize}")
//            println(s"@ ${temp.getGroup}")
//            println(s"@ ${temp.getLen}")
//            println(s"@ ${temp.getPath}")
//
//            val tempLimit = limit * 1024
//
//          }

//          os.write(temp.getBytes())
//          os.close()
        }
      })
    }

}