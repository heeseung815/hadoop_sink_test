
import java.text.SimpleDateFormat
import java.util.Calendar

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.typesafe.config.ConfigFactory
import common.TimeUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}

import scala.collection.mutable

// TODO: 리모트로 하둡 파일시스템 연결이 안되는 경우 재시도하는 로직을 생각해 두어야한다.
object HdfsPublisher extends GraphStage[SinkShape[String]] {

  // for sink
  val in: Inlet[String] = Inlet("flow.in")
  override val shape: SinkShape[String] = SinkShape(in)

  // for application configuration
  val config = ConfigFactory.load()
  val limit = config.getInt("hadoop.fs.file.limit")
  val basePath = config.getString("hadoop.fs.file.path")
  val fileExt = config.getString("hadoop.fs.file.extension")
  val fileOption = config.getString("hadoop.fs.file.option")

  // for hadoop configuration
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://localhost:9000")
  conf.set("dfs.support.append", "true")
//  System.setProperty("HADOOP_USER_NAME", "hscho")
  var fs: FileSystem = FileSystem.get(conf)
  private var os: FSDataOutputStream = _

  // TODO: 센서별로 파일스트림 관리할 수 있는 컬렉션 필요
  private val sensorStreamTable = mutable.Map[String, FSDataOutputStream]()

  // for sensor
  val sensorId = "sensor001"

  // for time
  val fmt = new SimpleDateFormat("yyyy-MM-dd-HHmmss")


  // for file
  var currentFilePath: Path = _

  private def getSensorStream(sensorId: String): Option[FSDataOutputStream] = {
    sensorStreamTable.get(sensorId)
  }
  private def registerSensorStream(sensorId: String, outputStream: FSDataOutputStream) = {
    println(s"reigster sensor stream: $sensorId")
    sensorStreamTable.put(sensorId, outputStream)
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // This requests one element at the Sink startup
      override def preStart(): Unit = {
        println("# preStart called...\n")
        // 입력 값 최초 요청 시에 설정한 이름으로 파일을 생성하고 해당 파일에 쓰기 위한 스트림을 오픈한다.
//        currentFilePath = new Path(basePath + "/" + TimeUtil.getCurrentYear + "/" + TimeUtil.getCurrentMonth + "/" + TimeUtil.getCurrentDay + "/" + sensorId + "_" + TimeUtil.getCurrentTime + fileExt)
//        println(s"# Current file path: ${currentFilePath.getName}")
//        os = fs.create(currentFilePath)
        pull(in)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          if (currentFilePath == null) currentFilePath = new Path(basePath + "/" + TimeUtil.getCurrentYear + "/" + TimeUtil.getCurrentMonth + "/" + TimeUtil.getCurrentDay + "/" + sensorId + "_" + TimeUtil.getCurrentTime + fileExt)
          // 최초 파일 또는 새로운 파일
          if (fs.exists(currentFilePath)) {
            println("exist")
            val fileStatus = fs.getFileStatus(currentFilePath)
            if (fileStatus.getLen >= (limit * 1024)) {
              println("exist but exceed limit")
              TimeUtil.resetDate
              currentFilePath = new Path(basePath + "/" + TimeUtil.getCurrentYear + "/" + TimeUtil.getCurrentMonth + "/" + TimeUtil.getCurrentDay + "/" + sensorId + "_" + TimeUtil.getCurrentTime + fileExt)
              os = fs.create(currentFilePath)
            }
            os = fs.append(currentFilePath)
//            os = fs.create(currentFilePath)
          } else {
            println("not exist")
            os = fs.create(currentFilePath)
          }

          // TODO: 플로우로부터 받은 데이터 유효성 검사 로직 필요한지 여부 확인 할 것
          val data: String = grab(in)
          println(s"# os: $os")

//          PrintWriter()
          os.write(data.getBytes())
          print(s"# Data : $data")

//          os.close()
          if (os != null) {
            println("stream close")
            os.close()
            os = null
            println("null set")
          }

          val read = scala.io.Source.fromInputStream(fs.open(currentFilePath))
          read.getLines().foreach(println)


          println("pull(in)\n")
          pull(in)
/*
          // 쓰고자 하는 파일의 사이즈가 기준치를 넘어섰는지 먼저 체크한다.
          val currentFileStatus = fs.getFileStatus(currentFilePath)
          if (currentFileStatus.getLen == 0) {
            println("# create called ")
            os.write(data.getBytes())
            os.close()
            os = null
          } else if (currentFileStatus.getLen >= (limit * 1024)) {
            println("# create called due to size limit")
            // 넘어선 경우 새로운 파일을 만들고 스트림을 생성한다.
            TimeUtil.resetDate
            currentFilePath = new Path(basePath + "/" + TimeUtil.getCurrentYear + "/" + TimeUtil.getCurrentMonth + "/" + TimeUtil.getCurrentDay + "/" + sensorId + "_" + TimeUtil.getCurrentTime + fileExt)
            os = fs.create(currentFilePath)
          } else {
            println("# append called")
            println(s"##### $os")
            os = fs.append(currentFilePath)
          }

          // append
          // create




          pull(in)

*/
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