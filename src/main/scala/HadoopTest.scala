import Hdfs.conf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HadoopTest extends App {

  val hdfsConfig = new Configuration()
  hdfsConfig.set("fs.defaultFS", "hdfs://192.168.1.72:8020")
  System.setProperty("HADOOP_USER_NAME", "hadoop")
//  hdfsConfig.set("fs.defaultFS", "hdfs://localhost:9000")
//  System.setProperty("HADOOP_USER_NAME", "hscho")

  val path = new Path("/testFile1.txt")
  val fs = FileSystem.get(hdfsConfig)
  val os = fs.create(path)
  os.write("test".getBytes())

//  def write(filePath: String, data: Array[Byte]) = {
//    val path = new Path(filePath)
//    val fs = FileSystem.get(conf)
//    val os = fs.create(path)
//    os.write(data)
//    fs.close()
//  }
//
//  Hdfs.write("/testFile1.txt", "Hello World".getBytes)


}
