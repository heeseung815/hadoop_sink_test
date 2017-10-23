import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.io.StdIn

object JsonTest {

  def main(args: Array[String]): Unit = {
    println("JsonTest start...")

    val hdfsConfig = new Configuration()
    hdfsConfig.set("fs.defaultFS", "hdfs://localhost:9000")

    val path: Path = new Path("/test.json")
    val fs = FileSystem.get(hdfsConfig)

    if (!fs.createNewFile(path)) {
      fs.delete(path, true)
    } else {
      while (true) {
        var os: FSDataOutputStream = fs.append(path)
os.flush()
        print("> ")
        val data = StdIn.readLine()
        println(s"position: ${os.getPos}")

        if (data.equals("save")) {
          println("save and close...")
          os.close()
        } else {
          os.write(data.getBytes())
          println("writed...")
          os.close()
          os = null
        }
      }
    }

/*
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    val os: FSDataOutputStream = fs.create(path)

    while (true) {
      print("> ")
      val data = StdIn.readLine()
      println(s"InputData: $data")


      if (data.equals("save")) {
        println("closed after saving data...")
        os.close()
        return

      } else {
        println("writed...")
        os.write(data.getBytes())
        os.close()
//        os.flush()
      }
    }
*/
  }
}
