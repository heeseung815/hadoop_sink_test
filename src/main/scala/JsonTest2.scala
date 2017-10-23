import java.io.{BufferedReader, IOException, InputStreamReader, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object JsonTest2 {

  def configureFileSystem(coreSitePath: String, hdfsSitePath: String): FileSystem = {
    var fs: FileSystem = null
    val conf = new Configuration()
    conf.setBoolean("dfs.support.append", true)
    conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true)
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS")
    conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.best-effort", true)
    val coreSite: Path = new Path(coreSitePath)
    val hdfsSite = new Path(hdfsSitePath)
    conf.addResource(coreSite)
    conf.addResource(hdfsSite)
    fs = FileSystem.get(conf)
    fs
  }

  def appendToFile(fileSystem: FileSystem, content: String, dest: String): String = {
    val destPath = new Path(dest)
    if (!fileSystem.exists(destPath)) {
      println("File doesn't exist")
      fileSystem.create(destPath)
      return "Failure"
    }
    val isAppendable = fileSystem.getConf.get("dfs.support.append").toBoolean
    println(s"isAppendable: ${isAppendable}")
    if (isAppendable) {
      val fs_append = fileSystem.append(destPath)
      val writer = new PrintWriter(fs_append)
      writer.append(content)
      writer.flush()
      fs_append.hflush()
      writer.close()
      fs_append.close()
      "Success"
    }
    else {
      println("Please set the dfs.support.append property to true")
      "Failure"
    }
  }

  def readFromHdfs(fileSystem: FileSystem, hdfsFilePath: String): String = {
    val hdfsPath = new Path(hdfsFilePath)
    val fileContent = new StringBuilder("")
    try {
      val bfr = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)))
      var str = bfr.readLine()
      while (str != null) {
        fileContent.append(str + "\n")
        str = bfr.readLine()
      }
    } catch {
      case ex: IOException =>
        println("----------Could not read from HDFS---------\n")
    }
    fileContent.toString
  }

  def closeFileSystem(fileSystem: FileSystem): Unit = {
    try
      fileSystem.close()
    catch {
      case ex: IOException =>
        println("----------Could not close the FileSystem----------")
    }
  }

  def main(args: Array[String]): Unit = {
    val coreSite = "/usr/local/Cellar/hadoop/2.8.1/libexec/etc/hadoop/core-site.xml"
    val hdfsSite = "/usr/local/Cellar/hadoop/2.8.1/libexec/etc/hadoop/hdfs-site.xml"
    val fileSystem = configureFileSystem(coreSite, hdfsSite)

    val hdfsFilePath = "hdfs://localhost:9000/test.json"
    val res = appendToFile(fileSystem, "It's never too late" + " to start something good.\n ", hdfsFilePath)

    if (res.equalsIgnoreCase("success")) {
      println("Successfully appended to file")
      val content = readFromHdfs(fileSystem, hdfsFilePath)
      println(">>>>Content read from file<<<<\n" + content)
    }
    else println("couldn't append to file")

    closeFileSystem(fileSystem)
  }
}
