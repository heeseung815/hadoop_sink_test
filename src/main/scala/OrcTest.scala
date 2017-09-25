
import org.apache.crunch.types.orc.OrcUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, OrcStruct, RecordReader, Writer}
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.Text

object OrcTest extends App {

  println("@11")
  val conf = new Configuration()
  conf.set("fs.default.name", "hdfs://localhost:9000")
//  System.setProperty("HADOOP_USER_NAME", "hscho")
//  val schema = TypeDescription.fromString("struct<test_value:string>")

  val typeStr = "struct<test_value:string,value2:string>"
  val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr)
  val inspector = OrcStruct.createObjectInspector(typeInfo)
//  val writer1 = OrcFile.createWriter(new Path("test.orc"), OrcFile.writerOptions(conf).setSchema(schema))

  val text = "11111"
  val text2 = "2222"
  val orcLine = OrcUtils.createOrcStruct(typeInfo, new Text(text), new Text(text2))

  println(orcLine.toString)
  println(orcLine.getNumFields)

//  val fs = FileSystem.get(conf)
//  fs.delete(new Path("/test.orc"), true)
//  val os = fs.append(new Path("/test.orc"))
//  os.write(orcLine.toString.getBytes())
//  os.close()
//
//
  val tempPath = new Path("/Users/hscho/test123.orc")
  val writer: Writer = OrcFile.createWriter(tempPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000))
  writer.addRow(orcLine)
  writer.close()


  val reader = OrcFile.createReader(tempPath, OrcFile.readerOptions(conf))
  val temp: RecordReader = reader.rows()
  while (temp.hasNext) {
    println(s"####: ${temp.next()}")
  }

  temp.close()



//  def createOrcFile(input: String): Unit = {
//    val typeStr = "struct<string_value:string>"
////    val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr)
//    val schema = TypeDescription.fromString(typeStr)
////    val inspector = OrcStruct.createObjectInspector(typeInfo)
//
//
//    val inputTokens = input.split("\\t")
//
//    val orcLine = OrcUtils.createOrcStruct(
//      typeInfo,
//      new Text(inputTokens(0)),
//      new ShortWritable(inputTokens(1).toShort),
//      new IntWritable(inputTokens(2).toInt),
//      new LongWritable(inputTokens(3).toLong),
//      new DoubleWritable(inputTokens(4).toDouble),
//      new FloatWritable(inputTokens(5).toFloat)
//    )
//
//    val conf = new Configuration()
//    val tempPath = new Path("/tmp/test.orc")
//    val writer: Writer = OrcFile.createWriter(tempPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000))
//    writer.addRow(orcLine)
//    writer.close()
//  }

}
