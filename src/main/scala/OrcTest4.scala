/*
import java.util.{Random, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, VectorizedRowBatch}
import org.apache.orc.{OrcFile, RecordReader, TypeDescription, Writer}


object OrcTest4 extends App {
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://localhost:9000")
  val fs = FileSystem.get(conf)
  fs.delete(new Path("/myfile4.orc"), true)
  fs.close()

  val rand = new Random()
  val schema = TypeDescription.createStruct.addField("string_value", TypeDescription.createString)
  val writer: Writer = OrcFile.createWriter(new Path("/myfile4.orc"), OrcFile.writerOptions(conf).setSchema(schema))

  val batch: VectorizedRowBatch = schema.createRowBatch
  val stringVector: BytesColumnVector = batch.cols(0).asInstanceOf[BytesColumnVector]

  // batch.getMaxSize: 1024
  // VectorizedRowBatch 란?
  // 1024 행의 데이터가 포함된 VectorizedRowBatch의 인스턴스로 데이터가 ORC로 전달된다.
  // 초점은 속도와 데이터 필드에 직접 액세스 하는 것이다.
  // cols는 ColumnVector 배열이고, size는 행의 수 이다.
  println(s"# batch info: ${batch.cols.length}, ${batch.size}, ${batch.getMaxSize}")
  var r = 0
  while (r < 2) {
    println(s"# batch.size: ${batch.size}")
    val row = batch.size
    batch.size += 1
    println(s"###### row: ${row}")
    stringVector.setVal(row, UUID.randomUUID.toString.getBytes)
    writer.addRowBatch(batch)
    // batch size가 1024가 되면 파일에 기록하고 batch를 리셋한다
    if (batch.size == batch.getMaxSize) {
      println("@#@@@")
//      writer.addRowBatch(batch)
//      batch.reset()
    }

    r += 1
  }
/*
  if (batch.size != 0) {
    println("!@#!@#!@#!@#")
    writer.addRowBatch(batch)
    batch.reset()
  }
*/
  writer.close()

  val reader = OrcFile.createReader(new Path("/myfile4.orc"), OrcFile.readerOptions(conf))
  println(s"# reader: ${reader.getSchema.toJson}")
  println(s"# reader: ${reader.getSchema.toString}")
  val batch2 = reader.getSchema.createRowBatch()
  val rows: RecordReader = reader.rows()
  println(s"# rows: ${rows.getRowNumber}")
  println(s"# batch2 size: ${batch2.size}")
  while (rows.nextBatch(batch2)) {
    println(s"# started...${batch2.size}")
    val stringVector = batch2.cols(0).asInstanceOf[BytesColumnVector]

    var r = 0
    println(s"${batch2.size}")
    while ( {
      r < batch2.size
    }) {
      val stringValue = new String(stringVector.vector(r), stringVector.start(r), stringVector.length(r))
      println(s"# $stringValue")

      r += 1
    }
  }
  rows.close()
/*
//  val fs2 = FileSystem.get(conf)
//  val status = fs2.getFileStatus(new Path("/myfile4.orc"))
//  println(s"############### status: ${status.getLen}")
  val reader = OrcFile.createReader(new Path("/myfile4.orc"), OrcFile.readerOptions(conf))
  val batch2 = reader.getSchema.createRowBatch()
  val rows: RecordReader = reader.rows()
  while (rows.nextBatch(batch2)) {
    println(s"# started...")
    val stringVector = batch2.cols(0).asInstanceOf[BytesColumnVector]
    //    val stringVector2 = batch2.cols(1).asInstanceOf[BytesColumnVector]

    var r = 0
    println(s"###############s${batch2.size}")
    while ( {
      r < batch2.size
    }) {
      val stringValue = new String(stringVector.vector(r), stringVector.start(r), stringVector.length(r))
      //      val stringValue2 = new String(stringVector2.vector(r), stringVector2.start(r), stringVector2.length(r))
      println(s"############# $stringValue")
      //      println(s"# $stringValue, $stringValue2")

      {
        r += 1; r - 1
      }
    }
  }

  rows.close()
*/
}
*/