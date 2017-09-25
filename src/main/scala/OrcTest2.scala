import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, LongColumnVector, VectorizedRowBatch}
import org.apache.orc.{OrcFile, RecordReader, TypeDescription}

object OrcTest2 extends App {
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://localhost:9000")
//  System.setProperty("HADOOP_USER_NAME", "hscho")
    val fs = FileSystem.get(conf)
    fs.delete(new Path("/myfile.orc"), true)

  val schema: TypeDescription = TypeDescription.fromString("struct<x:int,y:int>")
  val writer = OrcFile.createWriter(new Path("/myfile.orc"), OrcFile.writerOptions(conf).setSchema(schema))

  val batch: VectorizedRowBatch = schema.createRowBatch()

  import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector

  val x = batch.cols(0).asInstanceOf[LongColumnVector]
  val y = batch.cols(1).asInstanceOf[LongColumnVector]

  var r = 0
  while ( {
    r < 10000
  }) {
    val row = {
      batch.size += 1; batch.size - 1
    }
    x.vector(row) = r
    y.vector(row) = r * 3
    // If the batch is full, write it out and start over.
    if (batch.size == batch.getMaxSize) {
      writer.addRowBatch(batch)
      batch.reset
    }

    {
      r += 1; r
    }
  }
  if (batch.size != 0) {
    writer.addRowBatch(batch)
    batch.reset
  }

  writer.close()

  val reader = OrcFile.createReader(new Path("/myfile.orc"), OrcFile.readerOptions(conf))
  val batch2 = reader.getSchema.createRowBatch()
  val rows: RecordReader = reader.rows()
  while (rows.nextBatch(batch2)) {
    println("##1")
    import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
    val intVector = batch.cols(0).asInstanceOf[LongColumnVector]
    val longVector = batch.cols(1).asInstanceOf[LongColumnVector]


    var r = 0
    while ( {
      r < batch2.size
    }) {
      val intValue = intVector.vector(r).toInt
      val longValue = longVector.vector(r)
      println(s"###### $intValue, $longValue #######")

      {
        r += 1; r - 1
      }
    }
  }

  rows.close()
}
