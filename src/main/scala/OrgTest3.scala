import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.{OrcFile, RecordReader, TypeDescription, Writer}
import java.util.Random
import java.util.UUID


object OrgTest3 extends App {
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://localhost:9000")
  val fs = FileSystem.get(conf)
  fs.delete(new Path("/myfile3.orc"), true)

  val rand = new Random()
//  val schema = TypeDescription.createStruct.addField("int_value", TypeDescription.createInt).addField("long_value", TypeDescription.createLong).addField("double_value", TypeDescription.createDouble).addField("float_value", TypeDescription.createFloat).addField("boolean_value", TypeDescription.createBoolean).addField("string_value", TypeDescription.createString)
  val schema = TypeDescription.createStruct.addField("string_value", TypeDescription.createString).addField("test_value", TypeDescription.createString)
  val writer: Writer = OrcFile.createWriter(new Path("/myfile3.orc"), OrcFile.writerOptions(conf).setSchema(schema))


  val batch = schema.createRowBatch
//  val intVector = batch.cols(0).asInstanceOf[LongColumnVector]
//  val longVector = batch.cols(1).asInstanceOf[LongColumnVector]
//  val doubleVector = batch.cols(2).asInstanceOf[DoubleColumnVector]
//  val floatColumnVector = batch.cols(3).asInstanceOf[DoubleColumnVector]
//  val booleanVector = batch.cols(4).asInstanceOf[LongColumnVector]
//  val stringVector = batch.cols(5).asInstanceOf[BytesColumnVector]
  val stringVector = batch.cols(0).asInstanceOf[BytesColumnVector]
  val stringVector2 = batch.cols(1).asInstanceOf[BytesColumnVector]


  var r = 0
  while ({r < 10}) {
    val row = {
      batch.size += 1; batch.size - 1
    }
//    intVector.vector(row) = rand.nextInt
//    longVector.vector(row) = rand.nextLong
//    doubleVector.vector(row) = rand.nextDouble
//    floatColumnVector.vector(row) = rand.nextFloat
//    booleanVector.vector(row) = if (rand.nextBoolean) 1 else 0
    stringVector.setVal(row, UUID.randomUUID.toString.getBytes)
    stringVector2.setVal(row, "Hello World!!!".getBytes)
    if (batch.size == batch.getMaxSize) {
      writer.addRowBatch(batch)
      batch.reset()
    }

    {
      r += 1; r
    }
  }
  if (batch.size != 0) {
    writer.addRowBatch(batch)
    batch.reset()
  }
  writer.close()

  val reader = OrcFile.createReader(new Path("/myfile3.orc"), OrcFile.readerOptions(conf))
  val batch2 = reader.getSchema.createRowBatch()
  val rows: RecordReader = reader.rows()
//  println(s"# batch2 size: ${batch2.size}, ${rows.nextBatch(batch2)}")
  while (rows.nextBatch(batch2)) {
    println("# started...")
//    val intVector = batch.cols(0).asInstanceOf[LongColumnVector]
//    val longVector = batch.cols(1).asInstanceOf[LongColumnVector]
//    val doubleVector = batch.cols(2).asInstanceOf[DoubleColumnVector]
//    val floatVector = batch.cols(3).asInstanceOf[DoubleColumnVector]
//    val booleanVector = batch.cols(4).asInstanceOf[LongColumnVector]
//    val stringVector = batch.cols(5).asInstanceOf[BytesColumnVector]
    val stringVector = batch2.cols(0).asInstanceOf[BytesColumnVector]
    val stringVector2 = batch2.cols(1).asInstanceOf[BytesColumnVector]


    var r = 0
    println(s"${batch2.size}")
    while ( {
      r < batch2.size
    }) {
      println(s"###### ${stringVector.toString(r)} ${stringVector2.toString(r)}")
//      val intValue = intVector.vector(r).toInt
//      val longValue = longVector.vector(r)
//      val doubleValue = doubleVector.vector(r)
//      val floatValue = floatVector.vector(r).toFloat
//      val boolValue = booleanVector.vector(r) != 0
      val stringValue = new String(stringVector.vector(r), stringVector.start(r), stringVector.length(r))
//      val stringValue2 = new String(stringVector2.vector(r), stringVector2.start(r), stringVector2.length(r))
//      System.out.println(intValue + ", " + longValue + ", " + doubleValue + ", " + floatValue + ", " + boolValue + ", " + stringValue)
//      println(s"# $stringValue")
//      println(s"# $stringValue, $stringValue2")

      {
        r += 1; r - 1
      }
    }
  }

  rows.close()

}
