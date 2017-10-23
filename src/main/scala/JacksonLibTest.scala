import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

//object JsonUtil {
//  val mapper = new ObjectMapper() with ScalaObjectMapper
//  mapper.registerModule(DefaultScalaModule)
//  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
//
//  def toJson(value: Map[Symbol, Any]): String = {
//    toJson(value map { case (k,v) => k.name -> v})
//  }
//
//  def toJson(value: Any): String = {
//    mapper.writeValueAsString(value)
//  }
//
//  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)
//
//  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
//    mapper.readValue[T](json)
//  }
//}

object JacksonLibTest extends App {
//  val originalMap: Map[String, List[Int]] = Map("a" -> List(1,2), "b" -> List(3,4,5), "c" -> List())
  val originalMap2 = Map("a" -> "xxx", "b" -> 1, "c" -> "yyy")

  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  val json2: String = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(originalMap2)
  println("======================================================")
//  println(originalMap.toString())
  println(originalMap2.toString())
  println(json2)

  /*
 * (Un)marshalling a simple map
 */
//  val json = JsonUtil.toJson(originalMap)
  // json: String = {"a":[1,2],"b":[3,4,5],"c":[]}
//  println("==================")
//  val json =
//  println(json)
//  val map = JsonUtil.toMap[Seq[Int]](json)
  // map: Map[String,Seq[Int]] = Map(a -> List(1, 2), b -> List(3, 4, 5), c -> List())


}


