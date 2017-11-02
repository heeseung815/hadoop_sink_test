import java.sql.Timestamp

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import play.api.libs.json._
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

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
  def parseJsonValueAny(value:Any) : JsValue = {
    value match {
      case value: String => JsString(value)
      case value: Int => JsNumber(value)
      case value: Long => JsNumber(value)
      case value: BigInt => JsNumber(value.longValue())
      case value: Double => JsNumber(value)
      case value: Float => JsNumber(value.toDouble)
      case value: Boolean => JsBoolean(value)
      case value: Timestamp => println("ddd"); JsString(value.toString)
      case value: Map[String, Any] => {
        var ret = Json.obj()
        value.foreach {
          case (k: String, v: Any) =>
            if (k.equals("timestamp")) ret = ret + (k, parseJsonValueAny(new Timestamp(v.asInstanceOf[Long])))
            else ret = ret + (k, parseJsonValueAny(v))
        }
        ret
      }
      case value: Seq[Any] => println("1"); JsArray(value.map(v => parseJsonValueAny(v)))
      case value: Array[Int] => println("2"); JsArray(value.map(v => parseJsonValueAny(v)))
      case _ => JsNull
    }
  }

  //new Timestamp(dataValue.asInstanceOf[Long])
  val originalMap = Map("x" -> Array(1, 2, 3), "y" -> List(3,4,5), "timestamp" -> System.currentTimeMillis())
  val schema = "x, b, timestamp"
  val keys: Array[String] = schema.replaceAll(" ", "").split(",")
  val result: JsValue = parseJsonValueAny(originalMap.filterKeys(keys.contains(_)))
//  val originalMap2 = Map("a" -> "xxx", "b" -> 1, "c" -> "yyy")

//  val mapper = new ObjectMapper() with ScalaObjectMapper
//  mapper.registerModule(DefaultScalaModule)
//  val json2: String = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(originalMap)
  println("======================================================")
  println(s"${Json.stringify(result)}")
  println("======================================================")
  println(Json.prettyPrint(result))
//  println(originalMap.toString())
//  println(originalMap.toString())
//  println(json2)

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


