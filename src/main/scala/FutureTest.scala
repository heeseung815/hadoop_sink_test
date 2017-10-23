import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object FutureTest extends App {
  val futures: Seq[Future[String]] = (0 to 9) map {
    i => Future {
      val s = i.toString
      println(s)
      s
    }
  }

//  val f: Future[String] = Future.reduceLeft(futures.toVector)((s1, s2) => s1 + s2)
  val f: Future[String] = Future.reduceLeft(futures.to[collection.immutable.Iterable])((s1, s2) => s1 + s2)

  val n = Await.result(f, Duration.Inf)
}
