package fs2
package hazelcast

import org.scalatest._

class Suites1[A](a: => A)(suites: (A => Suite)*)(shutdown: A => Unit) extends Suites with BeforeAndAfterAll { self =>

  val value = a

  lazy val nested = suites.map(_.apply(value))
  override val nestedSuites: collection.immutable.IndexedSeq[Suite] = Vector.empty ++ nested
  override def toString: String = s"${suiteName}(${nested.map(_.suiteName).mkString(",")})"
  override def afterAll(): Unit = shutdown(value)
}


object TestUtils {
  implicit val S: Strategy = Strategy.fromFixedDaemonPool(Runtime.getRuntime.availableProcessors)
  implicit val scheduler: Scheduler = Scheduler.fromFixedDaemonPool(1)
}

import com.hazelcast.{core => hz}

final class HazelcastNodeSpec extends Suites1(hz.Hazelcast.newHazelcastInstance())(
  new DMapSpec(_)
)(_.shutdown())
