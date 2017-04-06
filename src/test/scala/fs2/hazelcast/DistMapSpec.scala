package fs2
package hazelcast

import org.scalatest._

import com.hazelcast.{core => hz}

import java.util.concurrent.atomic.AtomicInteger

import TestUtils._

final class DistMapSpec(@transient hazelcast: hz.HazelcastInstance) extends FlatSpec with Matchers with Serializable {

  val serialVersionUID = -7720764477152944955L

  @transient val map: DistMap[Task, Int, String] = DistMap(hazelcast.getMap("foo"))

  "A DistMap" should "put and get values" in {
    val r = for {
      _ <- map.put(1, "foo")
      a <- map.get(1)
    } yield a
    r.unsafeRun should be(Some("foo"))
  }

  it should "modify an existing value" in {
    val r = for {
      _ <- map.put(2, "foo")
      _ <- map.modify(2, _ => "bar")
      a <- map.get(2)
    } yield a
    r.unsafeRun should be(Some("bar"))
  }

  it should "modifyLocal an existing value" in {
    val r = for {
      _ <- map.put(3, "foo")
      _ <- map.modifyLocal(3, _ => "bar")
      a <- map.get(3)
    } yield a
    r.unsafeRun should be(Some("bar"))
  }

  it should "read an existing value" in {
    val r = for {
      _ <- map.put(4, "foo")
      l <- map.reader(4, _.length)
    } yield l
    r.unsafeRun should be(3)
  }

  it should "readLocal an existing value" in {
    val r = for {
      _ <- map.put(5, "foo")
      l <- map.readerLocal(5, _.length)
    } yield l
    r.unsafeRun should be(3)
  }
}


