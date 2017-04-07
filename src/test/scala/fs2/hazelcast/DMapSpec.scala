package fs2
package hazelcast

import org.scalatest._

import com.hazelcast.{core => hz}

import java.util.concurrent.atomic.AtomicInteger

import TestUtils._

import cats.implicits._

import fs2._
import scala.concurrent.duration._

@SerialVersionUID(20170704)
final class DMapSpec(@transient hazelcast: hz.HazelcastInstance) extends FlatSpec with Matchers with Serializable with BeforeAndAfterEach {

  @transient var map: DMap[Task, Int, String] = _

  override def beforeEach(): Unit = {
    map = DMap[Task, Int, String](hazelcast.getMap("foo"))
  }

  override def afterEach(): Unit = {
    map.removeAll.unsafeRun
  }

  "A DMap" should "put and get values" in {
    val r = for {
      _ <- map.put(1, "foo")
      a <- map.get(1)
    } yield a
    r.unsafeRun should be(Some("foo"))
  }

  it should "check whether a key exists" in {
    val r = for {
      _ <- map.put(1, "foo")
      result <- map.containsKey(1)
    } yield result
    r.unsafeRun should be(true)
  }

  it should "find keys" in {
    val r = for {
      _ <- map.putAll(Map(1 -> "foo", 2 -> "bar"))
      result <- map.findKeys((i, _) => i < 2)
    } yield result
    r.unsafeRun should contain (1)
  }

  it should "get a null value as a None" in {
    val r = for {
      a <- map.get(1)
    } yield a
    r.unsafeRun should be(None)
  }

  it should "modify an existing value" in {
    val r = for {
      _ <- map.put(1, "foo")
      _ <- map.modify(1, _ => "bar")
      a <- map.get(1)
    } yield a
    r.unsafeRun should be(Some("bar"))
  }

  it should "modifyLocal an existing value" in {
    val r = for {
      _ <- map.put(1, "foo")
      _ <- map.modifyLocal(1, _ => "bar")
      a <- map.get(1)
    } yield a
    r.unsafeRun should be(Some("bar"))
  }

  it should "read an existing value" in {
    val r = for {
      _ <- map.put(1, "foo")
      l <- map.reader(1, _.length)
    } yield l
    r.unsafeRun should be(3)
  }

  it should "readLocal an existing value" in {
    val r = for {
      _ <- map.put(1, "foo")
      l <- map.readerLocal(1, _.length)
    } yield l
    r.unsafeRun should be(3)
  }

  it should "project values" in {
    val r = for {
      _ <- map.putAll(Map(0 -> "fu", 1 -> "bar"))
      result <- map.project((_, v) => v.length)
    } yield result
    r.unsafeRun should contain allOf (2, 3)
  }

  it should "collect values" in {
    val r = for {
      _ <- map.putAll(Map(0 -> "fu", 1 -> "bar", 2 -> "car"))
      result <- map.collect { case (0, _) => 42 }
    } yield result
    r.unsafeRun should contain (42)
  }

  it should "fold an existing value" in {
    val map = DMap[Task, Int, Int](hazelcast.getMap("bar"))
    val r = for {
      _ <- map.put(0, 90)
      _ <- map.put(1, 10)
      sum <- map.fold
    } yield sum
    r.unsafeRun should be(100)
    map.removeAll.unsafeRun
  }

  it should "listen to kv updates" in {
    val update = fs2.time.every(300 milliseconds).flatMap(_ => Stream.eval(map.put(1, "foo")))
    val r = update.mergeDrainL(map.listen).take(1).runLog
    r.unsafeRun should be(Vector(DMapKVEvent.Update(1, "foo", "foo")))
  }

  it should "listen to kv adds" in {
    val add = fs2.time.sleep[Task](300 milliseconds).flatMap(_ => Stream.eval(map.put(1, "foo")))
    val r = add.mergeDrainL(map.listen).take(1).runLog
    r.unsafeRun should be(Vector(DMapKVEvent.Add(1, "foo")))
  }

  it should "listen to kv removes" in {
    map.put(0, "foo").unsafeRun
    val remove = fs2.time.sleep[Task](300 milliseconds).flatMap(_ => Stream.eval(map.remove(0)))
    val r = remove.mergeDrainL(map.listen).take(1).runLog
    r.unsafeRun should be(Vector(DMapKVEvent.Remove(0, "foo")))
  }

  it should "listen to k updates" in {
    val update = fs2.time.every(300 milliseconds).flatMap(_ => Stream.eval(map.put(1, "bar")))
    val r = update.mergeDrainL(map.listenKeys).take(1).runLog
    r.unsafeRun should be(Vector(DMapKEvent.Update(1)))
  }

  it should "listen to k adds" in {
    val add = fs2.time.sleep[Task](300 milliseconds).flatMap(_ => Stream.eval(map.put(1, "foo")))
    val r = add.mergeDrainL(map.listenKeys).take(1).runLog
    r.unsafeRun should be(Vector(DMapKEvent.Add(1)))
  }

  it should "listen to k removes" in {
    map.put(0, "foo").unsafeRun
    val remove = fs2.time.sleep[Task](300 milliseconds).flatMap(_ => Stream.eval(map.remove(0)))
    val r = remove.mergeDrainL(map.listenKeys).take(1).runLog
    r.unsafeRun should be(Vector(DMapKEvent.Remove(0)))
  }
}


