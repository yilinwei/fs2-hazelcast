package fs2
package hazelcast

import org.scalatest._

import com.hazelcast.{core => hz}

import java.util.concurrent.atomic.AtomicInteger

import TestUtils._

import cats.implicits._

import fs2._
import fs2.interop.cats._
import scala.concurrent.duration._

@SerialVersionUID(20170704)
final class DMapSpec(@transient val hazelcast: hz.HazelcastInstance) extends FlatSpec with Matchers with Serializable with BeforeAndAfterEach {

  @transient var map: DMap[Task, Int, String] = _

  val delay = fs2.time.sleep[Task](30 milliseconds)

  override def beforeEach(): Unit = {
    map = DMap[Task, Int, String](hazelcast.getMap("foo"))
  }

  override def afterEach(): Unit = {
    map.removeAll.unsafeRun
  }

  "A DMap" should "put and get values" in {
    val r = map.put(1, "foo") >> map.get(1)
    r.unsafeRun should be(Some("foo"))
  }

  it should "check whether a key exists" in {
    val r = map.put(1, "foo") >> map.containsKey(1)
    r.unsafeRun should be(true)
  }

  it should "find keys" in {
    val r = map.putAll(Map(1 -> "foo", 2 -> "bar")) >> map.findKeys((i, _) => i < 2)
    r.unsafeRun should contain (1)
  }

  it should "get a null value as a None" in {
    map.get(1).unsafeRun should be(None)
  }

  it should "modify an existing value" in {
    val r = map.put(1, "foo") >> map.modify(1, _ => "bar") >> map.get(1)
    r.unsafeRun should be(Some("bar"))
  }

  it should "modifyLocal an existing value" in {
    val r = map.put(1, "foo") >> map.modifyLocal(1, _ => "bar") >> map.get(1)
    r.unsafeRun should be(Some("bar"))
  }

  it should "read an existing value" in {
    val r = map.put(1, "foo") >> map.reader(1, _.length)
    r.unsafeRun should be(3)
  }

  it should "readLocal an existing value" in {
    val r = map.put(1, "foo") >> map.readerLocal(1, _.length)
    r.unsafeRun should be(3)
  }

  it should "project values" in {
    val r = map.putAll(Map(0 -> "fu", 1 -> "bar")) >> map.project((_, v) => v.length)
    r.unsafeRun should contain allOf (2, 3)
  }

  it should "collect values" in {
    val r = map.putAll(Map(0 -> "fu", 1 -> "bar", 2 -> "car")) >> map.collect { case (0, _) => 42 }
    r.unsafeRun should contain (42)
  }

  it should "fold an existing value" in {
    val map = DMap[Task, Int, Int](hazelcast.getMap("bar"))
    val r = map.put(0, 90) >> map.put(1, 10) >> map.fold
    r.unsafeRun should be(100)
  }

  it should "remove all values" in {
    val r = map.put(1, "foo") >> map.removeAll >> map.findKeys((_, _) => true)
    r.unsafeRun should be(Set.empty)
  }

  it should "listenCollect to filtered updates in" in {
    val updates = (delay >> Stream.eval(map.put(1, "foo"))).repeat
    val r = updates.mergeDrainL(map.listenCollect {
      case DMapKVEvent.Update(_, _, foo) => foo.length
    }).take(1).runLog
    r.unsafeRun should be(Vector(3))
  }

  it should "listen to kv updates" in {
    val update = delay >> Stream.eval(map.put(1, "foo"))
    val r = map.put(1, "foo") >> update.mergeDrainL(map.listen).take(1).runLog
    r.unsafeRun should be(Vector(DMapKVEvent.Update(1, "foo", "foo")))
  }

  it should "listen to kv sets" in {
    val set = delay >> Stream.eval(map.set(1, "foo"))
    val r = map.put(1, "foo") >> set.mergeDrainL(map.listen).take(1).runLog
    r.unsafeRun should be(Vector(DMapKVEvent.Update(1, null, "foo")))
  }

  it should "listen to kv adds" in {
    val add = delay >> Stream.eval(map.put(1, "foo"))
    val r = add.mergeDrainL(map.listen).take(1).runLog
    r.unsafeRun should be(Vector(DMapKVEvent.Add(1, "foo")))
  }

  it should "listen to kv removes" in {
    val remove = delay >> Stream.eval(map.remove(0))
    val r = map.put(0, "foo") >> remove.mergeDrainL(map.listen).take(1).runLog
    r.unsafeRun should be(Vector(DMapKVEvent.Remove(0, "foo")))
  }

  it should "listen to k updates" in {
    val update = delay >> Stream.eval(map.put(1, "bar"))
    val r = map.put(1, "bar") >> update.mergeDrainL(map.listenKeys).take(1).runLog
    r.unsafeRun should be(Vector(DMapKEvent.Update(1)))
  }

  it should "listen to k adds" in {
    val add = delay >> Stream.eval(map.put(1, "foo"))
    val r = add.mergeDrainL(map.listenKeys).take(1).runLog
    r.unsafeRun should be(Vector(DMapKEvent.Add(1)))
  }

  it should "listen to k removes" in {
    val remove = delay >> Stream.eval(map.remove(0))
    val r = map.put(0, "foo") >> remove.mergeDrainL(map.listenKeys).take(1).runLog
    r.unsafeRun should be(Vector(DMapKEvent.Remove(0)))
  }

  it should "run atomic updates" in {
    import DMapAtomic._
    val r = map.atomic {
      for {
        _ <- lock(0)
        _ <- put(0, "foo")
        a <- get(0)
        _ <- unlock(0)
      } yield a
    }
    r.unsafeRun should be(Some("foo"))
  }

  it should "cleanup locks within an atomic block" in {
    import DMapAtomic._
    (map.put(0, "foo") >> map.atomic(lock(0)) >> map.get(0)).unsafeRun should be(Some("foo"))
  }



}


