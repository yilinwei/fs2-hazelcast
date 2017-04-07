package fs2
package hazelcast

import com.hazelcast.{core => hz}
import com.hazelcast.map._
import com.hazelcast.map.listener._
import com.hazelcast.aggregation._
import com.hazelcast.projection._
import com.hazelcast.query._

import fs2._
import fs2.util._
import fs2.util.syntax._

object Hazelcast {
  def run[F[_], A](fut: => hz.ICompletableFuture[A])(implicit F: Async[F]): F[A] =
    F.async { cb =>
      F.delay {
        fut.andThen(new hz.ExecutionCallback[A] {
          def onFailure(t: Throwable): Unit = cb(Left(t))
          def onResponse(a: A): Unit = cb(Right(a))
        })
      }
    }

  def runEffect[F[_] : Async, A](fut: => hz.ICompletableFuture[A]): F[Unit] =
    run(fut).map(_ => ())
}


import Hazelcast._

private[hazelcast] final class WriteBackupProcessor[K, V](f: (K, V) => V) extends EntryBackupProcessor[K, V] {

  import java.util.Map

  def processBackup(entry: Map.Entry[K, V]): Unit = 
    entry.setValue(f(entry.getKey, entry.getValue))
}

private[hazelcast] final class WriteProcessor[K, V](f: (K, V) => V) extends EntryProcessor[K, V] {

  import java.util.Map

  def process(entry: Map.Entry[K, V]): Object = {
    entry.setValue(f(entry.getKey, entry.getValue))
    null
  }

  val getBackupProcessor = new WriteBackupProcessor(f)
}

private[hazelcast] final class ReadProcessor[K, V, A](f: (K, V) => A) extends EntryProcessor[K, V] {

  import java.util.Map

  def process(entry: Map.Entry[K, V]): AnyRef = 
    f(entry.getKey, entry.getValue).asInstanceOf[AnyRef]

  val getBackupProcessor = null
}

private[hazelcast] final class MapReduce[A, B](b: B, f: A => B, g: (B, B) => B) extends Aggregator[A, B] {

  var current: B = b

  def accumulate(a: A): Unit = current = g(f(a), current)
  def combine(that: Aggregator[_, _]): Unit = current = g(current, that.asInstanceOf[MapReduce[A, B]].current)
  def aggregate: B = current
}

private[hazelcast] final class EntryPredicate[K, V](f: (K, V) => Boolean) extends Predicate[K, V] {

  import java.util.Map

  def apply(entry: Map.Entry[K, V]): Boolean = f(entry.getKey, entry.getValue)
}

private[hazelcast] final class ProjectionFunction[A, B](f: A => B) extends Projection[A, B] {
  def transform(a: A): B = f(a)
}

private[hazelcast] final class EntryKVHandler[K, V](cb: DMapKVEvent[K, V] => Unit)
    extends MapListener
    with MapClearedListener
    with MapEvictedListener
    with EntryAddedListener[K, V]
    with EntryEvictedListener[K, V]
    with EntryRemovedListener[K, V]
    with EntryMergedListener[K, V]
    with EntryUpdatedListener[K, V]
{


  def entryAdded(entry: hz.EntryEvent[K, V]): Unit = 
    cb(DMapKVEvent.Add(entry.getKey, entry.getValue))

  def entryRemoved(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKVEvent.Remove(entry.getKey, entry.getOldValue))

  def entryUpdated(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKVEvent.Update(entry.getKey, entry.getOldValue, entry.getValue))

  def entryMerged(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKVEvent.Merged(entry.getKey, entry.getOldValue, entry.getMergingValue, entry.getValue))

  def entryEvicted(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKVEvent.Evict(entry.getKey, entry.getValue))

  def mapCleared(event: hz.MapEvent): Unit =
    cb(DMapKVEvent.Cleared(event.getNumberOfEntriesAffected))

  def mapEvicted(event: hz.MapEvent): Unit =
    cb(DMapKVEvent.Evicted(event.getNumberOfEntriesAffected))
}

private[hazelcast] final class EntryKHandler[K, V](cb: DMapKEvent[K] => Unit)
    extends MapListener
    with MapClearedListener
    with MapEvictedListener
    with EntryAddedListener[K, V]
    with EntryEvictedListener[K, V]
    with EntryRemovedListener[K, V]
    with EntryMergedListener[K, V]
    with EntryUpdatedListener[K, V]
{

  def entryAdded(entry: hz.EntryEvent[K, V]): Unit = 
    cb(DMapKEvent.Add(entry.getKey))

  def entryRemoved(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKEvent.Remove(entry.getKey))

  def entryUpdated(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKEvent.Update(entry.getKey))

  def entryMerged(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKEvent.Merged(entry.getKey))

  def entryEvicted(entry: hz.EntryEvent[K, V]): Unit =
    cb(DMapKEvent.Evict(entry.getKey))

  def mapCleared(event: hz.MapEvent): Unit =
    cb(DMapKEvent.Cleared(event.getNumberOfEntriesAffected))

  def mapEvicted(event: hz.MapEvent): Unit =
    cb(DMapKEvent.Evicted(event.getNumberOfEntriesAffected))
}
