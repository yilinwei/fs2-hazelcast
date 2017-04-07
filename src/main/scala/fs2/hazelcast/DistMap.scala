package fs2
package hazelcast

import fs2._
import fs2.util._
import fs2.util.syntax._
import cats.data._

import com.hazelcast.{core => hz}
import scala.collection.JavaConverters._

import Hazelcast._

object DistMap {

  def containsKey[F[_], K, V](k: K)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Boolean] =
    ReaderT { map =>
      F.delay { map.containsKey(k) }
    }

  def putAll[F[_], K, V](kv: Map[K, V])(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] = ReaderT { map =>
    F.delay { map.putAll(kv.asJava) }
  }

  def reader[F[_] : Async, K, V, A](k: K, f: V => A): ReaderT[F, hz.IMap[K, V], A] =
    ReaderT { map =>
      run { map.submitToKey(k, new ReadProcessor[K, V, A]((_, v) => f(v))).asInstanceOf[hz.ICompletableFuture[A]] }
    }

  def readerLocal[F[_] : Async, K, V, A](k: K, f: V => A): ReaderT[F, hz.IMap[K, V], A] =
    ReaderT { map => run(map.getAsync(k)).map(f) }

  def delete[F[_], K, V](k: K)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => F.delay(map.delete(k)) }

  def get[F[_] : Async, K, V](k: K): ReaderT[F, hz.IMap[K, V], Option[V]] =
    ReaderT { map => run(map.getAsync(k)).map(Option(_)) }

  def put[F[_] : Async, K, V](k: K, v: V): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => runEffect(map.putAsync(k, v)) }


  def modify[F[_], K, V](k: K, f: V => V)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => runEffect { map.submitToKey(k, new WriteProcessor[K, V]((_, v) => f(v))) } }

  def lock[F[_], K, V, A](k: K)(fa: ReaderT[F, hz.IMap[K, V], A])(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], A] = ReaderT { map =>
    val run = fa.apply(map)
    Stream
      .bracket(F.delay(map.lock(k)))(_ => Stream.eval(run), _ => F.delay(map.unlock(k)))
      .runLast.map(_.get)
  }

  def modifyLocal[F[_], K, V](k: K, f: V => V)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    lock(k) {
      ReaderT { map =>
        F.delay {
          Option(map.get(k)) match {
            case Some(a) =>
              map.put(k, f(a))
              F.pure(())
            case None =>
              F.fail[Unit](new IllegalArgumentException(s"key $k is not present, cannot modify"))
          }
        }.flatMap(identity)
      }
    }


  def mapReduce[F[_], K, V, B](b: B, f: (K, V) => B, g: (B, B) => B)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], B] =
    ReaderT { map =>
      F.delay {
        map.aggregate(new MapReduce[java.util.Map.Entry[K, V], B](b, entry => f(entry.getKey, entry.getValue), g))
      }
    }

  def project[F[_], K, V, B](f: (K, V) => B)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Seq[B]] =
    ReaderT { map =>
      F.delay {
        map
          .project(new ProjectionFunction[java.util.Map.Entry[K, V], B](entry => f(entry.getKey, entry.getValue)))
          .asScala
          .toSeq
      }
    }

  def collect[F[_], K, V, B](pf: PartialFunction[(K, V), B])(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Seq[B]] =
    ReaderT { map =>
      F.delay {
        map
          .project(
            new ProjectionFunction[java.util.Map.Entry[K, V], B](entry => pf(entry.getKey -> entry.getValue)),
            new EntryPredicate[K, V]((k, v) => pf.isDefinedAt(k -> v))
          )
          .asScala
          .toSeq
      }
    }

  def findKeys[F[_], K, V](f: (K, V) => Boolean)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Set[K]] =
    ReaderT { map =>
      F.delay { map.keySet(new EntryPredicate[K, V](f)).asScala.toSet }
    }

  def removeAll[F[_], K, V](implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map =>
      F.delay { map.destroy() }
    }


  def listen[F[_], K, V](implicit F: Async[F]): ReaderT[Stream[F, ?], hz.IMap[K, V], DMapKVEvent[K, V]] =
    ReaderT { map =>
      for {
        queue <- Stream.eval(async.unboundedQueue[F, DMapKVEvent[K, V]])
        values <- Stream.bracket[F, String, DMapKVEvent[K, V]](
          F.delay {
            map.addEntryListener(new EntryKVHandler[K, V](event => queue.enqueue1(event).unsafeRunAsync(_ => ())), true)
          }
        )(_ => queue.dequeue, id => F.delay(map.removeEntryListener(id)).map(_ => ()))
      } yield values
    }

  def apply[F[_] : Async, K, V](map: hz.IMap[K, V]): DistMap[F, K, V] = new DistMap[F, K, V] {

    def containsKey(k: K): F[Boolean] = DistMap.containsKey(k).apply(map)
    def get(k: K): F[Option[V]] = DistMap.get(k).apply(map)
    def put(k: K, v: V): F[Unit] = DistMap.put(k, v).apply(map)
    def putAll(vs: Map[K, V]): F[Unit] = DistMap.putAll(vs).apply(map)
    def modifyLocal(k: K, f: V => V): F[Unit] = DistMap.modifyLocal(k, f).apply(map)
    def modify(k: K, f: V => V): F[Unit] = DistMap.modify(k, f).apply(map)
    def reader[A](k: K, f: V => A) = DistMap.reader(k, f).apply(map)
    def readerLocal[A](k: K, f: V => A): F[A] = DistMap.readerLocal(k, f).apply(map)
    def mapReduce[B](b: B)(f: (K, V) => B, g: (B, B) => B): F[B] = DistMap.mapReduce(b, f, g).apply(map)
    def project[B](f: (K, V) => B): F[Seq[B]] = DistMap.project(f).apply(map)
    def collect[B](pf: PartialFunction[(K, V), B]): F[Seq[B]] = DistMap.collect(pf).apply(map)
    def findKeys(f: (K, V) => Boolean): F[Set[K]] = DistMap.findKeys(f).apply(map)
    def removeAll: F[Unit] = DistMap.removeAll.apply(map)

    def listen: Stream[F, DMapKVEvent[K, V]] = DistMap.listen.apply(map)
    
    override def toString: String = s"${map.getName}(${map.getServiceName}, ${map.getLocalMapStats})"
  }
}

import cats._

sealed trait DMapKVEvent[+K, +V]

object DMapKVEvent {
  case class Cleared(n: Long) extends DMapKVEvent[Nothing, Nothing]
  case class Evicted(n: Long) extends DMapKVEvent[Nothing, Nothing]
  case class Add[K, V](k: K, v: V) extends DMapKVEvent[K, V]
  case class Remove[K, V](k: K, v: V) extends DMapKVEvent[K, V]
  case class Update[K, V](k: K, prev: V, next: V) extends DMapKVEvent[K, V]
  case class Merged[K, V](k: K, prev: V, merge: V, v: V) extends DMapKVEvent[K, V]
  case class Evict[K, V](k: K, v: V) extends DMapKVEvent[K, V]
}

trait DistMap[F[_], K, V] extends Serializable {

  def containsKey(k: K): F[Boolean]
  def get(k: K): F[Option[V]]
  def put(k: K, v: V): F[Unit]
  def putAll(vs: Map[K, V]): F[Unit]
  def modifyLocal(k: K, f: V => V): F[Unit]
  def modify(k: K, f: V => V): F[Unit]
  def reader[A](k: K, f: V => A): F[A]
  def readerLocal[A](k: K, f: V => A): F[A]
  def mapReduce[B](b: B)(f: (K, V) => B, g: (B, B) => B): F[B]
  def project[B](f: (K, V) => B): F[Seq[B]]
  def collect[B](pf: PartialFunction[(K, V), B]): F[Seq[B]]
  def findKeys(f: (K, V) => Boolean): F[Set[K]]
  def removeAll: F[Unit]
  def listen: Stream[F, DMapKVEvent[K, V]]

  def fold(implicit M: Monoid[V]): F[V] = mapReduce(M.empty)((_, v) => v, M.combine)
}
