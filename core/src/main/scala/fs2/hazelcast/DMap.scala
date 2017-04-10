package fs2
package hazelcast

import fs2._
import fs2.util.{~> => _, Free => _, _}
import fs2.util.syntax._

import fs2.interop.cats._

import cats._
import cats.data._
import cats.free._

import com.hazelcast.{core => hz}
import com.hazelcast.map.listener._
import scala.collection.JavaConverters._

import Hazelcast._

object DMap {

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

  def remove[F[_], K, V](k: K)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => F.delay(map.remove(k)) }

  def get[F[_] : Async, K, V](k: K): ReaderT[F, hz.IMap[K, V], Option[V]] =
    ReaderT { map => run(map.getAsync(k)).map(Option(_)) }

  def put[F[_] : Async, K, V](k: K, v: V): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => runEffect(map.putAsync(k, v)) }

  def set[F[_] : Async, K, V](k: K, v: V): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => runEffect(map.setAsync(k, v)) }


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
              F.delay[Unit](map.put(k, f(a)))
            case None =>
              F.fail[Unit](new IllegalArgumentException(s"key $k is not present, cannot modify"))
          }
        }.flatMap(identity)
      }
    }


  def mapReduce[F[_], K, V, B](b: B, f: (K, V) => B, g: (B, B) => B)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], B] = {
    val mapReduce = new MapReduce[java.util.Map.Entry[K, V], B](b, entry => f(entry.getKey, entry.getValue), g)
    ReaderT { map =>
      F.delay {
        map.aggregate(mapReduce)
      }
    }
  }


  def project[F[_], K, V, B](f: (K, V) => B)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Seq[B]] = {
    val projection = new ProjectionFunction[java.util.Map.Entry[K, V], B](entry => f(entry.getKey, entry.getValue))
    ReaderT { map =>
      F.delay {
        map.project(projection).asScala.toSeq
      }
    }
  }

  def collect[F[_], K, V, B](pf: PartialFunction[(K, V), B])(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Seq[B]] = {
    val projection = new ProjectionFunction[java.util.Map.Entry[K, V], B](entry => pf(entry.getKey -> entry.getValue))
    val predicate = new EntryPredicate[K, V]((k, v) => pf.isDefinedAt(k -> v))
    ReaderT { map =>
      F.delay {
        map.project(projection, predicate).asScala.toSeq
      }
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

  def _listen[F[_], K, V, E](handler: async.mutable.Queue[F, E] => MapListener, includeValues: Boolean)(implicit F: Async[F]): ReaderT[Stream[F, ?], hz.IMap[K, V], E] =
    ReaderT { map =>
      for {
        queue <- Stream.eval(async.unboundedQueue[F, E])
        values <- Stream.bracket[F, String, E](
          F.delay {
            map.addEntryListener(handler(queue), includeValues)
          }
        )(_ => queue.dequeue, id => F.delay(map.removeEntryListener(id)).map(_ => ()))
      } yield values
    }

  def listen[F[_], K, V](implicit F: Async[F]): ReaderT[Stream[F, ?], hz.IMap[K, V], DMapKVEvent[K, V]] =
    _listen(queue => new EntryKVHandler[K, V](event => queue.enqueue1(event).unsafeRunAsync(_ => ())), true)

  def listenCollect[F[_] : Async, K, V, A](pf: PartialFunction[DMapKVEvent[K, V], A]): ReaderT[Stream[F, ?], hz.IMap[K, V], A] =
    _listen[F, K, V, A](queue => new EntryKVHandler[K, V](event => pf.lift(event).map { a =>
        queue.enqueue1(a).unsafeRunAsync(_ => ())
    }), true)

  def listenKeys[F[_], K, V](implicit F: Async[F]): ReaderT[Stream[F, ?], hz.IMap[K, V], DMapKEvent[K]] =
    _listen(queue => new EntryKHandler[K, V](event => queue.enqueue1(event).unsafeRunAsync(_ => ())), false)

  def apply[F[_], K, V](map: hz.IMap[K, V])(implicit F: Async[F]): DMap[F, K, V] = {

    val interpreter = DMapAtomic.interpreter[F, K, V]

    new DMap[F, K, V] {

      def containsKey(k: K): F[Boolean] = DMap.containsKey(k).apply(map)
      def get(k: K): F[Option[V]] = DMap.get(k).apply(map)
      def put(k: K, v: V): F[Unit] = DMap.put(k, v).apply(map)
      def putAll(vs: Map[K, V]): F[Unit] = DMap.putAll(vs).apply(map)
      def set(k: K, v: V): F[Unit] = DMap.set(k, v).apply(map)
      def modifyLocal(k: K, f: V => V): F[Unit] = DMap.modifyLocal(k, f).apply(map)
      def modify(k: K, f: V => V): F[Unit] = DMap.modify(k, f).apply(map)
      def reader[A](k: K, f: V => A) = DMap.reader(k, f).apply(map)
      def readerLocal[A](k: K, f: V => A): F[A] = DMap.readerLocal(k, f).apply(map)

      def mapReduce[B](b: B)(f: (K, V) => B, g: (B, B) => B): F[B] = DMap.mapReduce(b, f, g).apply(map)
      def project[B](f: (K, V) => B): F[Seq[B]] = DMap.project(f).apply(map)
      def collect[B](pf: PartialFunction[(K, V), B]): F[Seq[B]] = DMap.collect(pf).apply(map)
      def findKeys(f: (K, V) => Boolean): F[Set[K]] = DMap.findKeys(f).apply(map)
      def removeAll: F[Unit] = DMap.removeAll.apply(map)
      def remove(k: K): F[Unit] = DMap.remove(k).apply(map)

      def atomic[A](prg: DMapAtomic.PRG[K, V, A]): F[A] = {
        prg.foldMap(interpreter).apply(map).run(Set.empty).flatMap {
          case (locks, a) => F.delay {
            locks.map(map.unlock)
            a
          }
        }
      }

      def listen: Stream[F, DMapKVEvent[K, V]] = DMap.listen.apply(map)
      def listenCollect[A](pf: PartialFunction[DMapKVEvent[K, V], A]): Stream[F, A] = DMap.listenCollect(pf).apply(map)
      def listenKeys: Stream[F, DMapKEvent[K]] = DMap.listenKeys.apply(map)
    
      override def toString: String = s"${map.getName}(${map.getServiceName}, ${map.getLocalMapStats})"
    }
  }
}

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

sealed trait DMapKEvent[+K]

object DMapKEvent {
  case class Cleared(n: Long) extends DMapKEvent[Nothing]
  case class Evicted(n: Long) extends DMapKEvent[Nothing]
  case class Add[K](k: K) extends DMapKEvent[K]
  case class Remove[K](k: K) extends DMapKEvent[K]
  case class Update[K](k: K) extends DMapKEvent[K]
  case class Merged[K](k: K) extends DMapKEvent[K]
  case class Evict[K](k: K) extends DMapKEvent[K]
}

sealed trait DMapAtomic[+K, +V, A]


object DMapAtomic {


  type PRG[K, V, A] = Free[DMapAtomic[K, V, ?], A]

  case class Get[K, V](k: K) extends DMapAtomic[K, V, Option[V]]
  case class Set[K, V](k: K, v: V) extends DMapAtomic[K, V, Unit]
  case class Put[K, V](k: K, v: V) extends DMapAtomic[K, V, Unit]
  case class Lock[K](k: K) extends DMapAtomic[K, Nothing, Unit]
  case class Unlock[K](k: K) extends DMapAtomic[K, Nothing, Unit]

  private def liftF[K, V, A](dsl: DMapAtomic[K, V, A]): PRG[K, V, A] = Free.liftF[DMapAtomic[K, V, ?], A](dsl)

  def get[K, V](k: K): PRG[K, V, Option[V]] = liftF(Get(k))
  def put[K, V](k: K, v: V): PRG[K, V, Unit] = liftF(Put(k, v))
  def set[K, V](k: K, v: V): PRG[K, V, Unit] = liftF(Set(k, v))
  def lock[K, V](k: K): PRG[K, V, Unit] = liftF(Lock(k))
  def unlock[K, V](k: K): PRG[K, V, Unit] = liftF(Unlock(k))

  type AtomicRun[F[_], K, V, A] = ReaderT[StateT[F, scala.collection.Set[K], ?], hz.IMap[K, V], A]

  def interpreter[F[_], K, V](implicit F: Async[F]): DMapAtomic[K, V, ?] ~> AtomicRun[F, K, V, ?] =
    new (DMapAtomic[K, V, ?] ~> AtomicRun[F, K, V, ?]) {
      def apply[A](fa: DMapAtomic[K, V, A]): AtomicRun[F, K, V, A] = fa match {
        case Get(k) => ReaderT { map =>
          StateT.lift(F.delay(Option(map.get(k)).asInstanceOf[A]))
        }
        case Set(k, v) => ReaderT { map =>
          StateT.lift(F.delay(map.set(k, v)))
        }
        case Put(k, v) => ReaderT { map =>
          StateT.lift(F.delay {
            map.put(k, v)
            ()
          })
        }
        case Lock(k) => ReaderT { map =>
          StateT { locks => F.delay((locks + k) -> map.lock(k)) }
        }
        case Unlock(k) => ReaderT { map =>
          StateT { locks => F.delay((locks - k) -> map.lock(k)) }
        }
      }
    }


}


trait DMap[F[_], K, V] extends Serializable {

  def containsKey(k: K): F[Boolean]
  def get(k: K): F[Option[V]]
  def put(k: K, v: V): F[Unit]
  def putAll(vs: Map[K, V]): F[Unit]
  def set(k: K, v: V): F[Unit]
  def modifyLocal(k: K, f: V => V): F[Unit]
  def modify(k: K, f: V => V): F[Unit]
  def reader[A](k: K, f: V => A): F[A]
  def readerLocal[A](k: K, f: V => A): F[A]
  def mapReduce[B](b: B)(f: (K, V) => B, g: (B, B) => B): F[B]
  def project[B](f: (K, V) => B): F[Seq[B]]
  def collect[B](pf: PartialFunction[(K, V), B]): F[Seq[B]]
  def findKeys(f: (K, V) => Boolean): F[Set[K]]
  def remove(k: K): F[Unit]
  def removeAll: F[Unit]
  def listen: Stream[F, DMapKVEvent[K, V]]
  def listenCollect[A](pf: PartialFunction[DMapKVEvent[K, V], A]): Stream[F, A]
  def listenKeys: Stream[F, DMapKEvent[K]]

  def atomic[A](prg: DMapAtomic.PRG[K, V, A]): F[A]

  def fold(implicit M: Monoid[V]): F[V] = mapReduce(M.empty)((_, v) => v, M.combine)
}
