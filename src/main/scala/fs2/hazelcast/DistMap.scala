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

  def lock[F[_], K, V](k: K)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => F.delay(map.lock(k)) }

  def unlock[F[_], K, V](k: K)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => F.delay(map.unlock(k)) }

  def modify[F[_], K, V](k: K, f: V => V)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT { map => runEffect { map.submitToKey(k, new WriteProcessor[K, V]((_, v) => f(v))) } }

  def modifyLocal[F[_], K, V](k: K, f: V => V)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] =
    ReaderT[F, hz.IMap[K, V], Unit] { map =>
      Stream.bracket[F, Unit, Unit](lock(k).apply(map))(_ =>
        Stream.eval {
          for {
            v <- get(k).apply(map)
            _ <- if(v.isDefined) put(k, f(v.get)).apply(map) else F.fail(new IllegalArgumentException(s"cannot modify non-existent value with key $k"))
          } yield ()
        },
        _ => unlock(k).apply(map)
      ).run
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

  def apply[F[_] : Async, K, V](map: hz.IMap[K, V]): DistMap[F, K, V] = new DistMap[F, K, V] {
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
    def removeAll: F[Unit] = DistMap.removeAll.apply(map)
  }
}

import cats._

trait DistMap[F[_], K, V] extends Serializable {
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
  def removeAll: F[Unit]

  def fold(implicit M: Monoid[V]): F[V] = mapReduce(M.empty)((_, v) => v, M.combine)
}
