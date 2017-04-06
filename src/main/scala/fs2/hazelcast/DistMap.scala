package fs2
package hazelcast

import fs2._
import fs2.util._
import fs2.util.syntax._
import cats.data._

import com.hazelcast.{core => hz}

import Hazelcast._

object DistMap {

  def reader[F[_] : Async, K, V, A](k: K, f: V => A): ReaderT[F, hz.IMap[K, V], A] = ReaderT { map =>
    run { map.submitToKey(k, new ReadProcessor[K, V, A]((_, v) => f(v))).asInstanceOf[hz.ICompletableFuture[A]] }
  }

  def readerLocal[F[_] : Async, K, V, A](k: K, f: V => A): ReaderT[F, hz.IMap[K, V], A] = ReaderT { map =>
    run(map.getAsync(k)).map(f)
  }

  def get[F[_] : Async, K, V](k: K): ReaderT[F, hz.IMap[K, V], Option[V]] = ReaderT { map =>
    run(map.getAsync(k)).map(Option(_))
  }

  def put[F[_] : Async, K, V](k: K, v: V): ReaderT[F, hz.IMap[K, V], Unit] = ReaderT { map =>
    runEffect(map.putAsync(k, v))
  }

  def lock[F[_], K, V](k: K)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] = ReaderT { map =>
    F.delay(map.lock(k))
  }

  def unlock[F[_], K, V](k: K)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] = ReaderT { map =>
    F.delay(map.unlock(k))
  }

  def set[F[_], K, V](k: K, v: V)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] = ReaderT { map =>
    runEffect(map.setAsync(k, v))
  }

  def modify[F[_], K, V](k: K, f: V => V)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] = ReaderT { map =>
    runEffect { map.submitToKey(k, new WriteProcessor[K, V]((_, v) => f(v))) }
  }

  def modifyLocal[F[_], K, V](k: K, f: V => V)(implicit F: Async[F]): ReaderT[F, hz.IMap[K, V], Unit] = ReaderT[F, hz.IMap[K, V], Unit] { map =>
      Stream.bracket[F, Unit, Unit](lock(k).apply(map))(_ => {
        Stream.eval {
          for {
            v <- get(k).apply(map)
            _ <- if(v.isDefined) set(k, f(v.get)).apply(map) else F.fail(new IllegalArgumentException(s"cannot modify non-existent value with key $k"))
          } yield ()
        }
      },
        _ => unlock(k).apply(map)
      ).run
  }

  def apply[F[_] : Async, K, V](map: hz.IMap[K, V]): DistMap[F, K, V] = new DistMap[F, K, V] {
    def get(k: K): F[Option[V]] = DistMap.get(k).apply(map)
    def put(k: K, v: V): F[Unit] = DistMap.put(k, v).apply(map)
    def set(k: K, v: V): F[Unit] = DistMap.set(k, v).apply(map)
    def modifyLocal(k: K, f: V => V): F[Unit] = DistMap.modifyLocal(k, f).apply(map)
    def modify(k: K, f: V => V): F[Unit] = DistMap.modify(k, f).apply(map)
    def reader[A](k: K, f: V => A) = DistMap.reader(k, f).apply(map)
    def readerLocal[A](k: K, f: V => A): F[A] = DistMap.readerLocal(k, f).apply(map)
  }

}

trait DistMap[F[_], K, V] {
  def get(k: K): F[Option[V]]
  def put(k: K, v: V): F[Unit]
  def set(k: K, v: V): F[Unit]
  def modifyLocal(k: K, f: V => V): F[Unit]
  def modify(k: K, f: V => V): F[Unit]
  def reader[A](k: K, f: V => A): F[A]
  def readerLocal[A](k: K, f: V => A): F[A]
}
