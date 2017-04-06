package fs2
package hazelcast

import com.hazelcast.{core => hz}
import com.hazelcast.map._

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

  def process(entry: Map.Entry[K, V]): AnyRef = {
    f(entry.getKey, entry.getValue).asInstanceOf[AnyRef]
  }

  val getBackupProcessor = null
}



